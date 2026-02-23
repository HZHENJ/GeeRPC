# GeeRPC
本项目来自：[极客兔兔 7天用Go从零实现RPC框架](https://github.com/geektutu/7days-golang)

极客兔兔 7天用Go从零实现RPC框架

## 前置知识

RPC（Rmote Procedure Call，远程过程调用）是一种通信模式。它的的新目标是：让调用远程服务器上的函数，像调用本地函数一样简单。当你调用本地函数时，直接从内存中读取代码并执行；而RPC则把这个过程通过网络“屏蔽”了。RPC最核心的应用场景是微服务架构（Microservices）的内部通信，举例：当打开一个电商APP的首页，后台可能发生了几十次服务调用：用户服务，验证你的登陆状态；推荐服务，算出你可能喜欢的商品；库存服务，检查商品是否有货；价格服务，计算你的会员折扣。

这些服务通常部署在同一个内网环境的不同服务器上，使用RPC的原因是：

1. 高性能：内部调用极其频繁，RPC采用二进制序列化，比Json节省空间和CPU损耗

2. 低延迟：直接基于TCP或者HTTP/2，省去了复杂的HTTP Header解析开销

3. 强类型：通过IDL（接口定义语言）保证了服务A调用服务B时，参数类型绝对匹配，减少BUG

存储与中间件系统，很多分布式系统，其内部节点之间的指令传输和数据同步都是靠自定义RPC实现的：

* 消息队列：例如Kafka或RabbitMQ，生产者发送消息给Broker，本质就是一次RPC

* 分布式数据库：例如TiDB，计算节点与存储节点（TiKV）之间的交互

* 容器编排：K8s内部组件（如Kubelet与control plane）之间大量使用gRPC

## 服务端与消息编码

* 使用 encoding/gob 实现消息的编解码(序列化与反序列化)

* 实现一个简易的服务端，仅接受消息，不处理&#x20;

### 消息的序列化与反序列化

RPC帮助我们实现调用远程函数像调用本地函数一样，发起调用请求的一方叫做调用方，被调用的一方叫做服务提供方。调用方和服务提供方一般处于不同的服务器，所以需要用网络来传输数据，RPC默认采用TCP协议来传输数据，同时HTTP协议也是建立在TCP协议之上的。我们需要通过网络传输的数据是结构化数据，例如一个类（Class）或者一个结构体（Struct），所以要想使用网络框架的API来传输结构化的数据，就要实现结构化的数据与字节流之间的双向转换。这种将结构化数据转换成字节流的过程称为序列化，反之为反序列化。序列化的用途除了用在网络上传输数据以外，另一个重要的用途是，将结构化数据保存在文件中，因为文件内保存数据的形式也是二进制序列，和网络传输过程中的数据是一样的，所以序列化同样适用于将结构化的数据保存在文件中。

总结一下，什么是消息的序列化和反序列化以及为什么要进行序列化和反序列化？

* 什么是消息的序列化和反序列化：将类或者结构体这样的结构化数据转换成字节流的过程被称为序列化，反过来被称为反序列化

* 为什么要进行序列化和反序列化：

    * 在网络传输中数据是以字节的形式进行传输的

    * 文件内保存数据的形式也是二进制序列，将结构化数据存储文件中

**一个典型的RPC调用过程如下**

```go
err = client.Call("Arith.Multiply", args, &reply)
```

客户端发送的请求包括：服务名 `Arith`，方法名 `Multiply`，参数 `args` 三个，服务端的响应包括错误 `error`，返回值 `reply` 2 个。我们将请求和响应中的参数和返回值抽象为 body，剩余的信息放在 header 中，那么就可以抽象出数据结构 `Header`：

```go
package codec

import "io"

type Header struct {
        ServiceMethod string // format "Service.Method"
        Seq           uint64 // sequence number chosen by client
        Error         string
}
```

* `ServiceMethod` 是服务名和方法名，通常与 Go 语言中的结构体和方法相映射。

* `Seq`是请求的序号，也可以认为是某个请求的 ID，用来区分不同的请求。

* `Error`是错误信息，客户端置为空，服务端如果如果发生错误，将错误信息置于 Error 中。

```go
type Codec interface {
    io.Closer
    ReadHeader(*Header) error
    ReadBody(interface{}) error
    Write(*Header, interface{}) error
}
```

这里需要复习一下`interface`的内容

* `io.Closer`当一次RPC回话结束后，或者网络出错时，可以直接关闭底层TCP连接 ，释放系统资源。

* `ReadHeader(*Header)`读取请求头Header

* `ReadBody(interface{})`读取消息体Body

    * 为什么参数是一个空`interface`？因为要接收任意的结构体

* `Write(*Header, interface{})`对Header和Body进行编码

```go
type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
    GobType  Type = "application/gob"
    JsonType Type = "application/json" // not implemented
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
    NewCodecFuncMap = make(map[Type]NewCodecFunc)
    NewCodecFuncMap[GobType] = NewGobCodec
}
```

这里定义了两种Codec，分别是`Gob`和`Json`，这里只实现`Gob`一种。

`GobCodec`结构体：

* `conn`是由构建函数传入，通常是通过 TCP 或者 Unix 建立 socket 时得到的链接实例

* `dec` 和 `enc` 对应 gob 的 `Decoder` 和 `Encoder`

* `buf` 是为了防止阻塞而创建的带缓冲的 `Writer`

```go
package codec

// import ...

type GobCodec struct {
    conn io.ReadWriteCloser
    buf  *bufio.Writer
    dec  *gob.Decoder
    enc  *gob.Encoder
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
    buf := bufio.NewWriter(conn)
    return &GobCodec{
        conn: conn,
        buf:  buf,
        dec:  gob.NewDecoder(conn),
        enc:  gob.NewEncoder(buf),
    }
}
```

实现Codec接口中的内容`ReadHeader`、`ReadBody`、`Write` 和 `Close` 方法。

```go
func (c *GobCodec) ReadHeader(h *Header) error {
    return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
    return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
    defer func() {
        _ = c.buf.Flush()
        if err != nil {
                _ = c.Close()
        }
    }()
    if err := c.enc.Encode(h); err != nil {
        log.Println("rpc codec: gob error encoding header:", err)
        return err
    }
    if err := c.enc.Encode(body); err != nil {
        log.Println("rpc codec: gob error encoding body:", err)
        return err
    }
    return nil
}

func (c *GobCodec) Close() error {
    return c.conn.Close()
}
```

### 通信过程

客户端与服务端的通信需要协商一些内容，例如 HTTP 报文，分为 header 和 body 2 部分，body 的格式和长度通过 header 中的 Content-Type 和 Content-Length 指定，服务端通过解析 header 就能够知道如何从 body 中读取需要的信息。对于 RPC 协议来说，这部分协商是需要自主设计的。为了提升性能，一般在报文的最开始会规划固定的字节，来协商相关的信息。比如第1个字节用来表示序列化方式，第2个字节表示压缩方式，第3-6字节表示 header 的长度，7-10 字节表示 body 的长度。对于 GeeRPC 来说，目前需要协商的唯一一项内容是消息的编解码方式。我们将这部分信息，放到结构体 Option 中承载。目前，已经进入到服务端的实现阶段了。

```go
package geerpc

const MagicNumber = 0x3bef5c

type Option struct {
    MagicNumber int        // MagicNumber marks this's a geerpc request
    CodecType   codec.Type // client may choose different Codec to encode body
}

var DefaultOption = &Option{
    MagicNumber: MagicNumber,
    CodecType:   codec.GobType,
}
```

一般来说，涉及协议协商的这部分信息，需要设计固定的字节来传输的。但是为了实现上更简单，GeeRPC 客户端固定采用 JSON 编码 Option，后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，服务端首先使用 JSON 解码 Option，然后通过 Option 的 CodeType 解码剩余的内容。即报文将以这样的形式发送：

```go
| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
```

在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的。

```go
| Option | Header1 | Body1 | Header2 | Body2 | ...
```

TCP是字节流，没有边界，会存在沾包的问题；为了让服务器知道“哪里是一个请求的结束”，我们需要定义一个协议
最简单的RPC协议通常分为Header和Body。

* Header：元数据，包含方法名、请求序列号、错误信息

* Body：参数，具体调用参数，比如{A:1, B:2}

Gob的作用：Gob是Go独有的序列化方式，原理是在发送端将结构体转化成二进制，在接收端根据数据类型还原。因为Header和Body都是结构体，所以我们用Gob分别对其编码

### 服务端实现

```go
// Server represents an RPC Server.
type Server struct{}

// NewServer returns a new Server.
func NewServer() *Server {
    return &Server{}
}

// DefaultServer is the default instance of *Server.
var DefaultServer = NewServer()

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func (server *Server) Accept(lis net.Listener) {
    for {
        conn, err := lis.Accept()
        if err != nil {
            log.Println("rpc server: accept error:", err)
            return
        }
        go server.ServeConn(conn)
    }
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }
```

* `Server`结构体，没有任何成员字段

* 实现`Accept`方法`net.Listener` 作为参数，for 循环等待 socket 连接建立，用来接收客户端请求，并开启子协程处理，处理过程交给了 `ServerConn` 方法。如果没有用并发的话那么接下来的客户端请求可能就会被阻塞。

* `DefaultServer` 是一个默认的 `Server` 实例，主要为了用户使用方便。

如果想启动服务，传入listenser即可，tcp协议和unix协议都支持

```go
lis, _ := net.Listen("tcp", ":9999")
geerpc.Accept(lis)

// 区别在于
lis, _ := net.Listen("tcp", ":9999") 
myServer := geerpc.NewServer() // 1. 先得自己 new 一个实例 
myServer.Accept(lis)           // 2. 用实例去调用方法
```

处理过程的方法`ServeConn`的实现：

1. 先使用`json.NewDecoder`反序列化得到`Option`实例

2. 检查`MagicNumber`和`CodeType`是否正确

3. 根据`CodeType`的到对应的消息编解码器，交给`serverCodec`处理

```go
// ServeConn runs the server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
    defer func() { _ = conn.Close() }()
    var opt Option
    if err := json.NewDecoder(conn).Decode(&opt); err != nil {
        log.Println("rpc server: options error: ", err)
        return
    }
    if opt.MagicNumber != MagicNumber {
        log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
        return
    }
    f := codec.NewCodecFuncMap[opt.CodecType]
    if f == nil {
        log.Printf("rpc server: invalid codec type %s", opt.CodecType)
        return
    }
    server.serveCodec(f(conn))
}
```

`serveCodec`的实现过程：

1. 读取请求`readRequest`

2. 处理请求`handleRequest`

3. 回复请求`sendResponse`

* 为什么要有`invalidRequest`这个空结构体？

    * 回顾一下`Codec`中`Write`的定义`Write(*Header, interface{}) error`，所以当发生异常的时候需要这个空结构体进行占位。在Go语言中`struct{}{}`在内存中占据的大小是0字节。

* 为什么不能传`nil`值？

    * 底层的encoding/gob编码器在序列化时可能会直接Panic，因为它需要一个具体的类型来编码

* 在一次连接中，允许接收多个请求，即多个request header和request body，这里用了for循环进行等待请求的到来，和`func (server *Server) Accept(lis net.Listener)`方法类似，这里有几个点要注意：

    * `handleRequest`使用协程并发执行请求

    * `sending`发送锁：因为接收是并发的，但是响应是逐个发送的，需要确保同一时刻，只能有一个完整的响应被写入网络。不会出现A报文前一半和B报文后一半杂糅在一起。

    * `wg`等待组：确保在关闭连接之前，所有的请求都被处理完成。

```go
// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec) {
    sending := new(sync.Mutex) // make sure to send a complete response
    wg := new(sync.WaitGroup)  // wait until all request are handled
    for {
        req, err := server.readRequest(cc)
        if err != nil {
            if req == nil {
                break // it's not possible to recover, so close the connection
            }
            req.h.Error = err.Error()
            server.sendResponse(cc, req.h, invalidRequest, sending)
            continue
        }
        wg.Add(1)
        go server.handleRequest(cc, req, sending, wg)
    }
    wg.Wait()
    _ = cc.Close()
}
```

### 简单客户端实现

```go
// import ...
func startServer(addr chan string) {
    // pick a free port
    l, err := net.Listen("tcp", ":0")
    if err != nil {
            log.Fatal("network error:", err)
    }
    log.Println("start rpc server on", l.Addr())
    addr <- l.Addr().String()
    geerpc.Accept(l)
}

func main() {
    addr := make(chan string)
    go startServer(addr)

    // in fact, following code is like a simple geerpc client
    conn, _ := net.Dial("tcp", <-addr)
    defer func() { _ = conn.Close() }()

    time.Sleep(time.Second)
    // send options
    _ = json.NewEncoder(conn).Encode(geerpc.DefaultOption)
    cc := codec.NewGobCodec(conn)
    // send request & receive response
    for i := 0; i < 5; i++ {
        h := &codec.Header{
            ServiceMethod: "Foo.Sum",
            Seq:           uint64(i),
        }
        _ = cc.Write(h, fmt.Sprintf("geerpc req %d", h.Seq))
        _ = cc.ReadHeader(h)
        var reply string
        _ = cc.ReadBody(&reply)
        log.Println("reply:", reply)
    }
}
```

## 高性能客户端

* 实现支持异步和并发的高性能客户端

### Call的设计

`func (t *T) MethodName(argType T1, replyType *T2) error`

我们封装了结构体`Call`来承载一次RPC调用所需的信息。

```go
// Call represents an active RPC.
type Call struct {
    Seq           uint64
    ServiceMethod string      // format "<service>.<method>"
    Args          interface{} // arguments to the function
    Reply         interface{} // reply from the function
    Error         error       // if error occurs, it will be set
    Done          chan *Call  // Strobes when call is complete.
}

func (call *Call) done() {
    call.Done <- call
}
```

当底层的网络协程将服务端的响应接收并处理完毕后，就会主动调用`call.done()`方法，`call.done()`这个方法内部，执行的操作是将当前的Call对象本身写入到`call.Done`这个Channel中，一旦把 `Call` 写入了这个通道，之前那个因为执行了 `<-call.Done` 而卡住的业务协程，就立刻从通道里拿到了数据，解除阻塞，继续执行。

### Client实现

```go
// Client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type Client struct {
    cc       codec.Codec
    opt      *Option
    sending  sync.Mutex // protect following
    header   codec.Header
    mu       sync.Mutex // protect following
    seq      uint64
    pending  map[uint64]*Call
    closing  bool // user has called Close
    shutdown bool // server has told us to stop
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close the connection
func (client *Client) Close() error {
    client.mu.Lock()
    defer client.mu.Unlock()
    
    if client.closing {
        return ErrShutdown
    }
    client.closing = true
    return client.cc.Close()
}

// IsAvailable return true if the client does work
func (client *Client) IsAvailable() bool {
    client.mu.Lock()
    defer client.mu.Unlock()
    
    return !client.shutdown && !client.closing
}
```

* `cc` 是消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应。

* `sending` 是一个互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆。

* `header` 是每个请求的消息头，header 只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在 Client 结构体中可以复用。

* `seq` 用于给发送的请求编号，每个请求拥有唯一编号。

* `pending` 存储未处理完的请求，键是编号，值是 Call 实例。

* `closing` 和 `shutdown` 任意一个值置为 true，则表示 Client 处于不可用的状态，但有些许的差别，closing 是用户主动关闭的，即调用 `Close` 方法，而 shutdown 置为 true 一般是有错误发生。

```go
func (client *Client) registerCall(call *Call) (uint64, error) {
    client.mu.Lock()
    defer client.mu.Unlock()
    
    if client.closing || client.shutdown {
        return 0, ErrShutdown
    }
    call.Seq = client.seq
    client.pending[call.Seq] = call
    client.seq++
    return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
    client.mu.Lock()
    defer client.mu.Unlock()
    
    call := client.pending[seq]
    delete(client.pending, seq)
    return call
}

func (client *Client) terminateCalls(err error) {
    client.sending.Lock()
    defer client.sending.Unlock()
    
    client.mu.Lock()
    defer client.mu.Unlock()
    
    client.shutdown = true
    for _, call := range client.pending {
        call.Error = err
        call.done()
    }
}
```

* `registerCall`：将参数 `call` 添加到`client.pending` 中，并更新 `client.seq`。

* `removeCall`：根据 `seq`，从`client.pending `中移除对应的 `call`，并返回。

* `terminateCalls`：服务端或客户端发生错误时调用，将`shutdown`设置为`true`，且将错误信息通知**所有** `pending` 状态的 `call`。

对于客户端来说，接收响应、发送请求是最重要的2个功能。首先实现接收功能，接收到的响应有三种情况：

1. `call`不存在，可能是请求不完整，或者因为其他原因取消了，但是服务器仍旧处理

2. `call`存在，服务端处理出错，`h.Error`不为空

3. `call`存在，服务端正常处理，但是需要从`body`中读取`Reply`的值。

```go
func (client *Client) receive() {
    var err error
    for err == nil {
        var h codec.Header
        if err = client.cc.ReadHeader(&h); err != nil {
            break
        }
        call := client.removeCall(h.Seq)
        switch {
        case call == nil:
            // it usually means that Write partially failed
            // and call was already removed.
            err = client.cc.ReadBody(nil)
        case h.Error != "":
            call.Error = fmt.Errorf(h.Error)
            err = client.cc.ReadBody(nil)
            call.done()
        default:
            err = client.cc.ReadBody(call.Reply)
            if err != nil {
                call.Error = errors.New("reading body " + err.Error())
            }
            call.done()
        }
    }
    // error occurs, so terminateCalls pending calls
    client.terminateCalls(err)
}
```

创建Client实例时：

1. 需要完成一开始的协议交换，即发送`Option`信息给服务端。

2. 协商好消息的编解码方式之后，再创建一个子协程调用`receive()`接收响应。

```go
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
    f := codec.NewCodecFuncMap[opt.CodecType]
    if f == nil {
        err := fmt.Errorf("invalid codec type %s", opt.CodecType)
        log.Println("rpc client: codec error:", err)
        return nil, err
    }
    // send options with server
    if err := json.NewEncoder(conn).Encode(opt); err != nil {
        log.Println("rpc client: options error: ", err)
        _ = conn.Close()
        return nil, err
    }
    return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
    client := &Client{
        seq:     1, // seq starts with 1, 0 means invalid call
        cc:      cc,
        opt:     opt,
        pending: make(map[uint64]*Call),
    }
    go client.receive()
    return client
}
```

实现另外两个函数：

1. `parseOptions`校验`opts`参数

2. `Dial`建立网络连接，创建`Client`实例

```go
func parseOptions(opts ...*Option) (*Option, error) {
    // if opts is nil or pass nil as parameter
    if len(opts) == 0 || opts[0] == nil {
        return DefaultOption, nil
    }
    if len(opts) != 1 {
        return nil, errors.New("number of options is more than 1")
    }
    opt := opts[0]
    opt.MagicNumber = DefaultOption.MagicNumber
    if opt.CodecType == "" {
        opt.CodecType = DefaultOption.CodecType
    }
    return opt, nil
}

// Dial connects to an RPC server at the specified network address
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
    opt, err := parseOptions(opts...)
    if err != nil {
        return nil, err
    }
    conn, err := net.Dial(network, address)
    if err != nil {
        return nil, err
    }
    // close the connection if client is nil
    defer func() {
        if client == nil {
            _ = conn.Close()
        }
    }()
    return NewClient(conn, opt)
}
```

此时GeeRPC客户端已经具备了完整的创建连接和接收响应的能力，最后还需要实现发送请求功能：

```go
func (client *Client) send(call *Call) {
    // make sure that the client will send a complete request
    client.sending.Lock()
    defer client.sending.Unlock()

    // register this call.
    seq, err := client.registerCall(call)
    if err != nil {
        call.Error = err
        call.done()
        return
    }

    // prepare request header
    client.header.ServiceMethod = call.ServiceMethod
    client.header.Seq = seq
    client.header.Error = ""

    // encode and send the request
    if err := client.cc.Write(&client.header, call.Args); err != nil {
        call := client.removeCall(seq)
        // call may be nil, it usually means that Write partially failed,
        // client has received the response and handled
        if call != nil {
            call.Error = err
            call.done()
        }
    }
}

// Go invokes the function asynchronously.
// It returns the Call structure representing the invocation.
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
    if done == nil {
        done = make(chan *Call, 10)
    } else if cap(done) == 0 {
        log.Panic("rpc client: done channel is unbuffered")
    }
    call := &Call{
        ServiceMethod: serviceMethod,
        Args:          args,
        Reply:         reply,
        Done:          done,
    }
    client.send(call)
    return call
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
    call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
    return call.Error
}
```

`Go`和`Call`是客户端暴露给用户的两个RPC服务调用接口，`Go`是一个异步接口，返回`call`实例，`Call`是对`Go`的封装，阻塞`call.Done`，等待响应返回，是一个同步接口。

* `Go`在执行完`client.send(call)`后就直接返回`return call`，无需等待返回结果

### Demo实现

```go
func startServer(addr chan string) {
    // pick a free port
    l, err := net.Listen("tcp", ":0")
    if err != nil {
            log.Fatal("network error:", err)
    }
    log.Println("start rpc server on", l.Addr())
    addr <- l.Addr().String()
    geerpc.Accept(l)
}
```

```go
func main() {
    log.SetFlags(0)
        addr := make(chan string)
        go startServer(addr)
        client, _ := geerpc.Dial("tcp", <-addr)
        defer func() { _ = client.Close() }()

        time.Sleep(time.Second)
        // send request & receive response
        var wg sync.WaitGroup
        for i := 0; i < 5; i++ {
            wg.Add(1)
            go func(i int) {
                defer wg.Done()
                args := fmt.Sprintf("geerpc req %d", i)
                var reply string
                if err := client.Call("Foo.Sum", args, &reply); err != nil {
                    log.Fatal("call Foo.Sum error:", err)
                }
                log.Println("reply:", reply)
            }(i)
        }
        wg.Wait()
}

// start rpc server on [::]:50658
//&{Foo.Sum 5 } geerpc req 3
//&{Foo.Sum 1 } geerpc req 0
//&{Foo.Sum 3 } geerpc req 1
//&{Foo.Sum 2 } geerpc req 4
//&{Foo.Sum 4 } geerpc req 2
//reply: geerpc resp 1
//reply: geerpc resp 5
//reply: geerpc resp 3
//reply: geerpc resp 2
//reply: geerpc resp 4
```

1. `startServer`启动服务端，成功绑定并监听（Listen）本地端口之后，在后台协程把真实的监听地址存入`addr`管道，然后调用`geerpc.Accept` ，像个守门等待客户端的请求。

2. `main`**&#x20;**&#x4E3B;协程执行到`<-addr`时确实会卡住等待。一旦拿到地址，它立刻调用 `Dial` 冲向服务端，完成 TCP 三次握手和协议协商，创建出 `client`&#x20;

3. &#x20;wg 与协程并发： 并发调用与服务端处理，主协程瞬间派生出 5 个子协程。这 5 个子协程**同时**拿着这个共享的 `client`，向里面传入`Foo.Sum` 的方法名和参数。 `Accept` 里的服务端终于等到了这 1 个连接（注意，5 个请求复用这 1 个物理连接）。服务端会接管这个连接，解析出 5 个请求，计算出 5 个 reply，再返回回给客户端。最后，主协程通过 `wg.Wait()` 守在最后，确保 5 个子协程都拿到了属于自己的那一份返回值（reply），才结束整个进程。

## 超时处理（timeout）

* 增加连接超时的处理机制

* 增加服务端处理超时的处理机制

超时处理是RPC框架的一个比较基本的能力，如果缺少超时处理机制，无论是服务端还是客户端都容易因为网络或者其他错误导致挂死，资源耗尽，这些问题的出现大大降低了服务的可用性。因为我们需要在RPC框架中加入超时处理的能力。

纵观整个远程调用的过程，需要客户端处理超时的地方有：

* 与服务端建立俩按揭，导致的超时

* 发送请求到服务端，写报文导致的超时

* 等待服务端处理时，等待处理导致的超时（比如服务端已经挂死，迟迟不响应）

* 从服务端接收哦响应时，读报文导致的超时

需要服务端处理的超时的地方有：

* 读取客户端请求报文时，读报文导致的超时

* 发送响应报文时，写报文导致的超时

* 调用映射服务的方法时，处理报文导致的超时

GeeRPC 在 3 个地方添加了超时处理机制。分别是：

1. 客户端创建连接时

2. 客户端 `Client.Call()` 整个过程导致的超时（包含发送报文，等待处理，接收报文所有阶段）

3. 服务端处理报文，即 `Server.handleRequest` 超时。