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

## 通信过程

客户端与服务端的通信需要协商一些内容，例如 HTTP 报文，分为 header 和 body 2 部分，body 的格式和长度通过 header 中的 Content-Type 和 Content-Length 指定，服务端通过解析 header 就能够知道如何从 body 中读取需要的信息。对于 RPC 协议来说，这部分协商是需要自主设计的。为了提升性能，一般在报文的最开始会规划固定的字节，来协商相关的信息。比如第1个字节用来表示序列化方式，第2个字节表示压缩方式，第3-6字节表示 header 的长度，7-10 字节表示 body 的长度。
对于 GeeRPC 来说，目前需要协商的唯一一项内容是消息的编解码方式。我们将这部分信息，放到结构体 Option 中承载。目前，已经进入到服务端的实现阶段了。
脑子里有好多大大的疑问，默认实例模式（Default Instance Pattern）是什么？

***

Gob编解码与简易服务器构成RPC框架的骨架
原理：TCP是字节流，没有边界，会存在沾包的问题；为了让服务器知道“哪里是一个请求的结束”，我们需要定义一个协议
最简单的RPC协议通常分为Header和Body。
\- Header：元数据，包含方法名、请求序列号、错误信息
\- Body：参数，具体调用参数，比如{A:1, B:2}
Gob的作用：
Gob是Go独有的序列化方式，原理是在发送端将结构体转化成二进制，在接收端根据数据类型还原。因为Header和Body都是结构体，所以我们用Gob分别对其编码

```go
func (server *Server) serveCodec(cc codec.Codec) {
    sending := new(sync.Mutex) // make sure to send a complete response
    wg := new(sync.WaitGroup)  // wait until all request are handled
    for {
        // 1 读取请求
       req, err := server.readRequest(cc)
       if err != nil {
          if req == nil {
             break // it's not possible to recover, so close the connection
          }
          req.h.Error = err.Error()
          // 2 发送请求
          server.sendResponse(cc, req.h, invalidRequest, sending)
          continue
       }
       // 3 处理请求 - 并发
       wg.Add(1)
       go server.handleRequest(cc, req, sending, wg)
    }
    wg.Wait()
    _ = cc.Close()
}
```

`serveCodec`包含三个阶段：

* 读取请求：`readRequest`

* 处理请求：`handleRequest`

* 回复请求：`sendResponse`

在一次连接中，允许接收多个请求，即多个request header和request body，因此这里使用了for无限制等待请求的到来，知道发生错误（例如连接被关闭，接收到的报文有问题）。这里需要注意三个点：

1. handleRequest 使用了协程并发执行请求。

2. 处理请求是并发的，但是回复请求的报文必须是逐个发送的，并发容易导致多个回复报文交织在一起，客户端无法解析。在这里使用锁(sending)保证。

3. &#x20;尽力而为，只有在 header 解析失败时，才终止循环。

## 实现服务注册（service register）

