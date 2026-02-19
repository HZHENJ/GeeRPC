极客兔兔 7天用Go从零实现RPC框架

RPC前置知识：RPC，Rmote Procedure Call 远程过程调用，是一种通信模式。它的的新目标是：让调用远程服务器上的函数，像调用本地函数一样简单。
当你调用本地函数时，直接从内存中读取代码并执行；而RPC则把这个过程通过网络“屏蔽”了。

RPC最核心的应用场景是微服务架构（Microservices）的内部通信，举例：当打开一个电商APP的首页，后台可能发生了几十次服务调用：
用户服务，验证你的登陆状态；推荐服务，算出你可能喜欢的商品；库存服务，检查商品是否有货；价格服务，计算你的会员折扣。
这些服务通常部署在同一个内网环境的不同服务器上，使用RPC的原因是：
- 高性能：内部调用极其频繁，RPC采用二进制序列化，比Json节省空间和CPU损耗
- 低延迟：直接基于TCP或者HTTP/2，省去了复杂的HTTP Header解析开销
- 强类型：通过IDL（接口定义语言）保证了服务A调用服务B时，参数类型绝对匹配，减少BUG

存储与中间件系统，很多分布式系统，其内部节点之间的指令传输和数据同步都是靠自定义RPC实现的：
- 消息队列：例如Kafka或RabbitMQ，生产者发送消息给Broker，本质就是一次RPC
- 分布式数据库：例如TiDB，计算节点与存储节点（TiKV）之间的交互
- 容器编排：K8s内部组件（如Kubelet与control plane）之间大量使用gRPC

跨语言协作

# 第一篇
- 使用 encoding/gob 实现消息的编解码(序列化与反序列化)
- 实现一个简易的服务端，仅接受消息，不处理 

通信过程：
客户端与服务端的通信需要协商一些内容，例如 HTTP 报文，分为 header 和 body 2 部分，body 的格式和长度通过 header 中的 Content-Type 和 Content-Length 指定，服务端通过解析 header 就能够知道如何从 body 中读取需要的信息。对于 RPC 协议来说，这部分协商是需要自主设计的。为了提升性能，一般在报文的最开始会规划固定的字节，来协商相关的信息。比如第1个字节用来表示序列化方式，第2个字节表示压缩方式，第3-6字节表示 header 的长度，7-10 字节表示 body 的长度。

对于 GeeRPC 来说，目前需要协商的唯一一项内容是消息的编解码方式。我们将这部分信息，放到结构体 Option 中承载。目前，已经进入到服务端的实现阶段了。

Tips：先敲代码，然后整理思路

脑子里有好多大大的疑问，默认实例模式（Default Instance Pattern）是什么？

Gob编解码与简易服务器构成RPC框架的骨架
原理：TCP是字节流，没有边界，会存在沾包的问题；为了让服务器知道“哪里是一个请求的结束”，我们需要定义一个协议
最简单的RPC协议通常分为Header和Body。
- Header：元数据，包含方法名、请求序列号、错误信息
- Body：参数，具体调用参数，比如{A:1, B:2}

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
- 读取请求：`readRequest`
- 处理请求：`handleRequest`
- 回复请求：`sendResponse`

在一次连接中，允许接收多个请求，即多个request header和request body，因此这里使用了for无限制等待请求的到来，知道发生错误（例如连接被关闭，接收到的报文有问题）。
这里需要注意三个点：
1. handleRequest 使用了协程并发执行请求。
2. 处理请求是并发的，但是回复请求的报文必须是逐个发送的，并发容易导致多个回复报文交织在一起，客户端无法解析。在这里使用锁(sending)保证。
3. 尽力而为，只有在 header 解析失败时，才终止循环。

