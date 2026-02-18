package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser // 通常表示一个TCP连接 net.Conn
	buf  *bufio.Writer      // 带缓冲的写入器 直接往网络 conn 写数据（syscall）开销大。bufio 会先把数据攒在内存里，凑够一波再一次性发出去，极大提高性能。
	dec  *gob.Decoder       // Gob 解码器 负责从 conn 中读取字节流 -> 还原成 Go 结构体
	enc  *gob.Encoder       // Gob 编码器 负责把 Go 结构体 -> 变成字节流 -> 写入 buf
}

// 防御性编程技巧
var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	// 1. 创建带缓冲的Writer
	buf := bufio.NewWriter(conn)

	// 2. 返回接口类型，注意这里返回的是是Codec接口，而不是*GobCodec 指针。
	// 体现了多态：外部调用者不需要知道内部是GobCodec，当成Codec用
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

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

	// 对Header进行编码
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}

	// 对body进行编码
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
