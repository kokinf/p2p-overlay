package rpc

import (
	"net"
	"sync"
	"time"

	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
)

type conn struct {
	netConn net.Conn
	dec     *protocol.StreamDecoder
	remote  nodeid.ID
	writeMu sync.Mutex
}

func newConn(nc net.Conn) *conn {
	return &conn{netConn: nc, dec: protocol.NewStreamDecoder(nc)}
}

func (c *conn) write(f *protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.netConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return f.Encode(c.netConn)
}

func (c *conn) read(timeout time.Duration) (*protocol.Frame, error) {
	_ = c.netConn.SetReadDeadline(time.Now().Add(timeout))
	return c.dec.ReadFrame()
}

func (c *conn) close() { _ = c.netConn.Close() }
