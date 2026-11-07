package rpc

import (
	"net"
	"sync"
	"time"

	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
)

type conn struct {
	netConn      net.Conn
	dec          *protocol.StreamDecoder
	remote       nodeid.ID
	writeMu      sync.Mutex
	writeTimeout time.Duration
}

func newConn(nc net.Conn, writeTimeout time.Duration) *conn {
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	return &conn{
		netConn:      nc,
		dec:          protocol.NewStreamDecoder(nc),
		writeTimeout: writeTimeout,
	}
}

func (c *conn) write(f *protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.netConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return f.Encode(c.netConn)
}

func (c *conn) read(timeout time.Duration) (*protocol.Frame, error) {
	_ = c.netConn.SetReadDeadline(time.Now().Add(timeout))
	return c.dec.ReadFrame()
}

func (c *conn) close() { _ = c.netConn.Close() }
