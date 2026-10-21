package rpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
)

type Handler func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error)

type Manager struct {
	id      *identity.Identity
	codec   serialization.Codec
	timeout time.Duration
	handler Handler

	mu      sync.Mutex
	conns   map[nodeid.ID]*conn
	pending map[[16]byte]chan *protocol.Frame
}

func NewManager(id *identity.Identity, codec serialization.Codec, handler Handler, timeout time.Duration) *Manager {
	return &Manager{
		id:      id,
		codec:   codec,
		timeout: timeout,
		handler: handler,
		conns:   make(map[nodeid.ID]*conn),
		pending: make(map[[16]byte]chan *protocol.Frame),
	}
}

func (m *Manager) ID() nodeid.ID { return m.id.NodeID() }

func (m *Manager) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			return err
		}
		go m.serveConn(nc)
	}
}

func (m *Manager) serveConn(nc net.Conn) {
	c := newConn(nc)
	if err := m.serverHandshake(c); err != nil {
		log.Printf("rpc: server handshake: %v", err)
		c.close()
		return
	}
	m.mu.Lock()
	if existing, ok := m.conns[c.remote]; ok {
		m.mu.Unlock()
		_ = existing
		c.close()
		return
	}
	m.conns[c.remote] = c
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.conns, c.remote)
		m.mu.Unlock()
		c.close()
	}()
	m.readLoop(c)
}

func (m *Manager) readLoop(c *conn) {
	for {
		f, err := c.read(m.timeout)
		if err != nil {
			return
		}
		if isResponse(f.Type) {
			m.deliver(f)
			continue
		}
		go m.handleRequest(c, f)
	}
}

func (m *Manager) deliver(f *protocol.Frame) {
	m.mu.Lock()
	ch, ok := m.pending[f.RequestID]
	if ok {
		delete(m.pending, f.RequestID)
	}
	m.mu.Unlock()
	if ok {
		ch <- f
	}
}

func (m *Manager) handleRequest(c *conn, f *protocol.Frame) {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	resp, err := m.handler(ctx, c.remote, f)
	if err != nil {
		_ = c.write(&protocol.Frame{
			Version:   f.Version,
			Type:      protocol.FrameError,
			RequestID: f.RequestID,
			Payload:   []byte(err.Error()),
		})
		return
	}
	if resp == nil {
		return
	}
	resp.Version = f.Version
	resp.RequestID = f.RequestID
	if resp.Type == 0 {
		resp.Type = f.Type + 1
	}
	_ = c.write(resp)
}

func (m *Manager) Call(ctx context.Context, remote nodeid.ID, addr string, req *protocol.Frame) (*protocol.Frame, error) {
	c, err := m.getOrDial(ctx, remote, addr)
	if err != nil {
		return nil, err
	}
	if req.RequestID == ([16]byte{}) {
		rid, err := newRequestID()
		if err != nil {
			return nil, err
		}
		req.RequestID = rid
	}
	if req.Version == 0 {
		req.Version = 1
	}
	ch := make(chan *protocol.Frame, 1)
	m.mu.Lock()
	m.pending[req.RequestID] = ch
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.pending, req.RequestID)
		m.mu.Unlock()
	}()
	if err := c.write(req); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		if resp.Type == protocol.FrameError {
			return nil, fmt.Errorf("remote error: %s", string(resp.Payload))
		}
		return resp, nil
	}
}

func (m *Manager) DialUnverified(ctx context.Context, addr string) (nodeid.ID, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nodeid.ID{}, err
	}
	c := newConn(nc)
	if err := m.clientHandshake(c, nodeid.ID{}, false); err != nil {
		c.close()
		return nodeid.ID{}, err
	}
	m.mu.Lock()
	if existing, ok := m.conns[c.remote]; ok {
		m.mu.Unlock()
		_ = existing
		c.close()
		return c.remote, nil
	}
	m.conns[c.remote] = c
	m.mu.Unlock()
	go m.readLoop(c)
	return c.remote, nil
}

func (m *Manager) getOrDial(ctx context.Context, remote nodeid.ID, addr string) (*conn, error) {
	m.mu.Lock()
	if c, ok := m.conns[remote]; ok {
		m.mu.Unlock()
		return c, nil
	}
	m.mu.Unlock()

	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := newConn(nc)
	if err := m.clientHandshake(c, remote, true); err != nil {
		c.close()
		return nil, err
	}
	m.mu.Lock()
	if existing, ok := m.conns[remote]; ok {
		m.mu.Unlock()
		_ = existing
		c.close()
		return existing, nil
	}
	m.conns[remote] = c
	m.mu.Unlock()
	go m.readLoop(c)
	return c, nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.conns {
		c.close()
	}
	m.conns = map[nodeid.ID]*conn{}
}

func isResponse(t protocol.FrameType) bool {
	switch t {
	case protocol.FramePong,
		protocol.FrameFindNodeResponse,
		protocol.FrameStoreResponse,
		protocol.FrameFindValueResponse,
		protocol.FrameTunnelBuildOK,
		protocol.FrameTunnelBuildFail,
		protocol.FrameTunnelAck,
		protocol.FrameAppAck,
		protocol.FrameHelloOK,
		protocol.FrameError:
		return true
	}
	return false
}

func newRequestID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}
