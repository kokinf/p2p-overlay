package rpc

import (
	"context"
	"crypto/rand"
	"crypto/tls"
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

const maxConcurrentHandlers = 64

type Handler func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error)

type Manager struct {
	id           *identity.Identity
	codec        serialization.Codec
	timeout      time.Duration
	writeTimeout time.Duration
	handler      Handler
	tlsCert      tls.Certificate

	mu          sync.Mutex
	conns       map[nodeid.ID]*conn
	pending     map[[16]byte]chan *protocol.Frame
	replayCache map[[16]byte]time.Time
	sem         chan struct{}

	closeOnce sync.Once
	done      chan struct{}
}

func NewManager(id *identity.Identity, codec serialization.Codec, handler Handler, timeout time.Duration) *Manager {
	return NewManagerWithWriteTimeout(id, codec, handler, timeout, 10*time.Second)
}

func NewManagerWithWriteTimeout(id *identity.Identity, codec serialization.Codec, handler Handler, readTimeout, writeTimeout time.Duration) *Manager {
	cert, err := id.GenerateTLSCertificate()
	if err != nil {
		log.Fatalf("failed to generate TLS cert: %v", err)
	}
	m := &Manager{
		id:           id,
		codec:        codec,
		timeout:      readTimeout,
		writeTimeout: writeTimeout,
		handler:      handler,
		tlsCert:      cert,
		conns:        make(map[nodeid.ID]*conn),
		pending:      make(map[[16]byte]chan *protocol.Frame),
		replayCache:  make(map[[16]byte]time.Time),
		sem:          make(chan struct{}, maxConcurrentHandlers),
		done:         make(chan struct{}),
	}
	go m.cleanupReplayCache()
	return m
}

func (m *Manager) ID() nodeid.ID { return m.id.NodeID() }

func (m *Manager) Serve(ctx context.Context, ln net.Listener) error {
	config := &tls.Config{
		Certificates: []tls.Certificate{m.tlsCert},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	tlsListener := tls.NewListener(ln, config)

	go func() {
		<-ctx.Done()
		_ = tlsListener.Close()
	}()
	for {
		nc, err := tlsListener.Accept()
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
	tlsConn, ok := nc.(*tls.Conn)
	if !ok {
		_ = nc.Close()
		return
	}

	if err := tlsConn.Handshake(); err != nil {
		log.Printf("rpc: tls handshake failed: %v", err)
		_ = nc.Close()
		return
	}

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		_ = nc.Close()
		return
	}
	remoteID, err := identity.ExtractNodeIDFromCert(state.PeerCertificates[0])
	if err != nil {
		log.Printf("rpc: invalid client cert: %v", err)
		_ = nc.Close()
		return
	}

	c := newConn(tlsConn, m.writeTimeout)
	c.remote = remoteID

	m.mu.Lock()
	if existing, ok := m.conns[remoteID]; ok {
		existing.close()
	}
	m.conns[remoteID] = c
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		if m.conns[remoteID] == c {
			delete(m.conns, remoteID)
		}
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
		select {
		case ch <- f:
		default:
		}
	}
}

func (m *Manager) handleRequest(c *conn, f *protocol.Frame) {
	m.mu.Lock()
	if _, exists := m.replayCache[f.RequestID]; exists {
		m.mu.Unlock()
		log.Printf("rpc: replay detected for request_id %x from %s", f.RequestID[:4], c.remote)
		return
	}
	m.replayCache[f.RequestID] = time.Now()
	m.mu.Unlock()

	select {
	case m.sem <- struct{}{}:
	case <-m.done:
		return
	}
	defer func() { <-m.sem }()

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

func (m *Manager) cleanupReplayCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.mu.Lock()
			now := time.Now()
			for id, t := range m.replayCache {
				if now.Sub(t) > 2*time.Minute {
					delete(m.replayCache, id)
				}
			}
			m.mu.Unlock()
		}
	}
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
	config := &tls.Config{
		Certificates:       []tls.Certificate{m.tlsCert},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	}
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nodeid.ID{}, err
	}
	tlsConn := tls.Client(nc, config)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = nc.Close()
		return nodeid.ID{}, err
	}

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		_ = nc.Close()
		return nodeid.ID{}, fmt.Errorf("no cert provided")
	}
	remoteID, err := identity.ExtractNodeIDFromCert(state.PeerCertificates[0])
	if err != nil {
		_ = nc.Close()
		return nodeid.ID{}, err
	}

	c := newConn(tlsConn, m.writeTimeout)
	c.remote = remoteID

	m.mu.Lock()
	if _, ok := m.conns[remoteID]; ok {
		m.mu.Unlock()
		_ = nc.Close()
		return remoteID, nil
	}
	m.conns[remoteID] = c
	m.mu.Unlock()

	go m.readLoop(c)
	return remoteID, nil
}

func (m *Manager) getOrDial(ctx context.Context, remote nodeid.ID, addr string) (*conn, error) {
	m.mu.Lock()
	if c, ok := m.conns[remote]; ok {
		m.mu.Unlock()
		return c, nil
	}
	m.mu.Unlock()

	config := &tls.Config{
		Certificates:       []tls.Certificate{m.tlsCert},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	}

	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	tlsConn := tls.Client(nc, config)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = nc.Close()
		return nil, err
	}

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		_ = nc.Close()
		return nil, fmt.Errorf("no cert provided")
	}
	actualID, err := identity.ExtractNodeIDFromCert(state.PeerCertificates[0])
	if err != nil || actualID != remote {
		_ = nc.Close()
		return nil, fmt.Errorf("remote id mismatch: got %s, want %s", actualID, remote)
	}

	c := newConn(tlsConn, m.writeTimeout)
	c.remote = remote

	m.mu.Lock()
	if existing, ok := m.conns[remote]; ok {
		m.mu.Unlock()
		_ = nc.Close()
		return existing, nil
	}
	m.conns[remote] = c
	m.mu.Unlock()

	go m.readLoop(c)
	return c, nil
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() { close(m.done) })
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
		protocol.FrameTunnelCloseOk,
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
