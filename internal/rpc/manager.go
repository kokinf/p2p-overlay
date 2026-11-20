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
	ctx          context.Context
	cancel       context.CancelFunc
	incoming     map[net.Conn]struct{}
	maxPayload   uint32
	id           *identity.Identity
	codec        serialization.Codec
	timeout      time.Duration
	writeTimeout time.Duration
	handler      Handler
	tlsCert      tls.Certificate
	admission    map[nodeid.ID]bool

	mu          sync.Mutex
	allConns    map[*conn]struct{}
	conns       map[nodeid.ID]*conn
	pending     map[[16]byte]chan *protocol.Frame
	pendingPeer map[[16]byte]*conn
	replayCache map[[16]byte]time.Time
	sem         chan struct{}

	closeOnce sync.Once
	done      chan struct{}
}

func NewManager(id *identity.Identity, codec serialization.Codec, handler Handler, timeout time.Duration) *Manager {
	return NewManagerWithWriteTimeout(id, codec, handler, timeout, 10*time.Second)
}

func NewManagerWithWriteTimeout(id *identity.Identity, codec serialization.Codec, handler Handler, readTimeout, writeTimeout time.Duration) *Manager {
	if readTimeout <= 0 {
		readTimeout = 10 * time.Second
	}
	cert, err := id.GenerateTLSCertificate()
	if err != nil {
		log.Fatalf("failed to generate TLS cert: %v", err)
	}
	managerCtx, managerCancel := context.WithCancel(context.Background())
	m := &Manager{
		ctx: managerCtx, cancel: managerCancel, incoming: make(map[net.Conn]struct{}),
		maxPayload:   protocol.MaxFramePayload,
		id:           id,
		codec:        codec,
		timeout:      readTimeout,
		writeTimeout: writeTimeout,
		handler:      handler,
		tlsCert:      cert,
		conns:        make(map[nodeid.ID]*conn),
		allConns:     make(map[*conn]struct{}),
		pending:      make(map[[16]byte]chan *protocol.Frame),
		pendingPeer:  make(map[[16]byte]*conn),
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
		MaxVersion:   tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) != 1 {
				return fmt.Errorf("one identity certificate required")
			}
			id, err := identity.ExtractNodeIDFromCert(cs.PeerCertificates[0])
			if err != nil {
				return err
			}
			if m.admission != nil && !m.admission[id] {
				return fmt.Errorf("identity not admitted: %s", id)
			}
			return nil
		},
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
		m.mu.Lock()
		select {
		case <-m.done:
			m.mu.Unlock()
			nc.Close()
			return nil
		default:
		}
		if len(m.incoming) >= 256 {
			m.mu.Unlock()
			nc.Close()
			continue
		}
		m.incoming[nc] = struct{}{}
		m.mu.Unlock()
		go m.serveConn(nc)
	}
}

func (m *Manager) serveConn(nc net.Conn) {
	defer func() { m.mu.Lock(); delete(m.incoming, nc); m.mu.Unlock(); nc.Close() }()
	_ = nc.SetDeadline(time.Now().Add(m.timeout))
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

	_ = nc.SetDeadline(time.Time{})
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
	c.dec.MaxPayload = m.maxPayload
	c.remote = remoteID
	m.mu.Lock()
	select {
	case <-m.done:
		m.mu.Unlock()
		c.close()
		return
	default:
	}
	m.allConns[c] = struct{}{}
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
	defer func() {
		m.mu.Lock()
		delete(m.allConns, c)
		if m.conns[c.remote] == c {
			delete(m.conns, c.remote)
		}
		m.mu.Unlock()
		c.close()
	}()

	for {
		f, err := c.read(m.timeout)
		if err != nil {
			return
		}
		if isResponse(f.Type) {
			m.deliver(c, f)
			continue
		}
		select {
		case m.sem <- struct{}{}:
			go func() { defer func() { <-m.sem }(); m.handleRequest(c, f) }()
		default:
			_ = c.write(&protocol.Frame{Version: f.Version, Type: protocol.FrameError, RequestID: f.RequestID, Payload: []byte("RPC capacity exceeded")})
		}
	}
}

func (m *Manager) deliver(c *conn, f *protocol.Frame) {
	m.mu.Lock()
	ch, ok := m.pending[f.RequestID]
	ok = ok && m.pendingPeer[f.RequestID] == c
	if ok {
		delete(m.pendingPeer, f.RequestID)
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
	if !m.reserveReplay(f.RequestID, time.Now()) {
		log.Printf("rpc: replay or cache capacity rejected request_id=%x peer=%s", f.RequestID, c.remote)
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, m.timeout)
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
				if now.Sub(t) >= replayWindow {
					delete(m.replayCache, id)
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *Manager) Call(ctx context.Context, remote nodeid.ID, addr string, req *protocol.Frame) (*protocol.Frame, error) {
	if len(req.Payload) > int(m.maxPayload) {
		return nil, fmt.Errorf("configured payload limit exceeded")
	}
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
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
	if _, exists := m.pending[req.RequestID]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("request id already pending")
	}
	m.pending[req.RequestID] = ch
	m.pendingPeer[req.RequestID] = c
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.pending, req.RequestID)
		delete(m.pendingPeer, req.RequestID)
		m.mu.Unlock()
	}()
	if err := c.write(req); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.done:
		return nil, fmt.Errorf("RPC manager closed")
	case resp := <-ch:
		if resp.Version != req.Version {
			return nil, fmt.Errorf("unexpected response version %d", resp.Version)
		}
		if resp.Type == protocol.FrameError {
			return nil, fmt.Errorf("remote error: %s", string(resp.Payload))
		}
		if !validResponse(req.Type, resp.Type) {
			return nil, fmt.Errorf("unexpected response type %d for request %d", resp.Type, req.Type)
		}
		return resp, nil
	}
}

func (m *Manager) DialUnverified(ctx context.Context, addr string) (nodeid.ID, error) {
	select {
	case <-m.done:
		return nodeid.ID{}, fmt.Errorf("RPC manager closed")
	default:
	}
	config := &tls.Config{
		Certificates:       []tls.Certificate{m.tlsCert},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
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
	c.dec.MaxPayload = m.maxPayload
	c.remote = remoteID

	m.mu.Lock()
	if _, ok := m.conns[remoteID]; ok {
		m.mu.Unlock()
		_ = nc.Close()
		return remoteID, nil
	}
	m.conns[remoteID] = c
	m.allConns[c] = struct{}{}
	m.mu.Unlock()

	go m.readLoop(c)
	return remoteID, nil
}

func (m *Manager) getOrDial(ctx context.Context, remote nodeid.ID, addr string) (*conn, error) {
	select {
	case <-m.done:
		return nil, fmt.Errorf("RPC manager closed")
	default:
	}
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
		MaxVersion:         tls.VersionTLS13,
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
	c.dec.MaxPayload = m.maxPayload
	c.remote = remote

	m.mu.Lock()
	select {
	case <-m.done:
		m.mu.Unlock()
		nc.Close()
		return nil, fmt.Errorf("RPC manager closed")
	default:
	}
	if existing, ok := m.conns[remote]; ok {
		m.mu.Unlock()
		_ = nc.Close()
		return existing, nil
	}
	m.conns[remote] = c
	m.allConns[c] = struct{}{}
	m.mu.Unlock()

	go m.readLoop(c)
	return c, nil
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() { close(m.done); m.cancel() })
	m.mu.Lock()
	defer m.mu.Unlock()
	for c := range m.incoming {
		c.Close()
	}
	m.incoming = make(map[net.Conn]struct{})
	for c := range m.allConns {
		c.close()
	}
	m.allConns = make(map[*conn]struct{})
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

func (m *Manager) SetMaxPayload(limit uint32) {
	m.maxPayload = limit
}

func (m *Manager) Connect(ctx context.Context, remote nodeid.ID, address string) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	_, err := m.getOrDial(ctx, remote, address)
	return err
}

func (m *Manager) SetAdmissionAllowlist(ids []nodeid.ID) {
	if ids == nil {
		m.admission = nil
		return
	}
	m.admission = make(map[nodeid.ID]bool, len(ids))
	for _, id := range ids {
		m.admission[id] = true
	}
}

func validResponse(request, response protocol.FrameType) bool {
	switch request {
	case protocol.FramePing:
		return response == protocol.FramePong
	case protocol.FrameFindNodeRequest:
		return response == protocol.FrameFindNodeResponse
	case protocol.FrameStoreRequest:
		return response == protocol.FrameStoreResponse
	case protocol.FrameFindValueRequest:
		return response == protocol.FrameFindValueResponse
	case protocol.FrameTunnelBuild:
		return response == protocol.FrameTunnelBuildOK || response == protocol.FrameTunnelBuildFail
	case protocol.FrameTunnelData:
		return response == protocol.FrameTunnelAck
	case protocol.FrameTunnelClose:
		return response == protocol.FrameTunnelCloseOk
	case protocol.FrameHello:
		return response == protocol.FrameHelloOK
	}
	return false
}

const replayWindow = 2 * time.Minute

func (m *Manager) reserveReplay(id [16]byte, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seen, ok := m.replayCache[id]; ok && now.Sub(seen) < replayWindow {
		return false
	}
	if _, ok := m.replayCache[id]; !ok && len(m.replayCache) >= 100000 {
		return false
	}
	m.replayCache[id] = now
	return true
}
