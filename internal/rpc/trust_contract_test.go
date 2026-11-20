package rpc

import (
	"context"
	"net"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"sync/atomic"
	"testing"
	"time"
)

func trustManager(t *testing.T, h Handler) *Manager {
	t.Helper()
	id, err := identity.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(id, serialization.MsgPackCodec{}, h, time.Second)
	t.Cleanup(m.Close)
	return m
}
func serveTrust(t *testing.T, m *Manager) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); ln.Close() })
	go m.Serve(ctx, ln)
	return ln.Addr().String()
}
func TestResponseTypeBoundToRequest(t *testing.T) {
	server := trustManager(t, func(context.Context, nodeid.ID, *protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Type: protocol.FrameFindNodeResponse}, nil
	})
	address := serveTrust(t, server)
	client := trustManager(t, nil)
	if _, err := client.Call(context.Background(), server.ID(), address, &protocol.Frame{Type: protocol.FramePing}); err == nil {
		t.Fatal("wrong response type accepted")
	}
}
func TestAdmissionAllowlist(t *testing.T) {
	var handled atomic.Int32
	server := trustManager(t, func(context.Context, nodeid.ID, *protocol.Frame) (*protocol.Frame, error) {
		handled.Add(1)
		return &protocol.Frame{Type: protocol.FramePong}, nil
	})
	allowed := trustManager(t, nil)
	denied := trustManager(t, nil)
	server.SetAdmissionAllowlist([]nodeid.ID{allowed.ID()})
	address := serveTrust(t, server)
	if _, err := allowed.Call(context.Background(), server.ID(), address, &protocol.Frame{Type: protocol.FramePing}); err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Call(context.Background(), server.ID(), address, &protocol.Frame{Type: protocol.FramePing}); err == nil {
		t.Fatal("unlisted identity admitted")
	}
	if handled.Load() != 1 {
		t.Fatal("denied peer caused an application action")
	}
}
func TestReplayWindowAndManagerRestart(t *testing.T) {
	m := trustManager(t, nil)
	id := [16]byte{1}
	now := time.Now()
	if !m.reserveReplay(id, now) || m.reserveReplay(id, now.Add(replayWindow-time.Nanosecond)) {
		t.Fatal("replay accepted inside window")
	}
	if !m.reserveReplay(id, now.Add(replayWindow)) {
		t.Fatal("expired request remains reserved")
	}
	restarted := NewManager(m.id, serialization.MsgPackCodec{}, nil, time.Second)
	t.Cleanup(restarted.Close)
	if !restarted.reserveReplay(id, now) {
		t.Fatal("new process cache unexpectedly persisted")
	}
	t.Log("same authenticated plaintext request may execute again after 2 minutes or a process restart; old TLS ciphertext remains invalid in a new session")
}

func TestResponseBoundToConnection(t *testing.T) {
	m := trustManager(t, nil)
	id := [16]byte{8}
	expected := &conn{}
	other := &conn{}
	ch := make(chan *protocol.Frame, 1)
	m.pending[id] = ch
	m.pendingPeer[id] = expected
	frame := &protocol.Frame{RequestID: id, Type: protocol.FramePong}
	m.deliver(other, frame)
	if len(ch) != 0 || len(m.pending) != 1 {
		t.Fatal("foreign connection satisfied pending RPC")
	}
	m.deliver(expected, frame)
	if len(ch) != 1 || len(m.pending) != 0 {
		t.Fatal("expected response not delivered")
	}
}
