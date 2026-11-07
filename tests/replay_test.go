package tests

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/rpc"
)

func startReceiver(t *testing.T, calls *int32) (nodeid.ID, string, func()) {
	t.Helper()
	idB, _ := identity.LoadOrCreate(t.TempDir())
	handler := func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		if req.Type == protocol.FramePing {
			atomic.AddInt32(calls, 1)
			return &protocol.Frame{Type: protocol.FramePong}, nil
		}
		return nil, nil
	}
	mB := rpc.NewManager(idB, serialization.MsgPackCodec{}, handler, 2*time.Second)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go mB.Serve(ctx, ln)
	return idB.NodeID(), ln.Addr().String(), func() {
		cancel()
		mB.Close()
		ln.Close()
	}
}

func TestReplayInCurrentSessionRejected(t *testing.T) {
	var calls int32
	targetID, addr, stop := startReceiver(t, &calls)
	defer stop()

	idA, _ := identity.LoadOrCreate(t.TempDir())
	mA := rpc.NewManager(idA, serialization.MsgPackCodec{},
		func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
			return &protocol.Frame{Type: protocol.FramePong}, nil
		}, 2*time.Second)
	defer mA.Close()

	rid := [16]byte{1, 2, 3, 4}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := mA.Call(ctx1, targetID, addr, &protocol.Frame{Version: 1, Type: protocol.FramePing, RequestID: rid})
	cancel1()
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = mA.Call(ctx2, targetID, addr, &protocol.Frame{Version: 1, Type: protocol.FramePing, RequestID: rid})
	cancel2()
	if err == nil {
		t.Fatal("expected timeout on replay")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("replay must not increment handler count, got %d", calls)
	}
}

func TestReplayFromClosedSessionRejected(t *testing.T) {
	var calls int32
	targetID, addr, stop := startReceiver(t, &calls)
	defer stop()

	idA, _ := identity.LoadOrCreate(t.TempDir())
	rid := [16]byte{0xAB, 0xCD, 0xEF, 0x01}

	mA1 := rpc.NewManager(idA, serialization.MsgPackCodec{},
		func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
			return &protocol.Frame{Type: protocol.FramePong}, nil
		}, 2*time.Second)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := mA1.Call(ctx1, targetID, addr, &protocol.Frame{Version: 1, Type: protocol.FramePing, RequestID: rid})
	cancel1()
	if err != nil {
		t.Fatalf("session-1 call failed: %v", err)
	}
	mA1.Close()

	mA2 := rpc.NewManager(idA, serialization.MsgPackCodec{},
		func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
			return &protocol.Frame{Type: protocol.FramePong}, nil
		}, 2*time.Second)
	defer mA2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = mA2.Call(ctx2, targetID, addr, &protocol.Frame{Version: 1, Type: protocol.FramePing, RequestID: rid})
	cancel2()
	if err == nil {
		t.Fatal("expected replay from closed session to be rejected")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("replay must not increment handler count, got %d", calls)
	}
}
