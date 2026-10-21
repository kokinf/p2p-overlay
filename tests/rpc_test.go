package tests

import (
	"context"
	"net"
	"testing"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/rpc"
)

func TestPingRoundtrip(t *testing.T) {
	a, _ := identity.LoadOrCreate(t.TempDir())
	b, _ := identity.LoadOrCreate(t.TempDir())

	handler := func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		if req.Type == protocol.FramePing {
			return &protocol.Frame{Type: protocol.FramePong}, nil
		}
		return nil, nil
	}
	mA := rpc.NewManager(a, serialization.MsgPackCodec{}, handler, 3*time.Second)
	mB := rpc.NewManager(b, serialization.MsgPackCodec{}, handler, 3*time.Second)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mB.Serve(ctx, ln)

	addr := ln.Addr().String()
	bID := b.NodeID()

	callCtx, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	resp, err := mA.Call(callCtx, bID, addr,
		&protocol.Frame{Version: 1, Type: protocol.FramePing})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Type != protocol.FramePong {
		t.Fatalf("expected PONG, got %d", resp.Type)
	}
	if resp.RequestID == ([16]byte{}) {
		t.Fatal("request id not echoed")
	}
}

func TestRPCWrongIdentityRejected(t *testing.T) {
	a, _ := identity.LoadOrCreate(t.TempDir())
	b, _ := identity.LoadOrCreate(t.TempDir())
	c, _ := identity.LoadOrCreate(t.TempDir())

	handler := func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Type: protocol.FramePong}, nil
	}
	mA := rpc.NewManager(a, serialization.MsgPackCodec{}, handler, 3*time.Second)
	mB := rpc.NewManager(b, serialization.MsgPackCodec{}, handler, 3*time.Second)

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mB.Serve(ctx, ln)

	wrongID := c.NodeID()
	callCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err := mA.Call(callCtx, wrongID, ln.Addr().String(),
		&protocol.Frame{Version: 1, Type: protocol.FramePing})
	if err == nil {
		t.Fatal("expected error for id mismatch")
	}
}
