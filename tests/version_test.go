package tests

import (
	"context"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/rpc"
)

func TestUnknownProtocolVersionRejected(t *testing.T) {
	port := mustFreePort(t)
	cfgB := &config.Config{
		Node: config.NodeConfig{
			Port: port, StateDir: t.TempDir(),
			ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		},
		Protocol: config.ProtocolConfig{Codec: "msgpack"},
		DHT:      config.DHTConfig{K: 3, Alpha: 3, Replication: 3},
	}
	nB, err := node.New(cfgB)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go nB.Run(ctx)
	defer nB.Stop()
	addrB := nB.Addr()

	idA, _ := identity.LoadOrCreate(t.TempDir())
	handler := func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Type: protocol.FramePong}, nil
	}
	mA := rpc.NewManager(idA, serialization.MsgPackCodec{}, handler, 2*time.Second)
	defer mA.Close()

	callCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = mA.Call(callCtx, nB.ID, addrB, &protocol.Frame{
		Version: 99, Type: protocol.FramePing,
	})
	cancel2()
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}

	callCtx2, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()
	resp, err := mA.Call(callCtx2, nB.ID, addrB, &protocol.Frame{
		Version: 1, Type: protocol.FramePing,
	})
	if err != nil {
		t.Fatalf("node should survive bad version: %v", err)
	}
	if resp.Type != protocol.FramePong {
		t.Fatalf("expected PONG, got %d", resp.Type)
	}
}

func TestMalformedRPCPayloadAndUnknownType(t *testing.T) {
	cfg := &config.Config{Node: config.NodeConfig{Port: mustFreePort(t), StateDir: t.TempDir(), ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second}, DHT: config.DHTConfig{K: 3}, Protocol: config.ProtocolConfig{Codec: "msgpack"}}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)
	defer n.Stop()
	addr := n.Addr()
	id, _ := identity.LoadOrCreate(t.TempDir())
	m := rpc.NewManager(id, serialization.MsgPackCodec{}, nil, 2*time.Second)
	defer m.Close()
	for _, f := range []*protocol.Frame{{Type: protocol.FramePing, Payload: []byte{0xc1}}, {Type: protocol.FrameType(255)}} {
		if _, err = m.Call(ctx, n.ID, addr, f); err == nil {
			t.Fatal("invalid RPC accepted")
		}
	}
	if _, err = m.Call(ctx, n.ID, addr, &protocol.Frame{Type: protocol.FramePing}); err != nil {
		t.Fatal("node failed after malformed input", err)
	}
}
