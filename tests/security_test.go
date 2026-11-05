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

func TestTLSWrongIdentityRejected(t *testing.T) {
	idA, _ := identity.LoadOrCreate(t.TempDir())
	idB, _ := identity.LoadOrCreate(t.TempDir())
	idEvil, _ := identity.LoadOrCreate(t.TempDir())

	handler := func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Type: protocol.FramePong}, nil
	}

	mB := rpc.NewManager(idB, serialization.MsgPackCodec{}, handler, 2*time.Second)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go mB.Serve(context.Background(), ln)

	mA := rpc.NewManager(idA, serialization.MsgPackCodec{}, handler, 2*time.Second)

	_, err := mA.Call(context.Background(), idEvil.NodeID(), ln.Addr().String(), &protocol.Frame{Type: protocol.FramePing})
	if err == nil {
		t.Fatal("TLS handshake accepted wrong identity")
	}
}
