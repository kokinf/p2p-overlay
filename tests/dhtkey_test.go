package tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
)

func TestStoreRejectsKeyMismatch(t *testing.T) {
	port := mustFreePort(t)
	dirB := t.TempDir()
	cfgB := &config.Config{
		Node: config.NodeConfig{
			Port: port, StateDir: dirB,
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
	rec := &records.NodeRecord{
		NodeID:    idA.NodeID(),
		PublicKey: idA.PublicKey,
		Addresses: []string{"127.0.0.1:9999"},
		Seq:       1,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}
	rec.Sign(idA)

	wrongKey := rec.DHTKey()
	wrongKey[0] ^= 0xFF

	recBytes, _ := msgpack.Marshal(rec)
	reqPayload, _ := msgpack.Marshal(rpc.StoreRequest{Key: wrongKey[:], Record: recBytes})

	handlerNoop := func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		return &protocol.Frame{Type: protocol.FramePong}, nil
	}
	mA := rpc.NewManager(idA, serialization.MsgPackCodec{}, handlerNoop, 2*time.Second)
	defer mA.Close()

	callCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err = mA.Call(callCtx, nB.ID, addrB, &protocol.Frame{
		Version: 1, Type: protocol.FrameStoreRequest, Payload: reqPayload,
	})
	if err == nil {
		t.Fatal("expected rejection of mismatched key")
	}
	if !errors.Is(err, context.DeadlineExceeded) && err.Error() == "" {
		t.Fatalf("unexpected error: %v", err)
	}

	if size := nB.Store.Size(); size != 0 {
		t.Fatalf("expected empty store, got %d records", size)
	}
}

func mustFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

var _ = fmt.Sprintf
