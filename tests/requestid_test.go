package tests

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/rpc"
)

func TestRequestIDMismatchNotDelivered(t *testing.T) {
	idServer, _ := identity.LoadOrCreate(t.TempDir())
	serverCert, err := idServer.GenerateTLSCertificate()
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	tlsLn := tls.NewListener(ln, tlsCfg)

	go func() {
		for {
			c, err := tlsLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				dec := protocol.NewStreamDecoder(c)
				f, err := dec.ReadFrame()
				if err != nil {
					return
				}
				_ = f
				resp := &protocol.Frame{
					Version:   1,
					Type:      protocol.FramePong,
					RequestID: [16]byte{0xDE, 0xAD, 0xBE, 0xEF},
				}
				_ = resp.Encode(c)
			}(c)
		}
	}()

	idA, _ := identity.LoadOrCreate(t.TempDir())
	mA := rpc.NewManager(idA, serialization.MsgPackCodec{},
		func(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
			return nil, nil
		}, 2*time.Second)
	defer mA.Close()

	callCtx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	_, err = mA.Call(callCtx, idServer.NodeID(), ln.Addr().String(), &protocol.Frame{
		Version: 1, Type: protocol.FramePing, RequestID: [16]byte{1, 2, 3, 4},
	})
	if err == nil {
		t.Fatal("expected timeout because response had different request_id")
	}
}
