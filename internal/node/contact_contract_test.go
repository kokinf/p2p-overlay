package node

import (
	"context"
	"fmt"
	"net"
	"p2p-overlay/internal/config"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"sync/atomic"
	"testing"
	"time"
)

func contractNode(t *testing.T, pins map[string]string) *Node {
	t.Helper()
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir(), AdvertiseAddress: "127.0.0.1:9999", ReadTimeout: time.Second}, DHT: config.DHTConfig{K: 3}, Bootstrap: config.BootstrapConfig{Pins: pins}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.RPC.Close)
	return n
}
func contractPeer(t *testing.T, handler rpc.Handler) (*identity.Identity, string) {
	t.Helper()
	id, err := identity.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := rpc.NewManager(id, serialization.MsgPackCodec{}, handler, time.Second)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); m.Close(); ln.Close() })
	go m.Serve(ctx, ln)
	return id, ln.Addr().String()
}
func TestContactResponseContract(t *testing.T) {
	n := contractNode(t, nil)
	target := nodeid.ID{}
	idA := nodeid.ID{1}
	idB := nodeid.ID{2}
	a := rpc.ContactWire{NodeID: idA[:], Address: "127.0.0.1:9000"}
	b := rpc.ContactWire{NodeID: idB[:], Address: "node-b:9001"}
	good, err := n.validateContacts([]rpc.ContactWire{a, b}, target)
	if err != nil || len(good) != 2 || good[0].Verified || !good[0].LastSeen.IsZero() {
		t.Fatalf("not a candidate: %+v %v", good, err)
	}
	cases := map[string][]rpc.ContactWire{"limit": {a, b, a, b}, "duplicate": {a, a}, "order": {b, a}, "identity": {{NodeID: []byte{1}, Address: a.Address}}, "port": {{NodeID: idA[:], Address: "127.0.0.1:65536"}}, "empty": {{NodeID: idA[:]}}, "host": {{NodeID: idA[:], Address: "bad/host:9000"}}}
	for _, address := range []string{"0.0.0.0:9000", "[::]:9000", "224.0.0.1:9000", "255.255.255.255:9000", "127.0.0.1:+9000"} {
		cases[address] = []rpc.ContactWire{{NodeID: idA[:], Address: address}}
	}
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := n.validateContacts(wire, target); err == nil {
				t.Fatal("malformed response accepted")
			}
		})
	}
}
func TestReferralPromotionRequiresDirectRPC(t *testing.T) {
	codec := serialization.MsgPackCodec{}
	b, addrB := contractPeer(t, func(_ context.Context, _ nodeid.ID, _ *protocol.Frame) (*protocol.Frame, error) {
		payload, _ := codec.Marshal(rpc.FindNodeResponse{})
		return &protocol.Frame{Type: protocol.FrameFindNodeResponse, Payload: payload}, nil
	})
	idB := b.NodeID()
	a, addrA := contractPeer(t, func(_ context.Context, _ nodeid.ID, _ *protocol.Frame) (*protocol.Frame, error) {
		payload, _ := codec.Marshal(rpc.FindNodeResponse{Contacts: []rpc.ContactWire{{NodeID: idB[:], Address: addrB}}})
		return &protocol.Frame{Type: protocol.FrameFindNodeResponse, Payload: payload}, nil
	})
	n := contractNode(t, nil)
	peer := &dht.Contact{ID: a.NodeID(), Address: addrA}
	referrals, err := n.findNodeRPC(context.Background(), peer, idB)
	if err != nil {
		t.Fatal(err)
	}
	if len(referrals) != 1 || referrals[0].Verified || len(n.RT.Closest(idB, 0)) != 0 {
		t.Fatal("referral promoted without direct exchange")
	}
	if err := n.AddContactForTest(context.Background(), a.NodeID(), addrA); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Lookup(context.Background(), idB); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range n.RT.Closest(idB, 0) {
		if c.ID == idB {
			found = c.Verified && c.Verification == "tls_outbound_rpc"
		}
	}
	if !found {
		t.Fatal("successfully authenticated candidate not promoted")
	}
	fake := nodeid.ID{42}
	n.RT.Remove(idB)
	_, err = n.findNodeRPC(context.Background(), &dht.Contact{ID: fake, Address: addrB}, idB)
	if err == nil {
		t.Fatal("substituted identity accepted")
	}
	for _, c := range n.RT.Closest(fake, 0) {
		if c.ID == fake {
			t.Fatal("unauthenticated candidate promoted")
		}
	}
}
func TestPinnedBootstrap(t *testing.T) {
	var actions atomic.Int32
	seed, address := contractPeer(t, func(_ context.Context, _ nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
		actions.Add(1)
		if req.Type != protocol.FramePing {
			return nil, fmt.Errorf("unexpected request")
		}
		return &protocol.Frame{Type: protocol.FramePong}, nil
	})
	n := contractNode(t, map[string]string{address: seed.NodeID().String()})
	if err := n.bootstrapOne(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	if len(n.RT.Closest(seed.NodeID(), 0)) != 1 {
		t.Fatal("pinned seed not learned")
	}
	other := contractNode(t, map[string]string{address: nodeid.ID{77}.String()})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := other.bootstrapOne(ctx, address); err == nil {
		t.Fatal("wrong seed key accepted")
	}
	if len(other.RT.Closest(seed.NodeID(), 0)) != 0 {
		t.Fatal("wrong seed added")
	}
	if actions.Load() != 1 {
		t.Fatal("substituted seed triggered an application request")
	}
	if err := contractNode(t, nil).bootstrapOne(context.Background(), address); err == nil {
		t.Fatal("address-only seed accepted without explicit opt-in")
	}
}

func TestReplicationReportDoesNotCountLocalCopy(t *testing.T) {
	n := contractNode(t, nil)
	codec := serialization.MsgPackCodec{}
	var rejected atomic.Int32
	for index := 0; index < 3; index++ {
		i := index
		peer, addr := contractPeer(t, func(_ context.Context, _ nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
			switch req.Type {
			case protocol.FramePing:
				return &protocol.Frame{Type: protocol.FramePong}, nil
			case protocol.FrameFindNodeRequest:
				b, _ := codec.Marshal(rpc.FindNodeResponse{})
				return &protocol.Frame{Type: protocol.FrameFindNodeResponse, Payload: b}, nil
			case protocol.FrameStoreRequest:
				b, _ := codec.Marshal(rpc.StoreResponse{Success: i >= int(rejected.Load())})
				return &protocol.Frame{Type: protocol.FrameStoreResponse, Payload: b}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		})
		if err := n.AddContactForTest(context.Background(), peer.NodeID(), addr); err != nil {
			t.Fatal(err)
		}
	}
	rec := &records.NodeRecord{NodeID: n.ID, PublicKey: n.Identity.PublicKey, Addresses: []string{n.Addr()}, Seq: 1, IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix()}
	rec.Sign(n.Identity)
	rejected.Store(1)
	r, err := n.PutRecordWithReport(context.Background(), rec)
	if err != nil || !r.LocalCopy.Acknowledged || r.Confirmations != 2 || len(r.Holders) != 3 {
		t.Fatalf("expected two remote ACKs: %+v %v", r, err)
	}
	failed := 0
	for _, h := range r.Holders {
		if !h.Acknowledged {
			failed++
			if h.Error == "" {
				t.Fatal("missing per-holder error")
			}
		}
	}
	if failed != 1 {
		t.Fatal("missing rejected holder")
	}
	rejected.Store(2)
	r, err = n.PutRecordWithReport(context.Background(), rec)
	if err == nil || r.Confirmations != 1 || !r.LocalCopy.Acknowledged {
		t.Fatal("local copy incorrectly satisfied remote quorum")
	}
}

func TestFailedInitialIdentityPublicationIsRetried(t *testing.T) {
	var nodes []*Node
	start := func(seed *Node) *Node {
		cfg := &config.Config{Node: config.NodeConfig{StateDir: t.TempDir(), ReadTimeout: time.Second, WriteTimeout: time.Second}, DHT: config.DHTConfig{K: 4, RefreshInterval: 100 * time.Millisecond}}
		if seed != nil {
			cfg.Bootstrap.Peers = []string{seed.Addr()}
			cfg.Bootstrap.Pins = map[string]string{seed.Addr(): seed.ID.String()}
		}
		n, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() { cancel(); n.Stop() })
		go n.Run(ctx)
		_ = n.Addr()
		nodes = append(nodes, n)
		return n
	}
	owner := start(nil)
	deadline := time.Now().Add(3 * time.Second)
	for !owner.selfRecordPending.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !owner.selfRecordPending.Load() {
		t.Fatal("initial unavailable quorum not recorded")
	}
	for i := 0; i < 3; i++ {
		start(owner)
	}
	key := dhtKeyForNode(owner.ID)
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		copies := 0
		for _, n := range nodes[1:] {
			if _, ok := n.Store.Get(key); ok {
				copies++
			}
		}
		if copies == 3 && !owner.selfRecordPending.Load() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("owner did not automatically retry failed initial publication after peers joined")
}
