package node

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/nodeid"
)

func mustFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func startNode(t *testing.T, port, k int) (*Node, context.CancelFunc) {
	t.Helper()
	cfg := &config.Config{
		Node: config.NodeConfig{
			Port: port, StateDir: t.TempDir(),
			ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
		},
		Protocol: config.ProtocolConfig{Codec: "msgpack"},
		DHT:      config.DHTConfig{K: k, Alpha: 3, Replication: 3},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	n.Addr()
	return n, func() { cancel(); n.Stop() }
}

func inRT(n *Node, id nodeid.ID) bool {
	for _, c := range n.RT.Closest(n.ID, 1<<12) {
		if c.ID == id {
			return true
		}
	}
	return false
}

func idSameBucket(self, template nodeid.ID) nodeid.ID {
	d := nodeid.Distance(self, template)
	for i := 0; i < nodeid.Size; i++ {
		if d[i] == 0 {
			continue
		}
		for bit := 7; bit >= 0; bit-- {
			if d[i]&(1<<uint(bit)) != 0 {
				id := self
				id[i] ^= 1 << uint(bit)
				return id
			}
		}
	}
	return self
}

func TestLRULiveContactRetained(t *testing.T) {
	nA, stopA := startNode(t, mustFreePort(t), 1)
	defer stopA()
	nB, stopB := startNode(t, mustFreePort(t), 1)
	defer stopB()

	ctx := context.Background()

	if err := nA.AddContactForTest(ctx, nB.ID, nB.Addr()); err != nil {
		t.Fatalf("add B: %v", err)
	}
	if !inRT(nA, nB.ID) {
		t.Fatal("B должен быть в RT после добавления")
	}

	fakeC := idSameBucket(nA.ID, nB.ID)
	if fakeC == nA.ID || fakeC == nB.ID {
		t.Fatal("тест некорректен: fakeC коллидирует с self или B")
	}
	if nA.RT.BucketIndexFor(fakeC) != nA.RT.BucketIndexFor(nB.ID) {
		t.Fatalf("тест некорректен: fakeC(%d) и B(%d) не в одном bucket",
			nA.RT.BucketIndexFor(fakeC), nA.RT.BucketIndexFor(nB.ID))
	}

	_ = nA.AddContactForTest(ctx, fakeC, fmt.Sprintf("127.0.0.1:%d", mustFreePort(t)))

	if !inRT(nA, nB.ID) {
		t.Fatal("живой B должен сохраниться в bucket после PING")
	}
	if inRT(nA, fakeC) {
		t.Fatal("fakeC должен был быть отвергнут (LRU жив)")
	}
}

func TestLRUDeadContactReplaced(t *testing.T) {
	nA, stopA := startNode(t, mustFreePort(t), 1)
	defer stopA()
	nB, stopB := startNode(t, mustFreePort(t), 1)

	ctx := context.Background()

	if err := nA.AddContactForTest(ctx, nB.ID, nB.Addr()); err != nil {
		t.Fatalf("add B: %v", err)
	}

	stopB()
	time.Sleep(300 * time.Millisecond)

	fakeC := idSameBucket(nA.ID, nB.ID)
	if nA.RT.BucketIndexFor(fakeC) != nA.RT.BucketIndexFor(nB.ID) {
		t.Fatalf("тест некорректен: fakeC(%d) и B(%d) не в одном bucket",
			nA.RT.BucketIndexFor(fakeC), nA.RT.BucketIndexFor(nB.ID))
	}

	if err := nA.AddContactForTest(ctx, fakeC, fmt.Sprintf("127.0.0.1:%d", mustFreePort(t))); err != nil {
		t.Fatalf("add C: %v", err)
	}

	if inRT(nA, nB.ID) {
		t.Fatal("мёртвый B должен быть вытеснен")
	}
	if !inRT(nA, fakeC) {
		t.Fatal("fakeC должен заменить мёртвого B")
	}
}
