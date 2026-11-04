package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/records"
)

func TestReplicationAndFailure(t *testing.T) {
	var nodes []*node.Node
	var cancels []context.CancelFunc

	basePort := 9900
	for i := 0; i < 5; i++ {
		cfg := &config.Config{
			Node: config.NodeConfig{
				Port: basePort + i, StateDir: t.TempDir(), ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
			},
			Protocol: config.ProtocolConfig{Codec: "msgpack"},
			DHT:      config.DHTConfig{K: 3, Alpha: 3, Replication: 3},
			Bootstrap: config.BootstrapConfig{
				Peers: func() []string {
					if i == 0 {
						return nil
					}
					return []string{fmt.Sprintf("127.0.0.1:%d", basePort)}
				}(),
			},
		}
		n, err := node.New(cfg)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		go n.Run(ctx)

		nodes = append(nodes, n)
		cancels = append(cancels, cancel)
		time.Sleep(100 * time.Millisecond)
	}

	defer func() {
		for i := range cancels {
			cancels[i]()
			nodes[i].Stop()
		}
	}()

	time.Sleep(1 * time.Second)

	id, _ := identity.LoadOrCreate(t.TempDir())
	rec := &records.NodeRecord{
		NodeID:    id.NodeID(),
		PublicKey: id.PublicKey,
		Addresses: []string{"127.0.0.1:1111"},
		Seq:       1,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
	}
	rec.Sign(id)

	err := nodes[1].PutRecord(context.Background(), rec)
	if err != nil {
		t.Fatalf("PutRecord failed: %v", err)
	}

	foundRec, err := nodes[3].GetRecord(context.Background(), rec.DHTKey())
	if err != nil {
		t.Fatalf("GetRecord failed: %v", err)
	}
	if foundRec.NodeID != rec.NodeID {
		t.Fatal("wrong record found")
	}

	cancels[2]()
	nodes[2].Stop()
	time.Sleep(500 * time.Millisecond)

	foundRec2, err := nodes[0].GetRecord(context.Background(), rec.DHTKey())
	if err != nil {
		t.Fatalf("GetRecord after failure failed: %v", err)
	}
	if foundRec2.Seq != rec.Seq {
		t.Fatal("wrong record found after failure")
	}
}
