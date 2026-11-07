package tests

import (
	"context"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/node"
)

func TestBootstrapAfterSeedShutdown(t *testing.T) {
	const N = 8
	const K = 1

	type nodeHandle struct {
		n    *node.Node
		stop func()
		addr string
	}

	var nodes []*nodeHandle

	startNode := func(port int, bootstrapPeers []string) *nodeHandle {
		cfg := &config.Config{
			Node: config.NodeConfig{
				Port: port, StateDir: t.TempDir(),
				ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
			},
			Protocol:  config.ProtocolConfig{Codec: "msgpack"},
			DHT:       config.DHTConfig{K: K, Alpha: 3, Replication: 3, RefreshInterval: 5 * time.Second},
			Bootstrap: config.BootstrapConfig{Peers: bootstrapPeers},
		}
		n, err := node.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go n.Run(ctx)
		h := &nodeHandle{n: n, stop: func() { cancel(); n.Stop() }}
		h.addr = n.Addr()
		return h
	}

	seedPort := mustFreePort(t)
	seed := startNode(seedPort, nil)
	nodes = append(nodes, seed)
	time.Sleep(300 * time.Millisecond)

	for i := 1; i < N; i++ {
		h := startNode(mustFreePort(t), []string{seed.addr})
		nodes = append(nodes, h)
		time.Sleep(200 * time.Millisecond)
	}
	defer func() {
		for _, h := range nodes {
			h.stop()
		}
	}()

	time.Sleep(2 * time.Second)

	seed.stop()

	target := nodes[7].n.ID
	initiator := nodes[1].n
	initClosest := initiator.RT.Closest(target, 10)
	for _, c := range initClosest {
		if c.ID == target {
			t.Logf("цель уже в RT инициатора; тест может быть неинформативен")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := initiator.Lookup(ctx, target)
	if err != nil {
		t.Fatalf("lookup после остановки seed: %v", err)
	}
	if res.Iterations < 1 {
		t.Fatalf("lookup не выполнен: iterations=%d", res.Iterations)
	}
	t.Logf("lookup после остановки seed: iterations=%d rpcs=%d closest=%d",
		res.Iterations, res.RPCs, len(res.Closest))
}
