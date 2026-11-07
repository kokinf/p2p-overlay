package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/records"
)

func TestTunnelDeliveryAndRecovery(t *testing.T) {
	var nodes []*node.Node
	var cancels []context.CancelFunc
	basePort := 9600

	for i := 0; i < 6; i++ {
		cfg := &config.Config{
			Node: config.NodeConfig{
				Port: basePort + i, StateDir: t.TempDir(),
				ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
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
		n, _ := node.New(cfg)
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

	time.Sleep(2 * time.Second)

	for _, n := range nodes {
		tmp := records.NodeRecord{
			NodeID:    n.ID,
			PublicKey: n.Identity.PublicKey,
			Addresses: []string{n.Addr()},
			Seq:       1,
			IssuedAt:  time.Now().Unix(),
			ExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
		}
		tmp.Sign(n.Identity)
		n.PublishRecord(context.Background(), &tmp)
	}
	time.Sleep(1 * time.Second)

	sender := nodes[0]
	receiver := nodes[5]

	tun, err := sender.BuildTunnel(context.Background(), receiver.ID, 3)
	if err != nil {
		t.Fatalf("Ошибка построения: %v", err)
	}
	if len(tun.Path) != 5 {
		t.Fatalf("Неверная длина маршрута: %d", len(tun.Path))
	}

	err = sender.SendMessage(context.Background(), receiver.ID, "Секретное сообщение")
	if err != nil {
		t.Fatalf("Ошибка отправки: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	receiver.AppMu.Lock()
	recvCount := len(receiver.AppRecv)
	receiver.AppMu.Unlock()
	if recvCount != 1 {
		t.Fatalf("Получатель не получил сообщение, recvCount=%d", recvCount)
	}

	r1 := tun.Path[1]
	for i, n := range nodes {
		if n.ID == r1.ID {
			cancels[i]()
			n.Stop()
			break
		}
	}
	time.Sleep(500 * time.Millisecond)

	err = sender.SendMessage(context.Background(), receiver.ID, "Сообщение после отказа")
	if err != nil {
		t.Fatalf("Туннель не восстановился: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	receiver.AppMu.Lock()
	if len(receiver.AppRecv) != 2 {
		t.Errorf("Получатель не получил 2-е сообщение")
	}
	receiver.AppMu.Unlock()
}
