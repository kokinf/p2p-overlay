package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/records"
)

func measureFileTransferTime(t *testing.T, strategy string, fileSize int) (float64, bool) {
	t.Helper()

	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i % 256)
	}
	filename := fmt.Sprintf("bench_%d.bin", fileSize)
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, filename)
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	const N = 7
	basePort := 9860 + int(time.Now().UnixNano()%100)*10
	var nodes []*node.Node
	var cancels []context.CancelFunc

	for i := 0; i < N; i++ {
		cfg := &config.Config{
			Node: config.NodeConfig{
				Port: basePort + i, StateDir: t.TempDir(),
				ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
			},
			Files:    config.FileConfig{DownloadDir: t.TempDir()},
			Protocol: config.ProtocolConfig{Codec: "msgpack"},
			DHT:      config.DHTConfig{K: 3, Alpha: 3, Replication: 3, RelayStrategy: strategy},
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

	time.Sleep(3 * time.Second)

	for _, nd := range nodes {
		rec := &records.NodeRecord{
			NodeID: nd.ID, PublicKey: nd.Identity.PublicKey,
			Addresses: []string{nd.Addr()}, Seq: 1,
			IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
		}
		rec.Seq = uint64(time.Now().UnixNano())
		rec.Sign(nd.Identity)
		if err := nd.PublishRecord(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1 * time.Second)

	sender := nodes[0]
	receiver := nodes[N-1]

	start := time.Now()
	if err := sender.SendFile(context.Background(), receiver.ID, srcPath); err != nil {
		t.Logf("SendFile error: %v", err)
		return 0, false
	}

	downloadPath := filepath.Join(receiver.Config.Files.Directory(), filename)
	for i := 0; i < 100; i++ {
		if data, err := os.ReadFile(downloadPath); err == nil && bytes.Equal(data, content) {
			elapsed := time.Since(start).Seconds()
			return elapsed, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0, false
}

func TestRelayRatingComparison(t *testing.T) {
	const fileSize = 2 * 1024 * 1024 // 2 МБ
	const runs = 3

	type result struct {
		strategy string
		times    []float64
		success  int
	}

	results := []result{
		{strategy: "random"},
		{strategy: "rating"},
	}

	for idx := range results {
		r := &results[idx]
		t.Logf("Running strategy: %s (%d runs)", r.strategy, runs)
		for i := 0; i < runs; i++ {
			elapsed, ok := measureFileTransferTime(t, r.strategy, fileSize)
			if ok {
				r.times = append(r.times, elapsed)
				r.success++
				t.Logf("  run %d: %.2f sec", i+1, elapsed)
			} else {
				t.Logf("  run %d: FAILED", i+1)
			}
		}
	}

	t.Logf("=== Relay rating results ===")
	for _, r := range results {
		if len(r.times) > 0 {
			sum := 0.0
			for _, v := range r.times {
				sum += v
			}
			avg := sum / float64(len(r.times))
			t.Logf("%-8s: %d/%d runs, avg=%.2f sec", r.strategy, r.success, runs, avg)
		} else {
			t.Logf("%-8s: 0/%d runs", r.strategy, runs)
		}
	}

	if results[0].success != runs || results[1].success != runs {
		t.Fatal("Не все передачи завершились успешно")
	}
}
