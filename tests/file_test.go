package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"p2p-overlay/internal/tunnel"
)

func startTestNetwork(t *testing.T, n, basePort int) ([]*node.Node, func()) {
	t.Helper()
	var nodes []*node.Node
	var cancels []context.CancelFunc

	for i := 0; i < n; i++ {
		cfg := &config.Config{
			Node: config.NodeConfig{
				Port: basePort + i, StateDir: t.TempDir(),
				ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
			},
			Files:    config.FileConfig{DownloadDir: t.TempDir()},
			Protocol: config.ProtocolConfig{Codec: "msgpack"},
			DHT:      config.DHTConfig{K: 3, Alpha: 3, Replication: 3, RelayStrategy: "random"},
			Bootstrap: config.BootstrapConfig{
				Peers: func() []string {
					if i == 0 {
						return nil
					}
					return []string{fmt.Sprintf("127.0.0.1:%d", basePort)}
				}(),
			},
		}
		if path := os.Getenv("P2P_FILE_CONFIG"); path != "" {
			profile, err := config.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Files.Enabled = profile.Files.Enabled
		}
		cfg.Bootstrap.AllowUnpinned = false
		if len(cfg.Bootstrap.Peers) > 0 {
			cfg.Bootstrap.Pins = map[string]string{cfg.Bootstrap.Peers[0]: nodes[0].ID.String()}
		}

		nodeInst, err := node.New(cfg)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go nodeInst.Run(ctx)
		nodes = append(nodes, nodeInst)
		cancels = append(cancels, cancel)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	for _, nd := range nodes {
		rec := &records.NodeRecord{
			NodeID:    nd.ID,
			PublicKey: nd.Identity.PublicKey,
			Addresses: []string{nd.Addr()},
			Seq:       1,
			IssuedAt:  time.Now().Unix(),
			ExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
		}
		rec.Seq = uint64(time.Now().UnixNano())
		rec.Sign(nd.Identity)
		if err := nd.PublishRecord(context.Background(), rec); err != nil {
			t.Fatalf("publish record: %v", err)
		}
	}
	time.Sleep(1 * time.Second)

	return nodes, func() {
		for i := range cancels {
			cancels[i]()
			nodes[i].Stop()
		}
	}
}

func runFileTransferTest(t *testing.T, size int, basePort int) {
	t.Helper()

	content := make([]byte, size)
	for i := range content {
		content[i] = byte((i * 7) % 256)
	}
	filename := fmt.Sprintf("test_%d.bin", size)
	srcPath := filepath.Join(t.TempDir(), filename)
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	nodes, stop := startTestNetwork(t, 5, basePort)
	defer stop()

	sender := nodes[0]
	receiver := nodes[4]

	start := time.Now()
	if err := sender.SendFile(context.Background(), receiver.ID, srcPath); err != nil {
		t.Fatalf("SendFile: %v", err)
	}

	downloadPath := filepath.Join(receiver.Config.Files.Directory(), filename)
	var received []byte
	for i := 0; i < 100; i++ {
		data, err := os.ReadFile(downloadPath)
		if err == nil && len(data) == len(content) {
			received = data
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if received == nil {
		t.Fatalf("файл не был собран за 20 секунд")
	}

	expectedHash := sha256.Sum256(content)
	actualHash := sha256.Sum256(received)
	if !bytes.Equal(expectedHash[:], actualHash[:]) {
		t.Fatalf("SHA-256 mismatch: got %x, want %x", actualHash, expectedHash)
	}

	if !bytes.Equal(content, received) {
		t.Fatalf("содержимое файла не совпадает")
	}

	elapsed := time.Since(start).Seconds()
	t.Logf("FILE_METRIC bytes=%d seconds=%.9f goodput_Bps=%.3f sha256=%x", size, elapsed, float64(size)/elapsed, actualHash)
}

func TestSendFile_Small(t *testing.T) {
	runFileTransferTest(t, 1*1024, 9700)
}

func TestSendFile_Medium(t *testing.T) {
	runFileTransferTest(t, 100*1024, 9720)
}

func TestSendFile_Large(t *testing.T) {
	runFileTransferTest(t, 1024*1024, 9740)
}

func TestFinalizeFile_RejectsCorruptedBlock(t *testing.T) {

	cfg := &config.Config{
		Node: config.NodeConfig{
			Port: 9999, StateDir: t.TempDir(),
			ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		},
		Files:    config.FileConfig{DownloadDir: t.TempDir()},
		Protocol: config.ProtocolConfig{Codec: "msgpack"},
		DHT:      config.DHTConfig{K: 3, Alpha: 3, Replication: 3, RelayStrategy: "random"},
	}
	nd, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	original := []byte("hello world, this is test data")
	expectedHash := sha256.Sum256(original)

	validState := &tunnel.FileReceiveState{
		Meta: rpc.FileMetadataRequest{
			Filename:    "valid.bin",
			Size:        int64(len(original)),
			SHA256:      expectedHash[:],
			TotalBlocks: 1,
		},
		Blocks:   [][]byte{original},
		Received: 1,
	}
	if err := nd.FinalizeFile(validState); err != nil {
		t.Fatalf("валидный файл отклонен: %v", err)
	}
	if _, err := os.Stat(filepath.Join(nd.Config.Files.Directory(), "valid.bin")); err != nil {
		t.Fatalf("валидный файл не записан: %v", err)
	}

	corrupted := make([]byte, len(original))
	copy(corrupted, original)
	corrupted[0] ^= 0xFF

	corruptedState := &tunnel.FileReceiveState{
		Meta: rpc.FileMetadataRequest{
			Filename:    "corrupted.bin",
			Size:        int64(len(original)),
			SHA256:      expectedHash[:],
			TotalBlocks: 1,
		},
		Blocks:   [][]byte{corrupted},
		Received: 1,
	}
	if err := nd.FinalizeFile(corruptedState); err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("expected SHA-256 rejection, got %v", err)
	}

	if _, err := os.Stat(filepath.Join(nd.Config.Files.Directory(), "corrupted.bin")); err == nil {
		t.Fatal("повреждённый файл был записан на диск (это баг!)")
	}

	t.Logf("OK: валидный файл принят, повреждённый — отклонён")
}

func TestFileServiceDisabled(t *testing.T) {
	disabled := false
	nd, err := node.New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 3}, Files: config.FileConfig{Enabled: &disabled, DownloadDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer nd.RPC.Close()
	if err := nd.SendFile(context.Background(), nd.ID, "missing-file"); err == nil || err.Error() != "file service disabled" {
		t.Fatalf("unexpected result: %v", err)
	}
	if err := nd.FinalizeFile(&tunnel.FileReceiveState{}); err == nil {
		t.Fatal("disabled receiver accepted file")
	}
}

func TestFileProfiles(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		cfg, err := config.Load(filepath.Join("..", "internal", "configs", "files-"+name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Files.IsEnabled() != enabled {
			t.Fatalf("incorrect profile %s", name)
		}
	}
}
