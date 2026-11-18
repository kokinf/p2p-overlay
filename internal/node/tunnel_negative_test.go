package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"p2p-overlay/internal/config"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/rpc"
	"p2p-overlay/internal/tunnel"
	"path/filepath"
	"testing"
	"time"
)

func TestTunnelRejectsInvalidRoute(t *testing.T) {
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.RPC.Close()
	for _, hop := range []int{-1, 0, 100} {
		b, _ := n.Codec.Marshal(rpc.TunnelBuildRequest{HopIndex: hop})
		if _, err := n.handleTunnelBuild(context.Background(), n.ID, &protocol.Frame{Payload: b}); err == nil {
			t.Fatal("invalid route accepted")
		}
	}
}
func TestTunnelApplicationReplay(t *testing.T) {
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.RPC.Close()
	key := make([]byte, 32)
	tid := [16]byte{1}
	n.Tunnels.PutReceiver(&tunnel.ReceiverState{PrevID: n.ID, TunnelID: tid, SenderID: n.ID, E2EKey: key, ExpiresAt: time.Now().Add(time.Minute)})
	app, _ := n.Codec.Marshal(rpc.AppMessageRequest{MessageID: make([]byte, 16), Type: "text", Payload: []byte("once")})
	nonce := tunnel.RandomNonce()
	ct, err := tunnel.SealData(key, nonce, app, tid[:], 0)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := n.Codec.Marshal(rpc.TunnelDataRequest{TunnelID: tid[:], Nonce: nonce, Ciphertext: ct})
	for i := 0; i < 2; i++ {
		if _, err := n.handleTunnelData(context.Background(), n.ID, &protocol.Frame{Payload: b}); err != nil {
			t.Fatal(err)
		}
	}
	if len(n.AppRecv) != 1 {
		t.Fatalf("processed %d times", len(n.AppRecv))
	}
	newID := [16]byte{3}
	n.Tunnels.PutReceiver(&tunnel.ReceiverState{PrevID: n.ID, TunnelID: newID, SenderID: n.ID, E2EKey: key, ExpiresAt: time.Now().Add(time.Minute)})
	replayCT, err := tunnel.SealData(key, nonce, app, newID[:], 0)
	if err != nil {
		t.Fatal(err)
	}
	replayPayload, _ := n.Codec.Marshal(rpc.TunnelDataRequest{TunnelID: newID[:], Nonce: nonce, Ciphertext: replayCT})
	if _, err = n.handleTunnelData(context.Background(), n.ID, &protocol.Frame{Payload: replayPayload}); err != nil {
		t.Fatal(err)
	}
	if len(n.AppRecv) != 1 {
		t.Fatal("replay through a new tunnel processed twice")
	}
	ct[0] ^= 1
	b, _ = n.Codec.Marshal(rpc.TunnelDataRequest{TunnelID: tid[:], Nonce: nonce, Ciphertext: ct})
	if _, err := n.handleTunnelData(context.Background(), n.ID, &protocol.Frame{Payload: b}); err == nil {
		t.Fatal("corrupted ciphertext accepted")
	}
}

func TestMalformedApplicationData(t *testing.T) {
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.RPC.Close()
	tid := [16]byte{2}
	key := make([]byte, 32)
	n.Tunnels.PutReceiver(&tunnel.ReceiverState{PrevID: n.ID, TunnelID: tid, SenderID: n.ID, E2EKey: key, ExpiresAt: time.Now().Add(time.Minute), Files: make(map[string]*tunnel.FileReceiveState)})
	for _, meta := range []rpc.FileMetadataRequest{{TotalBlocks: -1}, {FileID: make([]byte, 16), Filename: "../escape", Size: 1, BlockSize: 1, TotalBlocks: 1, SHA256: make([]byte, 32)}, {FileID: make([]byte, 16), Filename: "safe", Size: 1 << 40, BlockSize: 1, TotalBlocks: 1, SHA256: make([]byte, 32)}} {
		payload, _ := n.Codec.Marshal(meta)
		app, _ := n.Codec.Marshal(rpc.AppMessageRequest{MessageID: make([]byte, 16), Type: "file_meta", Payload: payload})
		nonce := tunnel.RandomNonce()
		ct, _ := tunnel.SealData(key, nonce, app, tid[:], 1)
		b, _ := n.Codec.Marshal(rpc.TunnelDataRequest{TunnelID: tid[:], Nonce: nonce, Ciphertext: ct, Seq: 1})
		if _, err := n.handleTunnelData(context.Background(), n.ID, &protocol.Frame{Payload: b}); err == nil {
			t.Fatal("malformed metadata accepted")
		}
	}
}
func TestTunnelHeaderAndAckIntegrity(t *testing.T) {
	key := make([]byte, 32)
	id := make([]byte, 16)
	nonce := tunnel.RandomNonce()
	ct, err := tunnel.SealData(key, nonce, []byte("secret"), id, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tunnel.OpenData(key, nonce, ct, id, 2); err == nil {
		t.Fatal("modified sequence accepted")
	}
	tag := tunnel.AckTag(key, id, 1, ct)
	if !tunnel.VerifyAck(key, id, 1, ct, tag) {
		t.Fatal("valid ack rejected")
	}
	tag[0] ^= 1
	if tunnel.VerifyAck(key, id, 1, ct, tag) {
		t.Fatal("forged ack accepted")
	}
}

func TestFileBlocksValidationAndDeduplication(t *testing.T) {
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 3}, Files: config.FileConfig{DownloadDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.RPC.Close()
	tid := [16]byte{8}
	key := make([]byte, 32)
	fileID := make([]byte, 16)
	state := &tunnel.ReceiverState{PrevID: n.ID, TunnelID: tid, SenderID: n.ID, E2EKey: key, ExpiresAt: time.Now().Add(time.Minute), Files: make(map[string]*tunnel.FileReceiveState)}
	n.Tunnels.PutReceiver(state)
	counter := byte(0)
	send := func(kind string, value any) error {
		counter++
		payload, _ := n.Codec.Marshal(value)
		msgID := make([]byte, 16)
		msgID[0] = counter
		app, _ := n.Codec.Marshal(rpc.AppMessageRequest{MessageID: msgID, Type: kind, Payload: payload})
		nonce := tunnel.RandomNonce()
		ct, _ := tunnel.SealData(key, nonce, app, tid[:], uint64(counter))
		frame, _ := n.Codec.Marshal(rpc.TunnelDataRequest{TunnelID: tid[:], Nonce: nonce, Ciphertext: ct, Seq: uint64(counter)})
		_, err := n.handleTunnelData(context.Background(), n.ID, &protocol.Frame{Payload: frame})
		return err
	}
	if send("file_block", rpc.FileBlockRequest{FileID: fileID, Data: []byte("ab")}) == nil {
		t.Fatal("unknown file acknowledged")
	}
	hash := sha256.Sum256([]byte("abcd"))
	if err := send("file_meta", rpc.FileMetadataRequest{FileID: fileID, Filename: "test.bin", Size: 4, SHA256: hash[:], BlockSize: 2, TotalBlocks: 2}); err != nil {
		t.Fatal(err)
	}
	if send("file_block", rpc.FileBlockRequest{FileID: fileID, Data: []byte("a")}) == nil {
		t.Fatal("short block acknowledged")
	}
	block := rpc.FileBlockRequest{FileID: fileID, Data: []byte("ab")}
	for i := 0; i < 2; i++ {
		if err := send("file_block", block); err != nil {
			t.Fatal(err)
		}
	}
	if state.Files[string(fileID)].Received != 1 {
		t.Fatal("duplicate counted twice")
	}
	if err := send("file_block", rpc.FileBlockRequest{FileID: fileID, BlockIdx: 1, Data: []byte("cd")}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(n.Config.Files.Directory(), "test.bin"))
	if err != nil || !bytes.Equal(data, []byte("abcd")) {
		t.Fatalf("bad file %q: %v", data, err)
	}
	disabled := false
	n.Config.Files.Enabled = &disabled
	if send("file_meta", rpc.FileMetadataRequest{}) == nil {
		t.Fatal("disabled file service acknowledged metadata")
	}
}
