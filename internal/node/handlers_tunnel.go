package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/rpc"
	"p2p-overlay/internal/tunnel"
)

func (n *Node) AddAppHistory(msg, status string) {
	n.AppMu.Lock()
	defer n.AppMu.Unlock()
	n.AppRecv = append(n.AppRecv, fmt.Sprintf("[%s] %s", status, msg))
}

func (n *Node) handleTunnelBuild(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
	var m rpc.TunnelBuildRequest
	if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
		return nil, err
	}
	if len(m.TunnelID) != 16 || len(m.Path) < 4 || len(m.Path) > 6 || m.MaxHops != len(m.Path) || m.HopIndex < 1 || m.HopIndex >= len(m.Path) || m.ExpiresAt <= time.Now().Unix() {
		return nil, errors.New("invalid route")
	}
	if !bytes.Equal(m.SenderID, m.Path[0].NodeID) {
		return nil, errors.New("sender mismatch")
	}
	seen := map[string]bool{}
	for _, h := range m.Path {
		if len(h.NodeID) != 32 || seen[string(h.NodeID)] {
			return nil, errors.New("repeated hop")
		}
		seen[string(h.NodeID)] = true
	}
	if !bytes.Equal(m.Path[m.HopIndex].NodeID, n.ID[:]) || !bytes.Equal(m.Path[m.HopIndex-1].NodeID, remote[:]) {
		return nil, errors.New("unexpected hop")
	}
	var tid [16]byte
	copy(tid[:], m.TunnelID)

	// я — Получатель
	if m.HopIndex == len(m.Path)-1 {
		sigData := append(tid[:], m.E2EPub...)
		sigData = append(sigData, m.SenderID...)
		senderID, _ := nodeid.FromBytes(m.SenderID)

		senderRec, err := n.GetRecord(ctx, dhtKeyForNode(senderID))
		if err != nil {
			return nil, fmt.Errorf("отправитель не найден в DHT")
		}
		if !identity.Verify(senderRec.PublicKey, sigData, m.E2ESig) {
			return nil, errors.New("неверная подпись E2E")
		}

		priv, pub := tunnel.NewE2EKeypair()
		e2eKey, err := tunnel.DeriveE2EKey(priv, m.E2EPub, tid)
		if err != nil {
			return nil, err
		}

		n.Tunnels.PutReceiver(&tunnel.ReceiverState{
			PrevID: remote, TunnelID: tid, SenderID: senderID, SenderE2EPub: m.E2EPub,
			E2EKey: e2eKey, ExpiresAt: time.Unix(m.ExpiresAt, 0),
			SeenMsgs: make(map[string]bool),
			Files:    make(map[string]*tunnel.FileReceiveState),
		})

		out, _ := n.Codec.Marshal(rpc.TunnelBuildResponse{
			TunnelID: tid[:], HopIndex: m.HopIndex, Accepted: true,
			ReceiverID: n.ID[:], ReceiverE2EPub: pub, ReceiverSig: n.Identity.Sign(append(append(append([]byte("tunnel-response-v1"), tid[:]...), m.E2EPub...), pub...)),
		})
		return &protocol.Frame{Type: protocol.FrameTunnelBuildOK, Payload: out}, nil
	}

	// я — Ретранслятор
	nextHop := m.Path[m.HopIndex+1]
	nextID, _ := nodeid.FromBytes(nextHop.NodeID)

	n.Tunnels.PutRelay(&tunnel.RelayState{
		TunnelID: tid, HopIndex: m.HopIndex, PrevID: remote,
		NextID: nextID, NextAddr: nextHop.Address, ExpiresAt: time.Unix(m.ExpiresAt, 0),
	})

	m.HopIndex++
	payload, _ := n.Codec.Marshal(m)
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := n.RPC.Call(callCtx, nextID, nextHop.Address, &protocol.Frame{
		Version: 1, Type: protocol.FrameTunnelBuild, Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (n *Node) handleTunnelData(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
	var m rpc.TunnelDataRequest
	if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
		return nil, err
	}
	var tid [16]byte
	copy(tid[:], m.TunnelID)

	// я — Получатель
	if rState, ok := n.Tunnels.GetReceiver(tid); ok {
		n.recvMu.Lock()
		defer n.recvMu.Unlock()
		if remote != rState.PrevID || time.Now().After(rState.ExpiresAt) {
			return nil, errors.New("expired receiver tunnel")
		}
		plaintext, err := tunnel.OpenData(rState.E2EKey, m.Nonce, m.Ciphertext, m.TunnelID, m.Seq)
		if err != nil {
			return nil, errors.New("ошибка расшифровки")
		}

		var appMsg rpc.AppMessageRequest
		if err := n.Codec.Unmarshal(plaintext, &appMsg); err != nil {
			return nil, err
		}

		if len(appMsg.MessageID) != 16 {
			return nil, errors.New("invalid message id")
		}
		dedupKey := rState.SenderID.String() + string(appMsg.MessageID)
		if n.seenApp[dedupKey] {
			out, _ := n.Codec.Marshal(rpc.TunnelAckResponse{TunnelID: tid[:], Seq: m.Seq, Success: true, Tag: tunnel.AckTag(rState.E2EKey, tid[:], m.Seq, m.Ciphertext)})
			return &protocol.Frame{Type: protocol.FrameTunnelAck, Payload: out}, nil
		}
		if len(n.seenApp) >= 100000 {
			return nil, errors.New("deduplication capacity exceeded")
		}
		// Обработка по типу
		switch appMsg.Type {
		case "text":
			log.Printf("[APP] %s: получен текст: %s", n.ID, string(appMsg.Payload))
			n.AddAppHistory(string(appMsg.Payload), "RECEIVED")
		case "file_meta":
			if !n.Config.Files.IsEnabled() {
				return nil, errors.New("file service disabled")
			}
			var meta rpc.FileMetadataRequest
			if err := n.Codec.Unmarshal(appMsg.Payload, &meta); err != nil {
				return nil, err
			}
			if len(meta.FileID) != 16 || !safeFilename(meta.Filename) || meta.Size <= 0 || meta.Size > 64*1024*1024 || meta.BlockSize < 1 || meta.BlockSize > ChunkSize || meta.TotalBlocks != int((meta.Size+int64(meta.BlockSize)-1)/int64(meta.BlockSize)) || len(meta.SHA256) != 32 || len(rState.Files) >= 4 {
				return nil, errors.New("invalid file metadata or capacity exceeded")
			}
			rState.Files[string(meta.FileID)] = &tunnel.FileReceiveState{
				Meta: meta, Blocks: make([][]byte, meta.TotalBlocks),
			}
			log.Printf("[FILE] %s: начинаю прием файла %s (%d блоков)", n.ID, meta.Filename, meta.TotalBlocks)
		case "file_block":
			if !n.Config.Files.IsEnabled() {
				return nil, errors.New("file service disabled")
			}
			var block rpc.FileBlockRequest
			if err := n.Codec.Unmarshal(appMsg.Payload, &block); err != nil {
				return nil, err
			}
			if fState, exists := rState.Files[string(block.FileID)]; exists {
				if block.BlockIdx < 0 || block.BlockIdx >= len(fState.Blocks) || len(block.Data) > fState.Meta.BlockSize {
					return nil, errors.New("invalid block index or size")
				}
				expectedSize := fState.Meta.BlockSize
				if block.BlockIdx == fState.Meta.TotalBlocks-1 {
					expectedSize = int(fState.Meta.Size) - block.BlockIdx*fState.Meta.BlockSize
				}
				if len(block.Data) != expectedSize {
					return nil, errors.New("incorrect block length")
				}
				// Дедупликация блока
				if fState.Blocks[block.BlockIdx] == nil {
					fState.Blocks[block.BlockIdx] = block.Data
					fState.Received++
				}
				// Если все блоки собраны
				if fState.Received == fState.Meta.TotalBlocks {
					if err := n.FinalizeFile(fState); err != nil {
						return nil, err
					} else {
						log.Printf("[FILE] %s: файл %s собран и проверен!", n.ID, fState.Meta.Filename)
					}
					delete(rState.Files, string(block.FileID))
				}
			} else {
				return nil, errors.New("unknown file transfer")
			}
		default:
			return nil, errors.New("unknown application type")
		}

		n.seenApp[dedupKey] = true
		out, _ := n.Codec.Marshal(rpc.TunnelAckResponse{TunnelID: tid[:], Seq: m.Seq, Success: true, Tag: tunnel.AckTag(rState.E2EKey, tid[:], m.Seq, m.Ciphertext)})
		return &protocol.Frame{Type: protocol.FrameTunnelAck, Payload: out}, nil
	}

	// я — Ретранслятор
	if rState, ok := n.Tunnels.GetRelay(tid); ok {
		if remote != rState.PrevID || time.Now().After(rState.ExpiresAt) {
			return nil, errors.New("invalid relay source or expired tunnel")
		}
		m.HopIndex++
		payload, _ := n.Codec.Marshal(m)
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		resp, err := n.RPC.Call(callCtx, rState.NextID, rState.NextAddr, &protocol.Frame{
			Version: 1, Type: protocol.FrameTunnelData, Payload: payload,
		})
		if err != nil {
			return nil, err
		}
		n.forwarded.Add(1)
		log.Printf("[TUNNEL_HOP] node=%s tunnel=%x seq=%d next=%s", n.ID, tid, m.Seq, rState.NextID)
		return resp, nil
	}

	return nil, errors.New("туннель не найден")
}

func (n *Node) handleTunnelClose(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
	var m rpc.TunnelCloseRequest
	if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
		return nil, err
	}
	var tid [16]byte
	copy(tid[:], m.TunnelID)

	if receiver, ok := n.Tunnels.GetReceiver(tid); ok {
		if receiver.PrevID != remote {
			return nil, errors.New("unexpected close source")
		}
		n.Tunnels.DeleteReceiver(tid)
	}
	if rState, ok := n.Tunnels.GetRelay(tid); ok {
		if rState.PrevID != remote {
			return nil, errors.New("unexpected close source")
		}
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		n.RPC.Call(callCtx, rState.NextID, rState.NextAddr, &protocol.Frame{
			Version: 1, Type: protocol.FrameTunnelClose, Payload: req.Payload,
		})
		n.Tunnels.DeleteRelay(tid)
	}
	return &protocol.Frame{Type: protocol.FrameTunnelCloseOk}, nil
}

func (n *Node) FinalizeFile(state *tunnel.FileReceiveState) error {
	if !n.Config.Files.IsEnabled() {
		return errors.New("file service disabled")
	}
	if !safeFilename(state.Meta.Filename) {
		return errors.New("unsafe filename")
	}
	var data []byte
	for _, b := range state.Blocks {
		data = append(data, b...)
	}
	if int64(len(data)) != state.Meta.Size {
		return errors.New("file size mismatch")
	}
	hash := sha256.Sum256(data)

	if !bytes.Equal(hash[:], state.Meta.SHA256) {
		return fmt.Errorf("SHA-256 mismatch для файла %s", state.Meta.Filename)
	}

	if err := os.MkdirAll(n.Config.Files.Directory(), 0755); err != nil {
		return fmt.Errorf("mkdir downloads: %w", err)
	}
	path := filepath.Join(n.Config.Files.Directory(), state.Meta.Filename)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

func safeFilename(name string) bool {
	return name != "" && name != "." && name != ".." && len(name) <= 200 && !strings.ContainsAny(name, "/\\:\x00") && filepath.Base(name) == name
}
