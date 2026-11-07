package node

import (
	"context"
	"errors"
	"fmt"
	"log"
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

	var tid [16]byte
	copy(tid[:], m.TunnelID)

	// Получатель
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
		e2eKey, _ := tunnel.DeriveE2EKey(priv, m.E2EPub, tid)

		n.Tunnels.PutReceiver(&tunnel.ReceiverState{
			TunnelID: tid, SenderID: senderID, SenderE2EPub: m.E2EPub,
			E2EKey: e2eKey, ExpiresAt: time.Unix(m.ExpiresAt, 0), SeenMsgs: make(map[string]bool),
		})

		out, _ := n.Codec.Marshal(rpc.TunnelBuildResponse{
			TunnelID: tid[:], HopIndex: m.HopIndex, Accepted: true,
			ReceiverID: n.ID[:], ReceiverE2EPub: pub,
		})
		return &protocol.Frame{Type: protocol.FrameTunnelBuildOK, Payload: out}, nil
	}

	// Ретранслятор
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

	// Получатель
	if rState, ok := n.Tunnels.GetReceiver(tid); ok {
		plaintext, err := tunnel.Decrypt(rState.E2EKey, m.Nonce, m.Ciphertext)
		if err != nil {
			return nil, errors.New("ошибка расшифровки (MITM?)")
		}

		var appMsg rpc.AppMessageRequest
		if err := n.Codec.Unmarshal(plaintext, &appMsg); err != nil {
			return nil, err
		}

		msgID := string(appMsg.MessageID)
		if rState.SeenMsgs[msgID] {
			log.Printf("[APP] %s: отброшен дубликат %x", n.ID, appMsg.MessageID)
		} else {
			rState.SeenMsgs[msgID] = true
			log.Printf("[APP] %s: получено сообщение: %s", n.ID, string(appMsg.Payload))
			n.AddAppHistory(string(appMsg.Payload), "RECEIVED")
		}

		out, _ := n.Codec.Marshal(rpc.TunnelAckResponse{TunnelID: tid[:], Seq: m.Seq, Success: true})
		return &protocol.Frame{Type: protocol.FrameTunnelAck, Payload: out}, nil
	}

	// Ретранслятор
	if rState, ok := n.Tunnels.GetRelay(tid); ok {
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

	if rState, ok := n.Tunnels.GetRelay(tid); ok {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		n.RPC.Call(callCtx, rState.NextID, rState.NextAddr, &protocol.Frame{
			Version: 1, Type: protocol.FrameTunnelClose, Payload: req.Payload,
		})
		n.Tunnels.DeleteRelay(tid)
	}
	return &protocol.Frame{Type: protocol.FrameTunnelCloseOk}, nil
}
