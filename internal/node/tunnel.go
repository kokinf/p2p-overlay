package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"time"

	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"p2p-overlay/internal/tunnel"
)

const (
	MinRelays = 2
	MaxRelays = 4
)

func dhtKeyForNode(id nodeid.ID) nodeid.ID {
	tmp := records.NodeRecord{NodeID: id}
	return tmp.DHTKey()
}

func (n *Node) selectRelays(exclude nodeid.ID, count int) []*dht.Contact {
	all := n.RT.Closest(n.ID, 0)
	seen := map[nodeid.ID]bool{n.ID: true, exclude: true}
	var out []*dht.Contact
	for _, c := range all {
		if seen[c.ID] || c.Address == "" {
			continue
		}
		seen[c.ID] = true
		out = append(out, c)
		if len(out) >= count {
			break
		}
	}
	return out
}

func (n *Node) BuildTunnel(ctx context.Context, receiverID nodeid.ID, relayCount int) (*tunnel.Tunnel, error) {
	if relayCount < MinRelays {
		return nil, fmt.Errorf("требуется минимум %d ретранслятора", MinRelays)
	}

	relays := n.selectRelays(receiverID, relayCount)
	if len(relays) < relayCount {
		return nil, fmt.Errorf("недостаточно ретрансляторов: нужно %d, найдено %d", relayCount, len(relays))
	}
	log.Printf("[TUNNEL] %s: выбор ретрансляторов: %v", n.ID, relays)

	recKey := dhtKeyForNode(receiverID)
	rec, err := n.GetRecord(ctx, recKey)
	if err != nil {
		return nil, fmt.Errorf("адрес получателя не найден в DHT: %w", err)
	}
	receiverAddr := rec.Addresses[0]

	var tid [16]byte
	rand.Read(tid[:])

	path := make([]rpc.HopInfo, 0, relayCount+2)
	path = append(path, rpc.HopInfo{NodeID: n.ID[:], Address: n.Addr()})
	for _, r := range relays {
		path = append(path, rpc.HopInfo{NodeID: r.ID[:], Address: r.Address})
	}
	path = append(path, rpc.HopInfo{NodeID: receiverID[:], Address: receiverAddr})

	e2ePriv, e2ePub := tunnel.NewE2EKeypair()
	sigData := append(tid[:], e2ePub...)
	sigData = append(sigData, n.ID[:]...)
	sig := n.Identity.Sign(sigData)

	req := rpc.TunnelBuildRequest{
		TunnelID: tid[:], Path: path, HopIndex: 1, SenderID: n.ID[:],
		E2EPub: e2ePub, E2ESig: sig, MaxHops: relayCount + 2,
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}

	payload, _ := n.Codec.Marshal(req)
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := n.RPC.Call(callCtx, relays[0].ID, relays[0].Address, &protocol.Frame{
		Version: 1, Type: protocol.FrameTunnelBuild, Payload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка построения туннеля: %w", err)
	}

	var buildResp rpc.TunnelBuildResponse
	n.Codec.Unmarshal(resp.Payload, &buildResp)
	if !buildResp.Accepted {
		return nil, fmt.Errorf("туннель отклонен: %s", buildResp.Reason)
	}

	e2eKey, err := tunnel.DeriveE2EKey(e2ePriv, buildResp.ReceiverE2EPub, tid)
	if err != nil {
		return nil, fmt.Errorf("ошибка E2E: %w", err)
	}

	t := &tunnel.Tunnel{
		ID: tid, State: tunnel.StateActive, CreatedAt: time.Now(),
		ReceiverID: receiverID, E2EKey: e2eKey, NextSeq: 1,
	}
	for _, h := range path {
		id, _ := nodeid.FromBytes(h.NodeID)
		t.Path = append(t.Path, tunnel.Hop{ID: id, Address: h.Address})
	}
	n.Tunnels.PutSender(t)

	log.Printf("[TUNNEL] %s: создан %x через %d ретрансляторов", n.ID, tid[:4], relayCount)
	return t, nil
}

func (n *Node) SendMessage(ctx context.Context, receiverID nodeid.ID, msg string) error {
	return n.sendMessageInternal(ctx, receiverID, msg, true)
}

func (n *Node) sendMessageInternal(ctx context.Context, receiverID nodeid.ID, msg string, allowRetry bool) error {
	var activeTun *tunnel.Tunnel
	for _, t := range n.Tunnels.ListSender() {
		if t.ReceiverID == receiverID && t.State == tunnel.StateActive {
			activeTun = t
			break
		}
	}

	if activeTun == nil {
		var err error
		activeTun, err = n.BuildTunnel(ctx, receiverID, 3)
		if err != nil {
			return err
		}
	}

	appMsg := rpc.AppMessageRequest{MessageID: make([]byte, 16), Payload: []byte(msg)}
	rand.Read(appMsg.MessageID)
	appBytes, _ := n.Codec.Marshal(appMsg)

	nonce := tunnel.RandomNonce()
	ciphertext, _ := tunnel.Encrypt(activeTun.E2EKey, nonce, appBytes)

	dataReq := rpc.TunnelDataRequest{
		TunnelID: activeTun.ID[:], HopIndex: 1, Seq: activeTun.NextSeq,
		Nonce: nonce, Ciphertext: ciphertext,
	}
	activeTun.NextSeq++

	payload, _ := n.Codec.Marshal(dataReq)
	nextHop := activeTun.Path[1]
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := n.RPC.Call(callCtx, nextHop.ID, nextHop.Address, &protocol.Frame{
		Version: 1, Type: protocol.FrameTunnelData, Payload: payload,
	})
	if err != nil {
		n.Tunnels.UpdateState(activeTun.ID, tunnel.StateDegraded)
		n.RT.Remove(nextHop.ID)
		log.Printf("[TUNNEL] %s: ретранслятор %s отказал, переход в DEGRADED", n.ID, nextHop.ID)

		if allowRetry {
			log.Printf("[TUNNEL] %s: перестроение маршрута...", n.ID)
			return n.sendMessageInternal(ctx, receiverID, msg, false)
		}
		return fmt.Errorf("отказ туннеля: %w", err)
	}

	var ack rpc.TunnelAckResponse
	n.Codec.Unmarshal(resp.Payload, &ack)
	if !ack.Success {
		return errors.New("получатель отклонил сообщение")
	}

	n.AppMu.Lock()
	n.AppSent = append(n.AppSent, fmt.Sprintf("[SENT] %s", msg))
	n.AppMu.Unlock()

	log.Printf("[TUNNEL] %s: сообщение доставлено через %d хопов", n.ID, len(activeTun.Path)-2)
	return nil
}
