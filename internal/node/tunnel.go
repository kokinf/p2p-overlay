package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"bytes"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"p2p-overlay/internal/tunnel"
)

const (
	MinRelays = 2
	MaxRelays = 4
	ChunkSize = 48 * 1024
)

func dhtKeyForNode(id nodeid.ID) nodeid.ID {
	tmp := records.NodeRecord{NodeID: id}
	return tmp.DHTKey()
}

func (n *Node) selectRelays(exclude nodeid.ID, count int) []*dht.Contact {
	all := n.RT.Closest(n.ID, 0)
	seen := map[nodeid.ID]bool{n.ID: true, exclude: true}
	var candidates []*dht.Contact

	for _, c := range all {
		if seen[c.ID] || c.Address == "" {
			continue
		}
		seen[c.ID] = true
		n.RelayProfiler.ObserveAddress(c.ID, c.Address)
		candidates = append(candidates, c)
	}

	profiles := n.RelayProfiler.Snapshot()
	if n.Config.DHT.RelayStrategy == "rating" {
		sort.Slice(candidates, func(i, j int) bool {
			si := profiles[candidates[i].ID]
			sj := profiles[candidates[j].ID]

			if (si.ConsecutiveFailures > 2) != (sj.ConsecutiveFailures > 2) {
				return si.ConsecutiveFailures <= 2
			}

			if si.EMASuccess != sj.EMASuccess {
				return si.EMASuccess > sj.EMASuccess
			}
			if si.EMALatency != sj.EMALatency {
				return si.EMALatency < sj.EMALatency
			}
			return candidates[i].ID.String() < candidates[j].ID.String()
		})
		log.Printf("[TUNNEL] %s: выбран режим RATING", n.ID)
	} else {
		mrand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
		log.Printf("[TUNNEL] %s: выбран режим RANDOM", n.ID)
	}

	if n.Config.Tunnel.MaxPerSubnet > 0 {
		used := make(map[string]int)
		filtered := make([]*dht.Contact, 0, len(candidates))
		for _, c := range candidates {
			prefix := profiles[c.ID].Subnet
			if prefix == "" || prefix == "unknown" || used[prefix] >= n.Config.Tunnel.MaxPerSubnet {
				log.Printf("[RELAY_REJECT] id=%s reason=subnet_limit_or_unknown", c.ID)
				continue
			}
			used[prefix]++
			filtered = append(filtered, c)
		}
		candidates = filtered
	}
	for i, c := range candidates {
		if i >= count {
			break
		}
		log.Printf("[RELAY_SELECT] id=%s subnet=%s strategy=%s", c.ID, profiles[c.ID].Subnet, n.Config.DHT.RelayStrategy)
	}
	if len(candidates) > count {
		candidates = candidates[:count]
	}
	return candidates
}

func (n *Node) BuildTunnel(ctx context.Context, receiverID nodeid.ID, relayCount int) (*tunnel.Tunnel, error) {
	if err := n.publishSelfRecord(ctx); err != nil {
		return nil, fmt.Errorf("publish sender identity: %w", err)
	}
	if relayCount < MinRelays || relayCount > MaxRelays {
		return nil, fmt.Errorf("требуется минимум %d ретранслятора", MinRelays)
	}
	var probes sync.WaitGroup
	sem := make(chan struct{}, 3)
	for _, c := range n.RT.Closest(n.ID, 0) {
		probes.Add(1)
		go func(c *dht.Contact) {
			defer probes.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			probeCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := n.ping(probeCtx, c)
			if ctx.Err() == nil {
				n.RelayProfiler.Update(c.ID, err == nil, time.Since(start).Seconds())
			}
			if err != nil && ctx.Err() == nil {
				n.RT.Remove(c.ID)
			}
		}(c)
	}
	probes.Wait()
	relays := n.selectRelays(receiverID, relayCount)
	if len(relays) < relayCount {
		return nil, fmt.Errorf("need %d distinct relays, found %d", relayCount, len(relays))
	}

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
		ExpiresAt: time.Now().Add(n.tunnelTTL()).Unix(),
	}

	t := &tunnel.Tunnel{ID: tid, State: tunnel.StateBuilding, CreatedAt: time.Now(), ExpiresAt: time.Unix(req.ExpiresAt, 0), ReceiverID: receiverID, NextSeq: 1}
	for _, h := range path {
		id, _ := nodeid.FromBytes(h.NodeID)
		t.Path = append(t.Path, tunnel.Hop{ID: id, Address: h.Address})
	}
	n.Tunnels.PutSender(t)
	built := false
	defer func() {
		if !built {
			n.Tunnels.UpdateState(tid, tunnel.StateDead)
		}
	}()
	payload, _ := n.Codec.Marshal(req)
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := n.RPC.Call(callCtx, relays[0].ID, relays[0].Address, &protocol.Frame{
		Version: 1, Type: protocol.FrameTunnelBuild, Payload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка построения: %w", err)
	}

	var buildResp rpc.TunnelBuildResponse
	n.Codec.Unmarshal(resp.Payload, &buildResp)
	if !buildResp.Accepted {
		return nil, fmt.Errorf("туннель отклонен: %s", buildResp.Reason)
	}

	transcript := append(append(append([]byte("tunnel-response-v1"), tid[:]...), e2ePub...), buildResp.ReceiverE2EPub...)
	if !bytes.Equal(buildResp.TunnelID, tid[:]) || !bytes.Equal(buildResp.ReceiverID, receiverID[:]) || !identity.Verify(rec.PublicKey, transcript, buildResp.ReceiverSig) {
		return nil, errors.New("invalid receiver authentication")
	}
	e2eKey, err := tunnel.DeriveE2EKey(e2ePriv, buildResp.ReceiverE2EPub, tid)
	if err != nil {
		return nil, fmt.Errorf("ошибка E2E: %w", err)
	}

	t.E2EKey = e2eKey
	n.Tunnels.PutSender(t)
	n.Tunnels.UpdateState(tid, tunnel.StateActive)
	t.State = tunnel.StateActive
	built = true
	log.Printf("[TUNNEL] %s: создан %x через %d ретрансляторов", n.ID, tid[:4], relayCount)
	return t, nil
}

func (n *Node) getActiveTunnel(receiverID nodeid.ID) *tunnel.Tunnel {
	for _, t := range n.Tunnels.ListSender() {
		if t.ReceiverID == receiverID && t.State == tunnel.StateActive && time.Now().Before(t.ExpiresAt) {
			return t
		}
	}
	return nil
}

func (n *Node) SendMessage(ctx context.Context, receiverID nodeid.ID, msg string) error {
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	if err := n.ensureTunnelPool(ctx, receiverID); err != nil {
		return err
	}
	return n.sendAppEnvelope(ctx, receiverID, "text", []byte(msg), true)
}

// SendFile отправляет файл
func (n *Node) SendFile(ctx context.Context, receiverID nodeid.ID, filePath string) error {
	if !n.Config.Files.IsEnabled() {
		return errors.New("file service disabled")
	}
	if !n.Config.Files.IsEnabled() {
		return errors.New("file service disabled")
	}
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	if err := n.ensureTunnelPool(ctx, receiverID); err != nil {
		return err
	}
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if info.Size() <= 0 || info.Size() > 64*1024*1024 {
		return errors.New("file size must be 1..64 MiB")
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("чтение файла: %w", err)
	}

	hash := sha256.Sum256(data)
	var fileID [16]byte
	rand.Read(fileID[:])

	meta := rpc.FileMetadataRequest{
		FileID:      fileID[:],
		Filename:    filepath.Base(filePath),
		Size:        int64(len(data)),
		SHA256:      hash[:],
		BlockSize:   ChunkSize,
		TotalBlocks: (len(data) + ChunkSize - 1) / ChunkSize,
	}

	// Отправляем метаданные
	metaBytes, _ := n.Codec.Marshal(meta)
	if err := n.sendAppEnvelope(ctx, receiverID, "file_meta", metaBytes, true); err != nil {
		return err
	}

	// Отправляем блоки
	for i := 0; i < meta.TotalBlocks; i++ {
		start, end := i*ChunkSize, (i+1)*ChunkSize
		if end > len(data) {
			end = len(data)
		}
		block := rpc.FileBlockRequest{
			FileID: fileID[:], BlockIdx: i, Data: data[start:end],
		}
		blockBytes, _ := n.Codec.Marshal(block)
		if err := n.sendAppEnvelope(ctx, receiverID, "file_block", blockBytes, true); err != nil {
			return fmt.Errorf("ошибка отправки блока %d: %w", i, err)
		}
	}

	log.Printf("[FILE] %s: файл %s отправлен (%d блоков)", n.ID, filepath.Base(filePath), meta.TotalBlocks)
	return nil
}

// универсальная отправка E2E-сообщения
func (n *Node) sendAppEnvelope(ctx context.Context, receiverID nodeid.ID, msgType string, payload []byte, allowRetry bool) error {
	appMsg := rpc.AppMessageRequest{MessageID: make([]byte, 16), Type: msgType, Payload: payload}
	if _, err := rand.Read(appMsg.MessageID); err != nil {
		return err
	}
	err := n.sendEnvelope(ctx, receiverID, appMsg, allowRetry)
	status := "DELIVERED"
	if err != nil {
		status = "FAILED"
	}
	entry := fmt.Sprintf("[%s] to=%s message_id=%x type=%s bytes=%d", status, receiverID, appMsg.MessageID, msgType, len(payload))
	n.AppMu.Lock()
	n.AppSent = append(n.AppSent, entry)
	n.AppMu.Unlock()
	log.Printf("[APP_SEND] %s: %s", n.ID, entry)
	return err
}
func (n *Node) sendEnvelope(ctx context.Context, receiverID nodeid.ID, appMsg rpc.AppMessageRequest, allowRetry bool) error {
	activeTun := n.getActiveTunnel(receiverID)
	if activeTun == nil {
		var err error
		activeTun, err = n.BuildTunnel(ctx, receiverID, n.relayCount())
		if err != nil {
			return err
		}
	}

	appBytes, _ := n.Codec.Marshal(appMsg)

	nonce := tunnel.RandomNonce()
	seq := n.Tunnels.NextSequence(activeTun.ID)
	ciphertext, err := tunnel.SealData(activeTun.E2EKey, nonce, appBytes, activeTun.ID[:], seq)
	if err != nil {
		return err
	}

	dataReq := rpc.TunnelDataRequest{
		TunnelID: activeTun.ID[:], HopIndex: 1, Seq: seq,
		Nonce: nonce, Ciphertext: ciphertext,
	}

	payloadBytes, _ := n.Codec.Marshal(dataReq)
	nextHop := activeTun.Path[1]
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := n.RPC.Call(callCtx, nextHop.ID, nextHop.Address, &protocol.Frame{
		Version: 1, Type: protocol.FrameTunnelData, Payload: payloadBytes,
	})

	if err != nil {
		n.Tunnels.UpdateState(activeTun.ID, tunnel.StateDegraded)
		for _, hop := range activeTun.Path[1 : len(activeTun.Path)-1] {
			c := &dht.Contact{ID: hop.ID, Address: hop.Address}
			if n.ping(ctx, c) != nil {
				n.RT.Remove(hop.ID)
				for _, other := range n.Tunnels.ListSender() {
					for _, h := range other.Path {
						if h.ID == hop.ID {
							n.Tunnels.UpdateState(other.ID, tunnel.StateDegraded)
						}
					}
				}
			}
		}
		if allowRetry {
			for _, other := range n.Tunnels.ListSender() {
				if other.ReceiverID == receiverID && other.State == tunnel.StateActive {
					n.Tunnels.UpdateState(other.ID, tunnel.StateDegraded)
				}
			}
			if _, buildErr := n.BuildTunnel(ctx, receiverID, n.relayCount()); buildErr != nil {
				return buildErr
			}
			return n.sendEnvelope(ctx, receiverID, appMsg, false)
		}
		return fmt.Errorf("отказ туннеля: %w", err)
	}

	var ack rpc.TunnelAckResponse
	n.Codec.Unmarshal(resp.Payload, &ack)
	if !ack.Success || !bytes.Equal(ack.TunnelID, activeTun.ID[:]) || ack.Seq != dataReq.Seq || !tunnel.VerifyAck(activeTun.E2EKey, activeTun.ID[:], seq, ciphertext, ack.Tag) {
		return errors.New("получатель отклонил сообщение")
	}
	return nil
}

func (n *Node) relayCount() int {
	if n.Config.Tunnel.Relays == 0 {
		return 3
	}
	return n.Config.Tunnel.Relays
}
func (n *Node) tunnelTTL() time.Duration {
	if n.Config.Tunnel.TTL <= 0 {
		return 5 * time.Minute
	}
	return n.Config.Tunnel.TTL
}
func (n *Node) ensureTunnelPool(ctx context.Context, receiver nodeid.ID) error {
	size := n.Config.Tunnel.PoolSize
	if size == 0 {
		size = 3
	}
	if size < 1 || size > 16 {
		return errors.New("invalid tunnel pool size")
	}
	count := 0
	for _, t := range n.Tunnels.ListSender() {
		if t.ReceiverID == receiver && t.State == tunnel.StateActive && time.Now().Before(t.ExpiresAt) {
			count++
		}
	}
	for count < size {
		var last error
		for attempt := 0; attempt < 3; attempt++ {
			_, last = n.BuildTunnel(ctx, receiver, n.relayCount())
			if last == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if last != nil {
			return last
		}
		count++
	}
	return nil
}
func (n *Node) EnsureTunnelPool(ctx context.Context, receiver nodeid.ID) error {
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	return n.ensureTunnelPool(ctx, receiver)
}
func (n *Node) CloseTunnel(ctx context.Context, id [16]byte) error {
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	t, ok := n.Tunnels.GetSender(id)
	if !ok {
		return errors.New("unknown tunnel")
	}
	n.Tunnels.UpdateState(id, tunnel.StateClosing)
	payload, _ := n.Codec.Marshal(rpc.TunnelCloseRequest{TunnelID: id[:]})
	_, err := n.RPC.Call(ctx, t.Path[1].ID, t.Path[1].Address, &protocol.Frame{Version: 1, Type: protocol.FrameTunnelClose, Payload: payload})
	n.Tunnels.UpdateState(id, tunnel.StateDead)
	return err
}
