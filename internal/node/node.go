package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"p2p-overlay/internal/tunnel"
)

type Node struct {
	ID       nodeid.ID
	Identity *identity.Identity
	RT       *dht.RoutingTable
	RPC      *rpc.Manager
	Codec    serialization.Codec
	Config   *config.Config

	listener      net.Listener
	listenerReady chan struct{}

	mu      sync.Mutex
	lookups []*dht.LookupResult

	Store        *records.Store
	ownedRecords map[nodeid.ID]*records.NodeRecord

	Tunnels *tunnel.Manager
	AppMu   sync.Mutex
	AppRecv []string
	AppSent []string
}

func New(cfg *config.Config) (*Node, error) {
	id, err := identity.LoadOrCreate(cfg.Node.StateDir)
	if err != nil {
		return nil, err
	}
	codec := serialization.ByName(cfg.Protocol.Codec)
	rt := dht.NewRoutingTable(id.NodeID(), cfg.DHT.K)

	n := &Node{
		ID:            id.NodeID(),
		Identity:      id,
		RT:            rt,
		Codec:         codec,
		Config:        cfg,
		Store:         records.NewStore(),
		ownedRecords:  make(map[nodeid.ID]*records.NodeRecord),
		listenerReady: make(chan struct{}),
		Tunnels:       tunnel.NewManager(),
	}
	n.RPC = rpc.NewManagerWithWriteTimeout(id, codec, n.handleRPC, cfg.Node.ReadTimeout, cfg.Node.WriteTimeout)
	return n, nil
}

func (n *Node) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Config.Node.Port))
	if err != nil {
		close(n.listenerReady)
		return err
	}
	n.listener = ln
	close(n.listenerReady)
	log.Printf("node %s listening on :%d (state=%s)",
		n.ID, n.Config.Node.Port, n.Config.Node.StateDir)

	go func() {
		if err := n.RPC.Serve(ctx, ln); err != nil {
			log.Printf("node %s: rpc serve: %v", n.ID, err)
		}
	}()

	for _, addr := range n.Config.Bootstrap.Peers {
		if err := n.bootstrapOne(ctx, addr); err != nil {
			log.Printf("node %s: bootstrap %s: %v", n.ID, addr, err)
		}
	}
	if len(n.Config.Bootstrap.Peers) > 0 {
		if _, err := n.Lookup(ctx, n.ID); err != nil {
			log.Printf("node %s: self-lookup: %v", n.ID, err)
		}
	}

	go n.refreshLoop(ctx)
	go n.maintenanceLoop(ctx)
	go n.experimentLoop(ctx)

	<-ctx.Done()
	n.RPC.Close()
	if n.listener != nil {
		_ = n.listener.Close()
	}
	return nil
}

func (n *Node) refreshLoop(ctx context.Context) {
	interval := n.Config.DHT.RefreshInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var target nodeid.ID
			_, _ = rand.Read(target[:])

			res, err := n.Lookup(ctx, target)
			if err != nil {
				log.Printf("node %s: refresh lookup error: %v", n.ID, err)
				continue
			}
			log.Printf("node %s: refresh lookup target=%x... iterations=%d rpcs=%d closest=%d",
				n.ID, target[:4], res.Iterations, res.RPCs, len(res.Closest))
		}
	}
}

func (n *Node) Addr() string {
	<-n.listenerReady
	if n.listener == nil {
		return fmt.Sprintf("127.0.0.1:%d", n.Config.Node.Port)
	}
	addr, ok := n.listener.Addr().(*net.TCPAddr)
	if !ok {
		return fmt.Sprintf("127.0.0.1:%d", n.Config.Node.Port)
	}
	return fmt.Sprintf("127.0.0.1:%d", addr.Port)
}

func (n *Node) handleRPC(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
	if req.Version != 1 {
		return nil, fmt.Errorf("unsupported protocol version %d", req.Version)
	}
	switch req.Type {
	case protocol.FramePing:
		var m rpc.PingRequest
		if len(req.Payload) > 0 {
			_ = n.Codec.Unmarshal(req.Payload, &m)
		}
		if m.Addr != "" {
			_, _ = n.RT.AddOrUpdate(&dht.Contact{ID: remote, Address: m.Addr, LastSeen: time.Now()})
		}
		out, _ := n.Codec.Marshal(rpc.PingResponse{})
		return &protocol.Frame{Type: protocol.FramePong, Payload: out}, nil

	case protocol.FrameFindNodeRequest:
		var m rpc.FindNodeRequest
		if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
			return nil, err
		}
		if m.Addr != "" {
			_, _ = n.RT.AddOrUpdate(&dht.Contact{ID: remote, Address: m.Addr, LastSeen: time.Now()})
		}
		target, err := nodeid.FromBytes(m.Target)
		if err != nil {
			return nil, err
		}
		closest := n.RT.Closest(target, n.RT.K())
		wire := make([]rpc.ContactWire, 0, len(closest))
		for _, c := range closest {
			wire = append(wire, rpc.ContactWire{NodeID: c.ID[:], Address: c.Address})
		}
		out, err := n.Codec.Marshal(rpc.FindNodeResponse{Contacts: wire})
		if err != nil {
			return nil, err
		}
		return &protocol.Frame{Type: protocol.FrameFindNodeResponse, Payload: out}, nil

	case protocol.FrameStoreRequest:
		var m rpc.StoreRequest
		if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
			return nil, err
		}
		var rec records.NodeRecord
		if err := n.Codec.Unmarshal(m.Record, &rec); err != nil {
			return nil, err
		}
		key, err := nodeid.FromBytes(m.Key)
		if err != nil {
			return nil, err
		}
		if rec.DHTKey() != key {
			return nil, fmt.Errorf("store: key %s does not match record DHTKey %s", key, rec.DHTKey())
		}
		if err := rec.Validate(); err != nil {
			return nil, fmt.Errorf("store: invalid record: %w", err)
		}
		err = n.Store.Put(key, &rec)
		success := (err == nil)
		if errors.Is(err, records.ErrSequenceRollback) {
			success = true
		}
		out, _ := n.Codec.Marshal(rpc.StoreResponse{Success: success})
		return &protocol.Frame{Type: protocol.FrameStoreResponse, Payload: out}, nil

	case protocol.FrameFindValueRequest:
		var m rpc.FindValueRequest
		if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
			return nil, err
		}
		if m.Addr != "" {
			_, _ = n.RT.AddOrUpdate(&dht.Contact{ID: remote, Address: m.Addr, LastSeen: time.Now()})
		}
		key, err := nodeid.FromBytes(m.Key)
		if err != nil {
			return nil, err
		}
		rec, found := n.Store.Get(key)
		if found {
			recBytes, _ := n.Codec.Marshal(rec)
			out, _ := n.Codec.Marshal(rpc.FindValueResponse{Record: recBytes})
			return &protocol.Frame{Type: protocol.FrameFindValueResponse, Payload: out}, nil
		}
		closest := n.RT.Closest(key, n.RT.K())
		wire := make([]rpc.ContactWire, 0, len(closest))
		for _, c := range closest {
			wire = append(wire, rpc.ContactWire{NodeID: c.ID[:], Address: c.Address})
		}
		out, _ := n.Codec.Marshal(rpc.FindValueResponse{Contacts: wire})
		return &protocol.Frame{Type: protocol.FrameFindValueResponse, Payload: out}, nil

	case protocol.FrameTunnelBuild:
		return n.handleTunnelBuild(ctx, remote, req)
	case protocol.FrameTunnelData:
		return n.handleTunnelData(ctx, remote, req)
	case protocol.FrameTunnelClose:
		return n.handleTunnelClose(ctx, remote, req)
	}
	return nil, fmt.Errorf("unknown request type %d", req.Type)
}

func (n *Node) bootstrapOne(ctx context.Context, addr string) error {
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		remote, err := n.RPC.DialUnverified(ctx, addr)
		if err == nil {
			c := &dht.Contact{ID: remote, Address: addr, LastSeen: time.Now()}
			if err := n.addContactWithEviction(ctx, c); err != nil {
				return err
			}
			log.Printf("node %s: bootstrap ok %s -> %s", n.ID, addr, remote)
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("bootstrap %s failed after retries: %w", addr, lastErr)
}

func (n *Node) AddContactForTest(ctx context.Context, id nodeid.ID, addr string) error {
	return n.addContactWithEviction(ctx, &dht.Contact{ID: id, Address: addr, LastSeen: time.Now()})
}

func (n *Node) addContactWithEviction(ctx context.Context, c *dht.Contact) error {
	lru, err := n.RT.AddOrUpdate(c)
	if err != nil && !errors.Is(err, dht.ErrBucketFull) {
		return err
	}
	if lru == nil {
		return nil
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := n.ping(pingCtx, lru); err == nil {
		n.RT.Touch(lru.ID)
		return nil
	}
	n.RT.ReplaceLRU(n.RT.BucketIndexFor(c.ID), c)
	return nil
}

func (n *Node) ping(ctx context.Context, c *dht.Contact) error {
	payload, _ := n.Codec.Marshal(rpc.PingRequest{Addr: n.Addr()})
	_, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{Version: 1, Type: protocol.FramePing, Payload: payload})
	return err
}

func (n *Node) findNodeRPC(ctx context.Context, c *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
	payload, err := n.Codec.Marshal(rpc.FindNodeRequest{Target: target[:], Addr: n.Addr()})
	if err != nil {
		return nil, err
	}
	resp, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{Version: 1, Type: protocol.FrameFindNodeRequest, Payload: payload})
	if err != nil {
		return nil, err
	}
	var m rpc.FindNodeResponse
	if err := n.Codec.Unmarshal(resp.Payload, &m); err != nil {
		return nil, err
	}
	out := make([]*dht.Contact, 0, len(m.Contacts))
	for _, w := range m.Contacts {
		id, err := nodeid.FromBytes(w.NodeID)
		if err != nil || id == n.ID {
			continue
		}
		out = append(out, &dht.Contact{ID: id, Address: w.Address, LastSeen: time.Now()})
	}
	return out, nil
}

func (n *Node) Lookup(ctx context.Context, target nodeid.ID) (*dht.LookupResult, error) {
	find := func(ctx context.Context, c *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
		contacts, err := n.findNodeRPC(ctx, c, target)
		if err == nil {
			for _, nc := range contacts {
				_ = n.addContactWithEviction(ctx, nc)
			}
		}
		return contacts, err
	}
	res := dht.IterativeLookup(ctx, n.RT, target, find)
	n.mu.Lock()
	n.lookups = append(n.lookups, res)
	n.mu.Unlock()
	return res, nil
}

func (n *Node) RoutingTableSnapshot() []dht.BucketSnapshot {
	return n.RT.Snapshot()
}

func (n *Node) Lookups() []*dht.LookupResult {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]*dht.LookupResult, len(n.lookups))
	copy(out, n.lookups)
	return out
}

func (n *Node) PutRecord(ctx context.Context, rec *records.NodeRecord) error {
	key := rec.DHTKey()
	if err := n.Store.Put(key, rec); err != nil {
		if !errors.Is(err, records.ErrSequenceRollback) {
			return fmt.Errorf("local store: %w", err)
		}
		log.Printf("node %s: local store has newer record for key %s (attempted seq=%d)",
			n.ID, key, rec.Seq)
	}

	closest := n.RT.Closest(key, n.Config.DHT.Replication)
	recBytes, _ := n.Codec.Marshal(rec)
	reqPayload, _ := n.Codec.Marshal(rpc.StoreRequest{Key: key[:], Record: recBytes})

	var successCount int32
	var wg sync.WaitGroup

	for _, c := range closest {
		wg.Add(1)
		go func(target *dht.Contact) {
			defer wg.Done()
			cCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			resp, err := n.RPC.Call(cCtx, target.ID, target.Address, &protocol.Frame{
				Version: 1, Type: protocol.FrameStoreRequest, Payload: reqPayload,
			})
			if err == nil && resp.Type == protocol.FrameStoreResponse {
				var sr rpc.StoreResponse
				if n.Codec.Unmarshal(resp.Payload, &sr) == nil && sr.Success {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(c)
	}
	wg.Wait()

	required := 2
	if len(closest)+1 < required {
		required = len(closest) + 1
	}
	if int(successCount)+1 < required {
		return fmt.Errorf("replication failed: got %d confirmations, required %d", successCount+1, required)
	}
	return nil
}

func (n *Node) GetRecord(ctx context.Context, key nodeid.ID) (*records.NodeRecord, error) {
	if rec, ok := n.Store.Get(key); ok {
		return rec, nil
	}

	findFunc := func(ctx context.Context, c *dht.Contact, k nodeid.ID) dht.FindValueResult {
		payload, _ := n.Codec.Marshal(rpc.FindValueRequest{Key: k[:], Addr: n.Addr()})
		resp, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{
			Version: 1, Type: protocol.FrameFindValueRequest, Payload: payload,
		})
		if err != nil {
			return dht.FindValueResult{Err: err}
		}
		if resp.Type == protocol.FrameError {
			return dht.FindValueResult{Err: fmt.Errorf("remote error")}
		}

		var m rpc.FindValueResponse
		if err := n.Codec.Unmarshal(resp.Payload, &m); err != nil {
			return dht.FindValueResult{Err: err}
		}
		if len(m.Record) > 0 {
			return dht.FindValueResult{Record: m.Record}
		}
		var contacts []*dht.Contact
		for _, w := range m.Contacts {
			id, _ := nodeid.FromBytes(w.NodeID)
			if id != n.ID {
				contacts = append(contacts, &dht.Contact{ID: id, Address: w.Address, LastSeen: time.Now()})
			}
		}
		return dht.FindValueResult{Contacts: contacts}
	}

	recordBytes, _ := dht.IterativeLookupValue(ctx, n.RT, key, findFunc)
	if recordBytes == nil {
		return nil, fmt.Errorf("record not found")
	}

	var rec records.NodeRecord
	if err := n.Codec.Unmarshal(recordBytes, &rec); err != nil {
		return nil, fmt.Errorf("invalid record format: %w", err)
	}
	if err := rec.Validate(); err != nil {
		return nil, fmt.Errorf("invalid record: %w", err)
	}
	if rec.DHTKey() != key {
		return nil, fmt.Errorf("record key mismatch: got %s, want %s", rec.DHTKey(), key)
	}

	_ = n.Store.Put(key, &rec)
	return &rec, nil
}

func (n *Node) Stop() {
	n.RPC.Close()
	if n.listener != nil {
		_ = n.listener.Close()
	}
}

func (n *Node) ResolveAlias(ctx context.Context, alias string) (*records.NodeRecord, error) {
	norm := records.NormalizeAlias(alias)
	if norm == "" {
		return nil, errors.New("empty alias")
	}
	tmp := records.NodeRecord{Alias: norm}
	key := tmp.DHTKey()
	rec, err := n.GetRecord(ctx, key)
	if err != nil {
		return nil, err
	}
	if records.NormalizeAlias(rec.Alias) != norm {
		return nil, errors.New("mismatched alias record")
	}
	return rec, nil
}

func (n *Node) PublishRecord(ctx context.Context, rec *records.NodeRecord) error {
	n.mu.Lock()
	n.ownedRecords[rec.DHTKey()] = rec
	n.mu.Unlock()
	return n.PutRecord(ctx, rec)
}

func (n *Node) maintenanceLoop(ctx context.Context) {
	ttl := time.Duration(n.Config.DHT.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 180 * time.Second
	}
	maxRepublish := ttl / 2
	republishInterval := n.Config.DHT.RepublishInterval
	if republishInterval <= 0 || republishInterval > maxRepublish {
		republishInterval = maxRepublish
	}
	if republishInterval <= 0 {
		republishInterval = 60 * time.Second
	}

	cleanupTicker := time.NewTicker(10 * time.Second)
	republishTicker := time.NewTicker(republishInterval)
	defer cleanupTicker.Stop()
	defer republishTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			deleted := n.Store.Cleanup()
			if deleted > 0 {
				log.Printf("node %s: TTL cleanup removed %d records", n.ID, deleted)
			}
			n.Tunnels.Cleanup(time.Now())
		case <-republishTicker.C:
			n.mu.Lock()
			recordsToRepublish := make([]*records.NodeRecord, 0, len(n.ownedRecords))
			for _, rec := range n.ownedRecords {
				if rec.ExpiresAt <= time.Now().Unix() {
					rec.ExpiresAt = time.Now().Add(ttl).Unix()
					rec.Seq++
					rec.Sign(n.Identity)
				}
				recordsToRepublish = append(recordsToRepublish, rec)
			}
			n.mu.Unlock()

			for _, rec := range recordsToRepublish {
				go func(r *records.NodeRecord) {
					repCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					defer cancel()
					if err := n.PutRecord(repCtx, r); err != nil {
						log.Printf("node %s: republish failed: %v", n.ID, err)
					}
				}(rec)
			}
		}
	}
}

func (n *Node) experimentLoop(ctx context.Context) {
	if !n.Config.Experiment.Enabled {
		return
	}
	delay := n.Config.Experiment.Delay
	if delay <= 0 {
		delay = 20 * time.Second
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}

	n.dumpMetricsSafe()

	targets := make([]nodeid.ID, 0, len(n.Config.Experiment.Targets))
	for _, h := range n.Config.Experiment.Targets {
		id, err := nodeid.ParseHex(h)
		if err != nil {
			log.Printf("node %s: invalid experiment target %q: %v", n.ID, h, err)
			continue
		}
		targets = append(targets, id)
	}

	for _, target := range targets {
		lookupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		res, err := n.Lookup(lookupCtx, target)
		cancel()
		if err != nil {
			log.Printf("node %s: experiment lookup %s failed: %v", n.ID, target, err)
			continue
		}
		log.Printf("node %s: experiment lookup target=%s iterations=%d rpcs=%d",
			n.ID, target, res.Iterations, res.RPCs)
	}

	n.dumpMetricsSafe()
}
