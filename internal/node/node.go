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
	selfPublishMu     sync.Mutex
	selfRecordPending atomic.Bool
	forwarded         atomic.Uint64
	stopOnce          sync.Once
	stopCh            chan struct{}
	runDone           chan struct{}
	runStarted        atomic.Bool
	runWG             sync.WaitGroup
	ID                nodeid.ID
	Identity          *identity.Identity
	RT                *dht.RoutingTable
	RPC               *rpc.Manager
	Codec             serialization.Codec
	Config            *config.Config

	joining       atomic.Bool
	listener      net.Listener
	listenerReady chan struct{}

	sequenceMu sync.Mutex
	mu         sync.Mutex
	lookups    []*dht.LookupResult

	Store        *records.Store
	ownedRecords map[nodeid.ID]*records.NodeRecord

	Tunnels       *tunnel.Manager
	RelayProfiler *RelayProfiler

	sendMu  sync.Mutex
	recvMu  sync.Mutex
	seenApp map[string]bool
	AppMu   sync.Mutex
	AppRecv []string
	AppSent []string
}

func New(cfg *config.Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	id, err := identity.LoadOrCreate(cfg.Node.StateDir)
	if err != nil {
		return nil, err
	}
	codec := serialization.ByName(cfg.Protocol.Codec)
	rt := dht.NewRoutingTable(id.NodeID(), cfg.DHT.K)

	n := &Node{
		stopCh: make(chan struct{}), runDone: make(chan struct{}),
		ID:            id.NodeID(),
		seenApp:       make(map[string]bool),
		Identity:      id,
		RT:            rt,
		Codec:         codec,
		Config:        cfg,
		Store:         records.NewStore(),
		ownedRecords:  make(map[nodeid.ID]*records.NodeRecord),
		listenerReady: make(chan struct{}),
		Tunnels:       tunnel.NewManager(),
		RelayProfiler: NewRelayProfiler(),
	}

	n.joining.Store(true)
	log.Printf("node %s: RelayStrategy = %s", n.ID, n.Config.DHT.RelayStrategy)

	n.RPC = rpc.NewManagerWithWriteTimeout(id, codec, n.handleRPC, cfg.Node.ReadTimeout, cfg.Node.WriteTimeout)
	if cfg.Admission.Mode == "allowlist" {
		ids := make([]nodeid.ID, 0, len(cfg.Admission.NodeIDs))
		for _, raw := range cfg.Admission.NodeIDs {
			id, _ := nodeid.ParseHex(raw)
			ids = append(ids, id)
		}
		n.RPC.SetAdmissionAllowlist(ids)
	}
	if cfg.Protocol.MaxFramePayload > 0 {
		n.RPC.SetMaxPayload(uint32(cfg.Protocol.MaxFramePayload))
	}
	return n, nil
}

func (n *Node) Run(ctx context.Context) error {
	n.runStarted.Store(true)
	ctx, cancel := context.WithCancel(ctx)
	defer func() { cancel(); n.runWG.Wait(); close(n.runDone) }()
	go func() {
		select {
		case <-n.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

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

	n.joining.Store(false)
	if err := n.publishSelfRecord(ctx); err != nil {
		log.Printf("node %s: publish self record: %v", n.ID, err)
	}

	for _, loop := range []func(context.Context){n.refreshLoop, n.maintenanceLoop, n.experimentLoop} {
		n.runWG.Add(1)
		go func(loop func(context.Context)) { defer n.runWG.Done(); loop(ctx) }(loop)
	}

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
			for _, addr := range n.Config.Bootstrap.Peers {
				if ctx.Err() == nil {
					_ = n.bootstrapOne(ctx, addr)
				}
			}
			_, _ = n.Lookup(ctx, n.ID)
			var target nodeid.ID
			_, _ = rand.Read(target[:])
			res, err := n.Lookup(ctx, target)
			if err != nil {
				continue
			}
			log.Printf("node %s: refresh lookup target=%x... iterations=%d rpcs=%d",
				n.ID, target[:4], res.Iterations, res.RPCs)
		}
	}
}

func (n *Node) Addr() string {
	if n.Config.Node.AdvertiseAddress != "" {
		return n.Config.Node.AdvertiseAddress
	}
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
			if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
				return nil, err
			}
		}
		if m.Addr != "" {
			if err := dht.ValidateAddress(m.Addr); err != nil {
				return nil, err
			}
		}
		if m.Addr != "" && !n.joining.Load() {
			_, _ = n.RT.AddOrUpdate(&dht.Contact{ID: remote, Address: m.Addr, LastSeen: time.Now(), Verified: true, Verification: "tls_inbound_identity"})
		}
		out, _ := n.Codec.Marshal(rpc.PingResponse{})
		return &protocol.Frame{Type: protocol.FramePong, Payload: out}, nil

	case protocol.FrameFindNodeRequest:
		var m rpc.FindNodeRequest
		if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
			return nil, err
		}
		if m.Addr != "" {
			if err := dht.ValidateAddress(m.Addr); err != nil {
				return nil, err
			}
		}
		if m.Addr != "" && !n.joining.Load() {
			_, _ = n.RT.AddOrUpdate(&dht.Contact{ID: remote, Address: m.Addr, LastSeen: time.Now(), Verified: true, Verification: "tls_inbound_identity"})
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
		out, _ := n.Codec.Marshal(rpc.FindNodeResponse{Contacts: wire})
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
			return nil, fmt.Errorf("store: key mismatch")
		}
		err = n.Store.Put(key, &rec)
		success := (err == nil)

		out, _ := n.Codec.Marshal(rpc.StoreResponse{Success: success})
		return &protocol.Frame{Type: protocol.FrameStoreResponse, Payload: out}, nil

	case protocol.FrameFindValueRequest:
		var m rpc.FindValueRequest
		if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
			return nil, err
		}
		if m.Addr != "" {
			if err := dht.ValidateAddress(m.Addr); err != nil {
				return nil, err
			}
		}
		if m.Addr != "" && !n.joining.Load() {
			_, _ = n.RT.AddOrUpdate(&dht.Contact{ID: remote, Address: m.Addr, LastSeen: time.Now(), Verified: true, Verification: "tls_inbound_identity"})
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
		var remote nodeid.ID
		var err error
		if pin, ok := n.Config.Bootstrap.Pins[addr]; ok {
			remote, err = nodeid.ParseHex(pin)
			if err == nil {
				err = n.RPC.Connect(ctx, remote, addr)
			}
		} else if n.Config.Bootstrap.AllowUnpinned {
			remote, err = n.RPC.DialUnverified(ctx, addr)
		} else {
			return fmt.Errorf("bootstrap identity pin required for %s", addr)
		}
		if err == nil {
			c := &dht.Contact{ID: remote, Address: addr, LastSeen: time.Now()}
			if err := n.ping(ctx, c); err != nil {
				return err
			}
			if err := n.addContactWithEviction(ctx, c); err != nil {
				return err
			}
			log.Printf("node %s: bootstrap ok %s -> %s", n.ID, addr, remote)
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("bootstrap %s failed: %w", addr, lastErr)
}

func (n *Node) AddContactForTest(ctx context.Context, id nodeid.ID, addr string) error {
	return n.addContactWithEviction(ctx, &dht.Contact{ID: id, Address: addr, LastSeen: time.Now()})
}

func (n *Node) addContactWithEviction(ctx context.Context, c *dht.Contact) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := dht.ValidateAddress(c.Address); err != nil {
		return err
	}
	copy := *c
	copy.Verified = true
	copy.Verification = "tls_outbound_rpc"
	copy.LastSeen = time.Now()
	c = &copy
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
	if err := ctx.Err(); err != nil {
		return err
	}
	n.RT.ReplaceLRU(n.RT.BucketIndexFor(c.ID), c)
	return nil
}

func (n *Node) ping(ctx context.Context, c *dht.Contact) error {
	payload, _ := n.Codec.Marshal(rpc.PingRequest{Addr: n.Addr()})
	resp, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{Version: 1, Type: protocol.FramePing, Payload: payload})
	if err != nil {
		return err
	}
	if len(resp.Payload) > 0 {
		var pong rpc.PingResponse
		return n.Codec.Unmarshal(resp.Payload, &pong)
	}
	return nil
}

func (n *Node) findNodeRPC(ctx context.Context, c *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
	payload, _ := n.Codec.Marshal(rpc.FindNodeRequest{Target: target[:], Addr: n.Addr()})
	resp, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{Version: 1, Type: protocol.FrameFindNodeRequest, Payload: payload})
	if err != nil {
		return nil, err
	}
	var m rpc.FindNodeResponse
	if err := n.Codec.Unmarshal(resp.Payload, &m); err != nil {
		return nil, err
	}
	return n.validateContacts(m.Contacts, target)

}

func (n *Node) Lookup(ctx context.Context, target nodeid.ID) (*dht.LookupResult, error) {
	find := func(ctx context.Context, c *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
		contacts, err := n.findNodeRPC(ctx, c, target)
		if err == nil {
			_ = n.addContactWithEviction(ctx, c)
		}
		return contacts, err
	}
	res := dht.IterativeLookup(ctx, n.RT, target, find)
	n.mu.Lock()
	snapshot := *res
	snapshot.Contacts = nil
	n.lookups = append(n.lookups, &snapshot)
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
	_, err := n.PutRecordWithReport(ctx, rec)
	return err
}

func (n *Node) PublishRecord(ctx context.Context, rec *records.NodeRecord) error {
	n.mu.Lock()
	n.ownedRecords[rec.DHTKey()] = records.Clone(rec)
	n.mu.Unlock()
	return n.PutRecord(ctx, rec)
}

func (n *Node) GetRecord(ctx context.Context, key nodeid.ID) (*records.NodeRecord, error) {
	best, _ := n.Store.Get(key)
	var bestMu sync.Mutex
	findFunc := func(ctx context.Context, c *dht.Contact, k nodeid.ID) dht.FindValueResult {
		payload, _ := n.Codec.Marshal(rpc.FindValueRequest{Key: k[:], Addr: n.Addr()})
		resp, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{
			Version: 1, Type: protocol.FrameFindValueRequest, Payload: payload,
		})
		if err != nil {
			return dht.FindValueResult{Err: err}
		}
		var m rpc.FindValueResponse
		if err := n.Codec.Unmarshal(resp.Payload, &m); err != nil {
			return dht.FindValueResult{Err: err}
		}
		if len(m.Record) > 0 && len(m.Contacts) > 0 {
			return dht.FindValueResult{Err: errors.New("ambiguous FIND_VALUE response")}
		}
		if len(m.Record) > 0 {
			var candidate records.NodeRecord
			if err := n.Codec.Unmarshal(m.Record, &candidate); err != nil {
				return dht.FindValueResult{Err: err}
			}
			if err := candidate.Validate(); err != nil {
				return dht.FindValueResult{Err: err}
			}
			if candidate.DHTKey() != key {
				return dht.FindValueResult{Err: errors.New("key mismatch")}
			}
			bestMu.Lock()
			if best == nil || records.Compare(&candidate, best) > 0 {
				best = records.Clone(&candidate)
			}
			bestMu.Unlock()
			_ = n.addContactWithEviction(ctx, c)
			return dht.FindValueResult{Record: m.Record}
		}
		contacts, err := n.validateContacts(m.Contacts, k)
		if err != nil {
			return dht.FindValueResult{Err: err}
		}
		_ = n.addContactWithEviction(ctx, c)
		return dht.FindValueResult{Contacts: contacts}
	}
	_, trace := dht.IterativeLookupValue(ctx, n.RT, key, findFunc)
	n.mu.Lock()
	n.lookups = append(n.lookups, trace)
	n.mu.Unlock()
	if best == nil {
		return nil, errors.New("record not found")
	}
	if err := n.Store.Put(key, best); err != nil {
		return nil, err
	}
	return best, nil
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

func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.RPC.Close()
	if n.runStarted.Load() {
		<-n.runDone
	}
}

func (n *Node) publishSelfRecord(ctx context.Context) (err error) {
	n.selfPublishMu.Lock()
	defer n.selfPublishMu.Unlock()
	defer func() { n.selfRecordPending.Store(err != nil) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	existing := records.Clone(n.ownedRecords[dhtKeyForNode(n.ID)])
	n.mu.Unlock()
	if existing != nil {
		return n.PutRecord(ctx, existing)
	}
	seq, err := n.nextRecordSequence()
	if err != nil {
		return err
	}

	rec := &records.NodeRecord{
		NodeID:    n.ID,
		PublicKey: n.Identity.PublicKey,
		Addresses: []string{n.Addr()},
		Seq:       seq,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(n.recordTTL()).Unix(),
	}
	rec.Sign(n.Identity)
	return n.PublishRecord(ctx, rec)
}

func (n *Node) maintenanceLoop(ctx context.Context) {
	ttl := time.Duration(n.Config.DHT.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 180 * time.Second
	}
	republishInterval := n.Config.DHT.RepublishInterval
	if republishInterval <= 0 || republishInterval > ttl/2 {
		republishInterval = ttl / 2
	}
	cleanupTicker := time.NewTicker(10 * time.Second)
	republishTicker := time.NewTicker(republishInterval)
	retryTicker := time.NewTicker(time.Second)
	defer retryTicker.Stop()
	defer cleanupTicker.Stop()
	defer republishTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-retryTicker.C:
			if n.selfRecordPending.Load() {
				retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := n.publishSelfRecord(retryCtx)
				cancel()
				if err != nil && ctx.Err() == nil {
					log.Printf("node %s: initial identity publication retry: %v", n.ID, err)
				}
			}
		case <-cleanupTicker.C:
			n.Store.Cleanup()
			n.Tunnels.Cleanup(time.Now())
		case <-republishTicker.C:
			n.mu.Lock()
			recordsToRepublish := make([]*records.NodeRecord, 0, len(n.ownedRecords))
			for key, rec := range n.ownedRecords {
				updated := records.Clone(rec)
				updated.IssuedAt = time.Now().Unix()
				updated.ExpiresAt = time.Now().Add(ttl).Unix()
				seq, err := n.nextRecordSequence()
				if err != nil {
					log.Printf("sequence persistence failed: %v", err)
					continue
				}
				updated.Seq = seq
				updated.Sign(n.Identity)
				n.ownedRecords[key] = updated
				recordsToRepublish = append(recordsToRepublish, updated)
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

	if err := n.publishSelfRecord(ctx); err != nil {
		log.Printf("node %s: experiment republish: %v", n.ID, err)
	}
	time.Sleep(2 * time.Second)

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

	repeats := n.Config.Experiment.Repeats
	if repeats == 0 {
		repeats = 1
	}
	for sample := 0; sample < repeats; sample++ {
		for _, target := range targets {
			started := time.Now()
			lookupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			res, err := n.Lookup(lookupCtx, target)
			cancel()
			if err == nil {
				log.Printf("node %s: experiment lookup target=%s iterations=%d rpcs=%d",
					n.ID, target, res.Iterations, res.RPCs)
			}

			log.Printf("[EXPERIMENT] sample=%d operation=lookup success=%t duration_ms=%d", sample, err == nil, time.Since(started).Milliseconds())
			if target == n.ID {
				continue
			}
			started = time.Now()
			msgCtx, msgCancel := context.WithTimeout(ctx, 15*time.Second)
			err = n.SendMessage(msgCtx, target, n.experimentMessage())
			msgCancel()
			log.Printf("[EXPERIMENT] sample=%d operation=message success=%t duration_ms=%d", sample, err == nil, time.Since(started).Milliseconds())
			if err != nil {
				log.Printf("node %s: experiment send to %s failed: %v", n.ID, target[:8], err)
			}
		}

		n.dumpMetricsSafe()
		interval := n.Config.Experiment.Interval
		if interval <= 0 {
			interval = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
	n.dumpMetricsSafe()
}

func (n *Node) recordTTL() time.Duration {
	if n.Config.DHT.TTLSeconds <= 0 {
		return 180 * time.Second
	}
	return time.Duration(n.Config.DHT.TTLSeconds) * time.Second
}

func (n *Node) experimentMessage() string {
	if n.Config.Experiment.Message != "" {
		return n.Config.Experiment.Message
	}
	return "pcap-control-secret-17nov2026"
}

func (n *Node) ForwardedFrames() uint64 { return n.forwarded.Load() }
