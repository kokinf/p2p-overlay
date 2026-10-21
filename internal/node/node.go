package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/protocol/serialization"
	"p2p-overlay/internal/rpc"
)

type Node struct {
	ID       nodeid.ID
	Identity *identity.Identity
	RT       *dht.RoutingTable
	RPC      *rpc.Manager
	Codec    serialization.Codec
	Config   *config.Config

	listener net.Listener

	mu      sync.Mutex
	lookups []*dht.LookupResult
}

func New(cfg *config.Config) (*Node, error) {
	id, err := identity.LoadOrCreate(cfg.Node.StateDir)
	if err != nil {
		return nil, err
	}
	codec := serialization.ByName(cfg.Protocol.Codec)
	rt := dht.NewRoutingTable(id.NodeID(), cfg.DHT.K)

	n := &Node{
		ID:       id.NodeID(),
		Identity: id,
		RT:       rt,
		Codec:    codec,
		Config:   cfg,
	}
	n.RPC = rpc.NewManager(id, codec, n.handleRPC, cfg.Node.ReadTimeout)
	return n, nil
}

func (n *Node) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Config.Node.Port))
	if err != nil {
		return err
	}
	n.listener = ln
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
	return fmt.Sprintf("127.0.0.1:%d", n.Config.Node.Port)
}

func (n *Node) handleRPC(ctx context.Context, remote nodeid.ID, req *protocol.Frame) (*protocol.Frame, error) {
	switch req.Type {
	case protocol.FramePing:
		return &protocol.Frame{Type: protocol.FramePong}, nil

	case protocol.FrameFindNodeRequest:
		var m rpc.FindNodeRequest
		if err := n.Codec.Unmarshal(req.Payload, &m); err != nil {
			return nil, err
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
	_, err := n.RPC.Call(ctx, c.ID, c.Address,
		&protocol.Frame{Version: 1, Type: protocol.FramePing})
	return err
}

func (n *Node) findNodeRPC(ctx context.Context, c *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
	payload, err := n.Codec.Marshal(rpc.FindNodeRequest{Target: target[:]})
	if err != nil {
		return nil, err
	}
	resp, err := n.RPC.Call(ctx, c.ID, c.Address, &protocol.Frame{
		Version: 1, Type: protocol.FrameFindNodeRequest, Payload: payload,
	})
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
		if err != nil {
			continue
		}
		if id == n.ID {
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
