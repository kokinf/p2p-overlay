package dht

import (
	"errors"
	"sort"
	"sync"

	"p2p-overlay/internal/nodeid"
)

var ErrBucketFull = errors.New("bucket full")

type RoutingTable struct {
	self    nodeid.ID
	k       int
	buckets []*Bucket
	mu      sync.RWMutex
}

func NewRoutingTable(self nodeid.ID, k int) *RoutingTable {
	rt := &RoutingTable{
		self:    self,
		k:       k,
		buckets: make([]*Bucket, nodeid.Size*8),
	}
	for i := range rt.buckets {
		rt.buckets[i] = NewBucket(k)
	}
	return rt
}

func (rt *RoutingTable) Self() nodeid.ID { return rt.self }
func (rt *RoutingTable) K() int          { return rt.k }

func (rt *RoutingTable) bucketIndex(id nodeid.ID) int {
	d := nodeid.Distance(rt.self, id)
	for i := 0; i < nodeid.Size; i++ {
		if d[i] == 0 {
			continue
		}
		for bit := 7; bit >= 0; bit-- {
			if d[i]&(1<<uint(bit)) != 0 {
				return i*8 + (7 - bit)
			}
		}
	}
	return -1
}

func (rt *RoutingTable) BucketIndexFor(id nodeid.ID) int {
	return rt.bucketIndex(id)
}

func (rt *RoutingTable) Bucket(i int) *Bucket {
	return rt.buckets[i]
}

func (rt *RoutingTable) AddOrUpdate(c *Contact) (*Contact, error) {
	if c.ID == rt.self {
		return nil, errors.New("cannot add self")
	}
	idx := rt.bucketIndex(c.ID)
	if idx < 0 {
		return nil, errors.New("invalid bucket index")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	b := rt.buckets[idx]
	if b.Get(c.ID) != nil {
		b.MoveToEnd(c.ID)
		return nil, nil
	}
	if b.Append(c) {
		return nil, nil
	}
	return b.First(), ErrBucketFull
}

func (rt *RoutingTable) ReplaceLRU(idx int, c *Contact) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if idx < 0 || idx >= len(rt.buckets) {
		return
	}
	rt.buckets[idx].ReplaceFirst(c)
}

func (rt *RoutingTable) Touch(id nodeid.ID) {
	idx := rt.bucketIndex(id)
	if idx < 0 {
		return
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	rt.buckets[idx].MoveToEnd(id)
}

func (rt *RoutingTable) Remove(id nodeid.ID) {
	idx := rt.bucketIndex(id)
	if idx < 0 {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.buckets[idx].Remove(id)
}

func (rt *RoutingTable) Closest(target nodeid.ID, n int) []*Contact {
	rt.mu.RLock()
	var all []*Contact
	for _, b := range rt.buckets {
		all = append(all, b.Contacts()...)
	}
	rt.mu.RUnlock()
	sort.Slice(all, func(i, j int) bool {
		return nodeid.Less(all[i].ID, all[j].ID, target)
	})
	if n > 0 && len(all) > n {
		all = all[:n]
	}
	return all
}

func (rt *RoutingTable) Size() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	total := 0
	for _, b := range rt.buckets {
		total += b.Len()
	}
	return total
}

type BucketSnapshot struct {
	Index    int      `json:"index"`
	Contacts []string `json:"contacts"`
}

func (rt *RoutingTable) Snapshot() []BucketSnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := make([]BucketSnapshot, 0, len(rt.buckets))
	for i, b := range rt.buckets {
		cs := b.Contacts()
		if len(cs) == 0 {
			continue
		}
		ids := make([]string, len(cs))
		for j, c := range cs {
			ids[j] = c.ID.String()
		}
		out = append(out, BucketSnapshot{Index: i, Contacts: ids})
	}
	return out
}
