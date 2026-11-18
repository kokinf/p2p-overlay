package records

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"p2p-overlay/internal/nodeid"
)

var (
	ErrRecordExpired    = errors.New("record expired")
	ErrSequenceRollback = errors.New("sequence number rollback detected")
)

type Store struct {
	now   func() time.Time
	mu    sync.RWMutex
	data  map[nodeid.ID]*NodeRecord
	floor map[nodeid.ID]uint64
}

func NewStore() *Store { return NewStoreWithClock(time.Now) }
func NewStoreWithClock(now func() time.Time) *Store {
	return &Store{
		now:   now,
		data:  make(map[nodeid.ID]*NodeRecord),
		floor: make(map[nodeid.ID]uint64),
	}
}

func (s *Store) Put(key nodeid.ID, rec *NodeRecord) error {
	if rec == nil {
		return errors.New("nil record")
	}
	if key != rec.DHTKey() {
		return errors.New("DHT key mismatch")
	}
	if err := rec.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if seq, ok := s.floor[key]; ok && rec.Seq < seq {
		return ErrSequenceRollback
	}
	if _, ok := s.floor[key]; !ok && len(s.floor) >= 100000 {
		return errors.New("record capacity exceeded")
	}
	if existing, ok := s.data[key]; ok {
		if rec.Seq < existing.Seq {
			return ErrSequenceRollback
		}
		if rec.Seq == existing.Seq && Compare(rec, existing) <= 0 {
			return nil
		}
	}

	s.floor[key] = rec.Seq
	s.data[key] = Clone(rec)
	return nil
}

func (s *Store) Get(key nodeid.ID) (*NodeRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.data[key]
	if !ok {
		return nil, false
	}

	if rec.ExpiresAt <= s.now().Unix() {
		return nil, false
	}

	return Clone(rec), true
}

func (s *Store) Cleanup() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().Unix()
	deleted := 0
	for k, v := range s.data {
		if v.ExpiresAt <= now {
			delete(s.data, k)
			deleted++
		}
	}
	return deleted
}

func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func Clone(r *NodeRecord) *NodeRecord {
	if r == nil {
		return nil
	}
	c := *r
	c.PublicKey = append([]byte(nil), r.PublicKey...)
	c.Signature = append([]byte(nil), r.Signature...)
	c.Addresses = append([]string(nil), r.Addresses...)
	return &c
}

func Compare(a, b *NodeRecord) int {
	if a.Seq > b.Seq {
		return 1
	}
	if a.Seq < b.Seq {
		return -1
	}
	if c := bytes.Compare(a.NodeID[:], b.NodeID[:]); c != 0 {
		return c
	}
	return bytes.Compare(a.CanonicalBytes(), b.CanonicalBytes())
}
