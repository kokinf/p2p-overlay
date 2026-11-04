package records

import (
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
	mu   sync.RWMutex
	data map[nodeid.ID]*NodeRecord
}

func NewStore() *Store {
	return &Store{
		data: make(map[nodeid.ID]*NodeRecord),
	}
}

func (s *Store) Put(key nodeid.ID, rec *NodeRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.data[key]; ok {
		if rec.Seq < existing.Seq {
			return ErrSequenceRollback
		}
		if rec.Seq == existing.Seq {
			return nil
		}
	}

	s.data[key] = rec
	return nil
}

func (s *Store) Get(key nodeid.ID) (*NodeRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.data[key]
	if !ok {
		return nil, false
	}

	if rec.ExpiresAt <= time.Now().Unix() {
		return nil, false
	}

	return rec, true
}

func (s *Store) Cleanup() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()
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
