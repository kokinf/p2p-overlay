package tunnel

import (
	"sort"
	"sync"
	"time"
)

type Manager struct {
	events        []StateEvent
	mu            sync.RWMutex
	senderTunnels map[[16]byte]*Tunnel
	relayStates   map[[16]byte]*RelayState
	recvStates    map[[16]byte]*ReceiverState
}

func NewManager() *Manager {
	return &Manager{
		senderTunnels: make(map[[16]byte]*Tunnel),
		relayStates:   make(map[[16]byte]*RelayState),
		recvStates:    make(map[[16]byte]*ReceiverState),
	}
}

func (m *Manager) PutSender(t *Tunnel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := *t
	copy.Path = append([]Hop(nil), t.Path...)
	copy.E2EKey = append([]byte(nil), t.E2EKey...)
	m.senderTunnels[t.ID] = &copy
}

func (m *Manager) GetSender(id [16]byte) (*Tunnel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.senderTunnels[id]
	if !ok {
		return nil, false
	}
	copy := *t
	copy.Path = append([]Hop(nil), t.Path...)
	copy.E2EKey = append([]byte(nil), t.E2EKey...)
	return &copy, true
}

func (m *Manager) UpdateState(id [16]byte, state State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.senderTunnels[id]; ok {
		m.events = append(m.events, StateEvent{TunnelID: id, From: t.State, To: state, At: time.Now()})
		t.State = state
	}
}

func (m *Manager) DeleteSender(id [16]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.senderTunnels, id)
}

func (m *Manager) ListSender() []*Tunnel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Tunnel
	for _, t := range m.senderTunnels {
		copy := *t
		copy.Path = append([]Hop(nil), t.Path...)
		copy.E2EKey = append([]byte(nil), t.E2EKey...)
		out = append(out, &copy)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (m *Manager) PutRelay(r *RelayState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.relayStates[r.TunnelID] = r
}

func (m *Manager) GetRelay(id [16]byte) (*RelayState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.relayStates[id]
	return r, ok
}

func (m *Manager) DeleteRelay(id [16]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.relayStates, id)
}

func (m *Manager) PutReceiver(r *ReceiverState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvStates[r.TunnelID] = r
}

func (m *Manager) GetReceiver(id [16]byte) (*ReceiverState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.recvStates[id]
	return r, ok
}

func (m *Manager) Cleanup(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, t := range m.senderTunnels {
		if !t.ExpiresAt.IsZero() && now.After(t.ExpiresAt) {
			m.events = append(m.events, StateEvent{TunnelID: id, From: t.State, To: StateDead, At: now})
			delete(m.senderTunnels, id)
		}
	}
	if len(m.events) > 10000 {
		m.events = append([]StateEvent(nil), m.events[len(m.events)-10000:]...)
	}
	for id, r := range m.relayStates {
		if now.After(r.ExpiresAt) {
			delete(m.relayStates, id)
		}
	}
	for id, r := range m.recvStates {
		if now.After(r.ExpiresAt) {
			delete(m.recvStates, id)
		}
	}
}

func (m *Manager) NextSequence(id [16]byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.senderTunnels[id]
	if t == nil {
		return 0
	}
	seq := t.NextSeq
	t.NextSeq++
	return seq
}
func (m *Manager) Events() []StateEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]StateEvent(nil), m.events...)
}

func (m *Manager) DeleteReceiver(id [16]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.recvStates, id)
}
