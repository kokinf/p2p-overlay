package tunnel

import (
	"sync"
	"time"
)

type Manager struct {
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
	m.senderTunnels[t.ID] = t
}

func (m *Manager) GetSender(id [16]byte) (*Tunnel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.senderTunnels[id]
	return t, ok
}

func (m *Manager) UpdateState(id [16]byte, state State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.senderTunnels[id]; ok {
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
		out = append(out, t)
	}
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
