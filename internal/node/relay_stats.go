package node

import (
	"net"
	"p2p-overlay/internal/nodeid"
	"sync"
	"time"
)

const emaAlpha = 0.2

type RelayStats struct {
	Successes           int
	Failures            int
	EMASuccess          float64
	EMALatency          float64
	ConsecutiveFailures int
	Subnet              string
	LastSeen            time.Time
}

type RelayProfiler struct {
	mu    sync.RWMutex
	stats map[nodeid.ID]*RelayStats
}

func NewRelayProfiler() *RelayProfiler {
	return &RelayProfiler{stats: make(map[nodeid.ID]*RelayStats)}
}

func (rp *RelayProfiler) Get(id nodeid.ID) *RelayStats {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	if s, ok := rp.stats[id]; ok {
		copy := *s
		return &copy
	}
	return &RelayStats{EMASuccess: 0.5, EMALatency: 1.0, LastSeen: time.Now()}
}

func (rp *RelayProfiler) Update(id nodeid.ID, success bool, latencySec float64) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	s, ok := rp.stats[id]
	if !ok {
		s = &RelayStats{EMASuccess: 0.5, EMALatency: 1.0}
		rp.stats[id] = s
	}

	if success {
		s.Successes++
		s.ConsecutiveFailures = 0
		s.EMASuccess = emaAlpha*1.0 + (1-emaAlpha)*s.EMASuccess
		s.EMALatency = emaAlpha*latencySec + (1-emaAlpha)*s.EMALatency
	} else {
		s.Failures++
		s.ConsecutiveFailures++
		s.EMASuccess = emaAlpha*0.0 + (1-emaAlpha)*s.EMASuccess
	}
	s.LastSeen = time.Now()
}

func (rp *RelayProfiler) ObserveAddress(id nodeid.ID, address string) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return
	}
	ip := net.ParseIP(host)
	subnet := "unknown"
	if ip != nil {
		bits := 64
		size := 128
		if ip.To4() != nil {
			ip = ip.To4()
			bits = 24
			size = 32
		}
		subnet = (&net.IPNet{IP: ip.Mask(net.CIDRMask(bits, size)), Mask: net.CIDRMask(bits, size)}).String()
	}
	rp.mu.Lock()
	defer rp.mu.Unlock()
	s := rp.stats[id]
	if s == nil {
		s = &RelayStats{EMASuccess: 0.5, EMALatency: 1}
		rp.stats[id] = s
	}
	s.Subnet = subnet
}

func (rp *RelayProfiler) Snapshot() map[nodeid.ID]RelayStats {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	out := make(map[nodeid.ID]RelayStats, len(rp.stats))
	for id, s := range rp.stats {
		out[id] = *s
	}
	return out
}
