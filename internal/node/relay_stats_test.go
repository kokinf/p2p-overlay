package node

import (
	"encoding/json"
	"fmt"
	"os"
	"p2p-overlay/internal/config"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/nodeid"
	"path/filepath"
	"sync"
	"testing"
)

func TestRelayProfileSnapshot(t *testing.T) {
	p := NewRelayProfiler()
	id := nodeid.ID{1}
	p.ObserveAddress(id, "192.0.2.4:9000")
	p.Update(id, true, .1)
	copy := p.Get(id)
	copy.Successes = 999
	if p.Get(id).Successes != 1 || p.Get(id).Subnet != "192.0.2.0/24" {
		t.Fatal("mutable snapshot or invalid subnet")
	}
	p.ObserveAddress(nodeid.ID{2}, "[2001:db8:1::2]:9000")
	if p.Get(nodeid.ID{2}).Subnet != "2001:db8:1::/64" {
		t.Fatal("invalid IPv6 subnet")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				p.Update(id, j%2 == 0, .01)
				p.Get(id)
				p.Snapshot()
			}
		}()
	}
	wg.Wait()
	s := p.Get(id)
	if s.Successes+s.Failures != 801 {
		t.Fatalf("lost updates: %+v", s)
	}
}
func TestRelayRatingSelection(t *testing.T) {
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 64, RelayStrategy: "rating"}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.RPC.Close()
	for i := 1; i <= 6; i++ {
		id := nodeid.ID{byte(i)}
		if _, err := n.RT.AddOrUpdate(&dht.Contact{ID: id, Address: fmt.Sprintf("192.0.%d.1:9000", i)}); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 8; j++ {
			n.RelayProfiler.Update(id, i <= 3, float64(i)/100)
		}
	}
	selected := n.selectRelays(nodeid.ID{}, 3)
	if len(selected) != 3 {
		t.Fatal("missing relays")
	}
	for _, c := range selected {
		if c.ID[0] > 3 {
			t.Fatal("failed candidate preferred")
		}
	}
}
func TestSubnetConcentrationExperiment(t *testing.T) {
	n, err := New(&config.Config{Node: config.NodeConfig{StateDir: t.TempDir()}, DHT: config.DHTConfig{K: 64, RelayStrategy: "random"}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.RPC.Close()
	for i := 1; i <= 28; i++ {
		id := nodeid.ID{byte(i)}
		ip := fmt.Sprintf("198.51.%d.1:9000", i)
		if i <= 20 {
			ip = fmt.Sprintf("192.0.2.%d:9000", i)
		}
		if _, err := n.RT.AddOrUpdate(&dht.Contact{ID: id, Address: ip}); err != nil {
			t.Fatal(err)
		}
	}
	type sample struct {
		Limit       int      `json:"limit"`
		Index       int      `json:"index"`
		Adversarial int      `json:"adversarial"`
		IDs         []string `json:"ids"`
	}
	var rows []sample
	for _, limit := range []int{0, 1} {
		n.Config.Tunnel.MaxPerSubnet = limit
		compromised := 0
		for i := 0; i < 100; i++ {
			selected := n.selectRelays(nodeid.ID{}, 3)
			if len(selected) != 3 {
				t.Fatal("insufficient diverse relays")
			}
			r := sample{Limit: limit, Index: i}
			for _, c := range selected {
				r.IDs = append(r.IDs, c.ID.String())
				if c.ID[0] <= 20 {
					r.Adversarial++
				}
			}
			if r.Adversarial == 3 {
				compromised++
			}
			if limit == 1 && r.Adversarial > 1 {
				t.Fatal("subnet cap violated")
			}
			rows = append(rows, r)
		}
		t.Logf("SUBNET_METRIC limit=%d samples=100 all_adversarial=%d", limit, compromised)
	}
	if out := os.Getenv("P2P_RELAY_RESULTS"); out != "" {
		if err := os.MkdirAll(out, 0755); err != nil {
			t.Fatal(err)
		}
		b, _ := json.MarshalIndent(rows, "", "  ")
		if err := os.WriteFile(filepath.Join(out, "subnet-selection.json"), b, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
