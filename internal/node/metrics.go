package node

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"p2p-overlay/internal/dht"
)

type NodeMetrics struct {
	NodeID    string               `json:"node_id"`
	Address   string               `json:"address"`
	Buckets   []dht.BucketSnapshot `json:"buckets"`
	Lookups   []*dht.LookupResult  `json:"lookups"`
	StoreSize int                  `json:"store_size"`
	Timestamp time.Time            `json:"timestamp"`
}

func (n *Node) DumpMetrics(path string) error {
	m := NodeMetrics{
		NodeID:    n.ID.String(),
		Address:   n.Addr(),
		Buckets:   n.RT.Snapshot(),
		Lookups:   n.Lookups(),
		StoreSize: n.Store.Size(),
		Timestamp: time.Now(),
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (n *Node) dumpMetricsSafe() {
	dir := n.Config.Experiment.OutputDir
	if dir == "" {
		dir = "results"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("node %s: mkdir %s: %v", n.ID, dir, err)
		return
	}
	short := n.ID.String()
	if len(short) > 8 {
		short = short[:8]
	}
	path := filepath.Join(dir, fmt.Sprintf("metrics-%s.json", short))
	if err := n.DumpMetrics(path); err != nil {
		log.Printf("node %s: metrics dump: %v", n.ID, err)
	}
}
