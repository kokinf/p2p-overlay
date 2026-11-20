package tests

import (
	"context"
	"encoding/json"
	"os"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/records"
	"path/filepath"
	"testing"
	"time"
)

func TestThreeRemoteStoreResults(t *testing.T) {
	nodes, stop := startTestNetwork(t, 6, mustFreePort(t))
	defer stop()
	owner := nodes[0]
	rec := &records.NodeRecord{NodeID: owner.ID, PublicKey: owner.Identity.PublicKey, Addresses: []string{owner.Addr()}, Seq: uint64(time.Now().UnixNano()), IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix()}
	rec.Sign(owner.Identity)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	report, err := owner.PutRecordWithReport(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	if !report.LocalCopy.Acknowledged || len(report.Holders) != 3 || report.Confirmations != 3 {
		t.Fatalf("incomplete report: %+v", report)
	}
	seen := map[string]bool{owner.ID.String(): true}
	for _, h := range report.Holders {
		if seen[h.NodeID] || !h.Acknowledged || h.Error != "" {
			t.Fatalf("invalid remote result: %+v", h)
		}
		seen[h.NodeID] = true
		id, err := nodeid.ParseHex(h.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		stored := false
		for _, n := range nodes {
			if n.ID == id {
				got, ok := n.Store.Get(rec.DHTKey())
				stored = ok && got.Seq == rec.Seq
			}
		}
		if !stored {
			t.Fatal("acknowledged holder has no matching record")
		}
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	t.Log(string(b))
	if dir := os.Getenv("P2P_TRUST_RESULTS"); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "replication.json"), b, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
