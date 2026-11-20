package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"sort"
	"sync"
	"time"
)

type StoreAttempt struct {
	NodeID       string `json:"node_id"`
	Address      string `json:"address"`
	Acknowledged bool   `json:"acknowledged"`
	Error        string `json:"error,omitempty"`
}
type AvailabilityProbe struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	Available bool   `json:"available"`
	Error     string `json:"error,omitempty"`
}
type ReplicationReport struct {
	LookupID      string              `json:"lookup_id"`
	CandidateIDs  []string            `json:"candidate_ids"`
	Probes        []AvailabilityProbe `json:"probes"`
	Key           string              `json:"key"`
	Owner         string              `json:"owner"`
	LocalCopy     StoreAttempt        `json:"local_copy"`
	Holders       []StoreAttempt      `json:"holders"`
	Confirmations int                 `json:"confirmations"`
}

func (n *Node) PutRecordWithReport(ctx context.Context, rec *records.NodeRecord) (report ReplicationReport, err error) {
	key := rec.DHTKey()
	report = ReplicationReport{Key: key.String(), Owner: rec.NodeID.String(), LocalCopy: StoreAttempt{NodeID: n.ID.String(), Address: n.Addr()}}
	defer func() { b, _ := json.Marshal(report); log.Printf("[DHT_STORE] %s", b) }()
	if err = n.Store.Put(key, rec); err != nil {
		report.LocalCopy.Error = err.Error()
		return report, fmt.Errorf("local store: %w", err)
	}
	report.LocalCopy.Acknowledged = true
	lookup, err := n.Lookup(ctx, key)
	if err != nil {
		return report, err
	}
	report.LookupID = lookup.LookupID
	candidates := append([]*dht.Contact(nil), lookup.Contacts...)
	sort.Slice(candidates, func(i, j int) bool { return nodeid.Less(candidates[i].ID, candidates[j].ID, key) })
	for _, c := range candidates {
		if c.ID != n.ID && c.ID != rec.NodeID {
			report.CandidateIDs = append(report.CandidateIDs, c.ID.String())
		}
	}
	seen := make(map[nodeid.ID]bool)
	for _, c := range candidates {
		if c.ID == n.ID || c.ID == rec.NodeID || seen[c.ID] {
			continue
		}
		seen[c.ID] = true
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		probeErr := n.ping(probeCtx, c)
		cancel()
		probe := AvailabilityProbe{NodeID: c.ID.String(), Address: c.Address, Available: probeErr == nil}
		if probeErr != nil {
			probe.Error = probeErr.Error()
		}
		report.Probes = append(report.Probes, probe)
		if probeErr != nil {
			continue
		}
		report.Holders = append(report.Holders, StoreAttempt{NodeID: c.ID.String(), Address: c.Address})
		if len(report.Holders) == 3 {
			break
		}
	}
	if len(report.Holders) != 3 {
		return report, fmt.Errorf("three remote holders required, found %d", len(report.Holders))
	}
	recBytes, err := n.Codec.Marshal(rec)
	if err != nil {
		return report, err
	}
	payload, err := n.Codec.Marshal(rpc.StoreRequest{Key: key[:], Record: recBytes})
	if err != nil {
		return report, err
	}
	var wg sync.WaitGroup
	for i := range report.Holders {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			attempt := &report.Holders[index]
			id, _ := nodeid.ParseHex(attempt.NodeID)
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			resp, e := n.RPC.Call(callCtx, id, attempt.Address, &protocol.Frame{Version: 1, Type: protocol.FrameStoreRequest, Payload: payload})
			if e == nil {
				var result rpc.StoreResponse
				e = n.Codec.Unmarshal(resp.Payload, &result)
				if e == nil && !result.Success {
					e = fmt.Errorf("STORE rejected")
				}
			}
			if e != nil {
				attempt.Error = e.Error()
			} else {
				attempt.Acknowledged = true
			}
		}(i)
	}
	wg.Wait()
	for _, attempt := range report.Holders {
		if attempt.Acknowledged {
			report.Confirmations++
		}
	}
	if report.Confirmations < 2 {
		return report, fmt.Errorf("replication: %d remote confirmations, need 2", report.Confirmations)
	}
	return report, nil
}
