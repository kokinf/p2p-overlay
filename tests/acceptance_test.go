package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"p2p-overlay/internal/config"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/node"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/records"
	"p2p-overlay/internal/rpc"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

type acceptanceRun struct {
	Replication        *node.ReplicationReport
	StoppedCandidate   string
	CandidateStoppedAt time.Time
	CandidateFailure   bool
	CandidateTrace     *dht.LookupResult
	N, K               int
	Scheme             string
	Repetition         int
	Started            time.Time
	ConvergenceMS      int64
	Rounds             []acceptanceRound
	Lookups            []*dht.LookupResult
	FullTables         int
	Holders            []string
	StoppedHolder      string
	SeedLookup         bool
	SeedValue          bool
	HolderValue        bool
	RelayRecovery      bool
	RecoveryMS         int64
	WarmMessageMS      []float64
	FirstMessageMS     float64
	Events             any
}
type acceptanceRound struct {
	At       time.Time
	Success  int
	Total    int
	Contacts int
}

func writeAcceptance(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, b, 0644); err != nil {
		t.Fatal(err)
	}
}
func found(res *dht.LookupResult, id nodeid.ID) bool {
	for _, s := range res.Closest {
		if s == id.String() {
			return true
		}
	}
	return false
}
func lookupAcceptance(n *node.Node, target nodeid.ID) *dht.LookupResult {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	r, _ := n.Lookup(ctx, target)
	return r
}

func TestAcceptanceMatrix(t *testing.T) {
	if os.Getenv("P2P_ACCEPTANCE") != "1" {
		t.Skip("set P2P_ACCEPTANCE=1 for the full acceptance matrix")
	}
	root := os.Getenv("P2P_ACCEPTANCE_OUT")
	if root == "" {
		root = filepath.Join("..", "results", "acceptance-"+time.Now().Format("20060102-150405"))
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	repeats := 5
	if raw := os.Getenv("P2P_ACCEPTANCE_REPEATS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 50 {
			t.Fatal("invalid repeat count")
		}
		repeats = value
	}
	if os.Getenv("P2P_ACCEPTANCE_SMOKE") == "1" {
		repeats = 1
	}
	cases := []struct {
		n, k   int
		scheme string
	}{{15, 3, "star"}, {15, 3, "ring"}, {21, 4, "star"}, {21, 4, "ring"}, {21, 4, "tree"}, {21, 4, "multi-seed"}}
	for _, c := range cases {
		if raw := os.Getenv("P2P_ACCEPTANCE_N"); raw != "" && raw != strconv.Itoa(c.n) {
			continue
		}
		if raw := os.Getenv("P2P_ACCEPTANCE_SCHEME"); raw != "" && raw != c.scheme {
			continue
		}
		for rep := 0; rep < repeats; rep++ {
			t.Run(fmt.Sprintf("n%d-%s-%d", c.n, c.scheme, rep), func(t *testing.T) {
				dir := filepath.Join(root, fmt.Sprintf("n%d-%s-%d", c.n, c.scheme, rep))
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				report := acceptanceRun{N: c.n, K: c.k, Scheme: c.scheme, Repetition: rep, Started: time.Now()}
				defer func() { writeAcceptance(t, filepath.Join(dir, "scenario.json"), report) }()
				nodes := make([]*node.Node, c.n)
				cancels := make([]context.CancelFunc, c.n)
				done := make([]chan error, c.n)
				ports := make([]int, c.n)
				usedPorts := make(map[int]bool)
				for i := range ports {
					for {
						port := mustFreePort(t)
						if !usedPorts[port] {
							ports[i] = port
							usedPorts[port] = true
							break
						}
					}
				}
				stateDirs := make([]string, c.n)
				identities := make([]*identity.Identity, c.n)
				for i := range nodes {
					stateDirs[i] = t.TempDir()
					var err error
					identities[i], err = identity.LoadOrCreate(stateDirs[i])
					if err != nil {
						t.Fatal(err)
					}
				}
				for i := range nodes {
					pins := make(map[string]string)
					var peers []string
					add := func(j int) {
						addr := fmt.Sprintf("127.0.0.1:%d", ports[j])
						peers = append(peers, addr)
						pins[addr] = identities[j].NodeID().String()
					}
					switch c.scheme {
					case "star":
						if i > 0 {
							add(0)
						}
					case "ring":
						add((i + 1) % c.n)
					case "tree":
						if i > 0 {
							add((i - 1) / 2)
						}
					case "multi-seed":
						if i < 3 {
							if i > 0 {
								add(0)
							}
						} else {
							for seed := 0; seed < 3; seed++ {
								add(seed)
							}
						}
					}
					cfg := &config.Config{Node: config.NodeConfig{Port: ports[i], StateDir: stateDirs[i], ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}, Protocol: config.ProtocolConfig{Codec: "msgpack"}, DHT: config.DHTConfig{K: c.k, Alpha: 3, Replication: 3, TTLSeconds: 180, RefreshInterval: 2 * time.Second, RepublishInterval: 60 * time.Second}, Tunnel: config.TunnelConfig{Relays: 3, PoolSize: 3}, Bootstrap: config.BootstrapConfig{Pins: pins, Scheme: c.scheme, Peers: peers}}
					var err error
					nodes[i], err = node.New(cfg)
					if err != nil {
						t.Fatal(err)
					}
				}
				stop := func(i int) {
					if cancels[i] != nil {
						cancels[i]()
						nodes[i].Stop()
						select {
						case err := <-done[i]:
							if err != nil {
								t.Errorf("node %d Run: %v", i, err)
							}
						case <-time.After(5 * time.Second):
							t.Errorf("node %d did not stop", i)
						}
						cancels[i] = nil
					}
				}
				defer func() {
					for i := range nodes {
						stop(i)
					}
				}()
				for i := range nodes {
					ctx, cancel := context.WithCancel(context.Background())
					cancels[i] = cancel
					done[i] = make(chan error, 1)
					go func(i int) { done[i] <- nodes[i].Run(ctx) }(i)
				}
				stable := 0
				lastContacts := 0
				deadline := time.Now().Add(50 * time.Second)
				for time.Now().Before(deadline) {
					round := acceptanceRound{At: time.Now(), Total: c.n}
					for i, n := range nodes {
						if found(lookupAcceptance(n, nodes[(i+7)%c.n].ID), nodes[(i+7)%c.n].ID) {
							round.Success++
						}
						round.Contacts += len(n.RT.Closest(n.ID, 0))
					}
					report.Rounds = append(report.Rounds, round)
					if round.Success*100 >= 95*round.Total && lastContacts > 0 && absInt(round.Contacts-lastContacts)*100 <= lastContacts*10 {
						stable++
					} else {
						stable = 0
					}
					lastContacts = round.Contacts
					if stable >= 2 {
						break
					}
					time.Sleep(2 * time.Second)
				}
				if stable < 2 {
					t.Fatal("operational convergence not reached")
				}
				report.ConvergenceMS = time.Since(report.Started).Milliseconds()
				for _, n := range nodes {
					if len(n.RT.Closest(n.ID, 0)) >= c.n-1 {
						report.FullTables++
					}
				}
				if report.FullTables*5 > c.n {
					t.Fatal("degenerate routing tables")
				}
				for pass := 0; pass < 3 && len(report.Lookups) < 30; pass++ {
					for i, n := range nodes {
						for j, target := range nodes {
							if i == j || len(report.Lookups) >= 30 {
								continue
							}
							known := false
							for _, ct := range n.RT.Closest(target.ID, 0) {
								if ct.ID == target.ID {
									known = true
								}
							}
							if known {
								continue
							}
							res := lookupAcceptance(n, target.ID)

							intermediate := false
							for _, it := range res.Trace {
								for _, id := range it.Queried {
									if id != target.ID.String() {
										intermediate = true
									}
								}
							}
							if !intermediate {
								t.Fatal("no intermediate DHT node")
							}
							report.Lookups = append(report.Lookups, res)
						}
					}
				}
				if len(report.Lookups) < 30 {
					t.Fatalf("only %d qualifying lookups", len(report.Lookups))
				}
				successful := 0
				for _, r := range report.Lookups {
					if found(r, r.Target) {
						successful++
					}
				}
				if successful*100 < 95*len(report.Lookups) {
					t.Fatalf("lookup success rate below 95%%: %d/%d", successful, len(report.Lookups))
				}
				for i, n := range nodes {
					if err := n.DumpMetrics(filepath.Join(dir, fmt.Sprintf("metrics-%02d.json", i))); err != nil {
						t.Fatal(err)
					}
				}
				candidateIndex := -1
				var candidateOrigin *node.Node
				var candidateTarget *node.Node
			outer:
				for i, origin := range nodes {
					for j, target := range nodes {
						if i == j {
							continue
						}
						known := false
						for _, ct := range origin.RT.Closest(target.ID, 0) {
							if ct.ID == target.ID {
								known = true
							}
						}
						if known {
							continue
						}
						for _, ct := range origin.RT.Closest(target.ID, c.k) {
							for z, member := range nodes {
								if member.ID == ct.ID && z > 1 && z != i && z != j {
									candidateIndex = z
									candidateOrigin = origin
									candidateTarget = target
									break outer
								}
							}
						}
					}
				}
				if candidateIndex < 0 {
					t.Fatal("no candidate failure scenario")
				}
				savedCfg := nodes[candidateIndex].Config
				report.StoppedCandidate = nodes[candidateIndex].ID.String()
				candidateLookupCtx, candidateLookupCancel := context.WithTimeout(context.Background(), 8*time.Second)
				report.CandidateTrace = dht.IterativeLookup(candidateLookupCtx, candidateOrigin.RT, candidateTarget.ID, func(ctx context.Context, contact *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
					if contact.ID.String() == report.StoppedCandidate {
						stop(candidateIndex)
						report.CandidateStoppedAt = time.Now()
					}
					payload, _ := candidateOrigin.Codec.Marshal(rpc.FindNodeRequest{Target: target[:], Addr: candidateOrigin.Addr()})
					response, err := candidateOrigin.RPC.Call(ctx, contact.ID, contact.Address, &protocol.Frame{Version: 1, Type: protocol.FrameFindNodeRequest, Payload: payload})
					if err != nil {
						return nil, err
					}
					var wire rpc.FindNodeResponse
					if err = candidateOrigin.Codec.Unmarshal(response.Payload, &wire); err != nil {
						return nil, err
					}
					var contacts []*dht.Contact
					for _, c := range wire.Contacts {
						id, err := nodeid.FromBytes(c.NodeID)
						if err != nil {
							return nil, err
						}
						contacts = append(contacts, &dht.Contact{ID: id, Address: c.Address})
					}
					return contacts, nil
				})
				candidateLookupCancel()
				report.CandidateFailure = found(report.CandidateTrace, candidateTarget.ID) && report.CandidateTrace.Errors > 0 && !report.CandidateStoppedAt.IsZero()

				if !report.CandidateFailure {
					t.Fatal("lookup failed to handle stopped candidate")
				}
				replacement, err := node.New(savedCfg)
				if err != nil {
					t.Fatal(err)
				}
				nodes[candidateIndex] = replacement
				candidateCtx, candidateCancel := context.WithCancel(context.Background())
				cancels[candidateIndex] = candidateCancel
				done[candidateIndex] = make(chan error, 1)
				go func() { done[candidateIndex] <- replacement.Run(candidateCtx) }()
				_ = replacement.Addr()
				time.Sleep(100 * time.Millisecond)
				sender := nodes[1]
				receiver := nodes[len(nodes)-1]
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				err = sender.SendMessage(ctx, receiver.ID, "acceptance-cold")
				cancel()
				report.FirstMessageMS = float64(time.Since(start).Microseconds()) / 1000
				if err != nil {
					t.Fatal(err)
				}
				active := sender.Tunnels.ListSender()
				count := 0
				var usedRelay nodeid.ID
				for _, tun := range active {
					if tun.State == 1 {
						count++
						if usedRelay == (nodeid.ID{}) {
							usedRelay = tun.Path[1].ID
						}
					}
				}
				if count < 3 {
					t.Fatal("pool contains fewer than three active tunnels")
				}
				for i := 0; i < 30; i++ {
					start = time.Now()
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
					err = sender.SendMessage(ctx, receiver.ID, fmt.Sprintf("acceptance-warm-%d", i))
					cancel()
					if err != nil {
						t.Fatal(err)
					}
					report.WarmMessageMS = append(report.WarmMessageMS, float64(time.Since(start).Microseconds())/1000)
				}
				signer, err := identity.LoadOrCreate(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				rec := &records.NodeRecord{NodeID: signer.NodeID(), PublicKey: signer.PublicKey, Addresses: []string{"127.0.0.1:12345"}, Seq: 1, IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(180 * time.Second).Unix()}
				rec.Sign(signer)
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
				storeReport, storeErr := nodes[1].PutRecordWithReport(ctx, rec)
				report.Replication = &storeReport
				err = storeErr
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				selected := report.Replication
				if selected == nil || len(selected.Holders) != 3 {
					t.Fatal("three remote holders missing")
				}
				candidates := make(map[string]bool)
				var previous nodeid.ID
				for i, raw := range selected.CandidateIDs {
					id, err := nodeid.ParseHex(raw)
					if err != nil {
						t.Fatal(err)
					}
					if i > 0 && nodeid.Less(id, previous, rec.DHTKey()) {
						t.Fatal("candidate pool not XOR sorted")
					}
					previous = id
					candidates[raw] = true
				}
				holderIndex := 0
				for i, probe := range selected.Probes {
					if i >= len(selected.CandidateIDs) || probe.NodeID != selected.CandidateIDs[i] {
						t.Fatal("nearest available selection skipped an unprobed candidate")
					}
					if !probe.Available {
						if probe.Error == "" {
							t.Fatal("unavailable candidate has no diagnostic")
						}
						continue
					}
					if holderIndex >= 3 || selected.Holders[holderIndex].NodeID != probe.NodeID {
						t.Fatal("holders do not match nearest available candidates")
					}
					holderIndex++
				}
				if holderIndex != 3 {
					t.Fatal("insufficient verified remote holders")
				}
				for _, h := range selected.Holders {
					if !candidates[h.NodeID] || h.NodeID == nodes[1].ID.String() {
						t.Fatal("invalid remote holder")
					}
					stored := false
					for _, member := range nodes {
						if member.ID.String() == h.NodeID {
							_, stored = member.Store.Get(rec.DHTKey())
						}
					}
					if !stored {
						t.Fatal("selected holder did not store the record")
					}
				}

				holder := -1
				reader := -1
				for i, n := range nodes {
					if _, ok := n.Store.Get(rec.DHTKey()); ok {
						report.Holders = append(report.Holders, n.ID.String())
						if i > 1 && i != len(nodes)-1 {
							holder = i
						}
					} else if i > 1 && i != len(nodes)-1 {
						reader = i
					}
				}
				if len(report.Holders) < 3 || holder < 0 || reader < 0 {
					t.Fatal("missing replicas or independent reader")
				}
				report.StoppedHolder = nodes[holder].ID.String()
				stop(holder)
				ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
				got, err := nodes[reader].GetRecord(ctx, rec.DHTKey())
				cancel()
				report.HolderValue = err == nil && got.Seq == rec.Seq
				if !report.HolderValue {
					t.Fatal("record unavailable after holder failure")
				}
				restart := func(i int) {
					cfg := nodes[i].Config
					var err error
					nodes[i], err = node.New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancels[i] = cancel
					done[i] = make(chan error, 1)
					go func() { done[i] <- nodes[i].Run(ctx) }()
					_ = nodes[i].Addr()
					time.Sleep(100 * time.Millisecond)
				}
				restart(holder)
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
				err = nodes[1].PutRecord(ctx, rec)
				cancel()
				if err != nil {
					t.Fatal("restore replicas between independent failure scenarios", err)
				}
				stop(0)
				report.SeedLookup = found(lookupAcceptance(nodes[1], nodes[reader].ID), nodes[reader].ID)
				seedReader := -1
				for i, n := range nodes {
					if i > 1 && i != holder && i != reader {
						if _, ok := n.Store.Get(rec.DHTKey()); !ok {
							seedReader = i
							break
						}
					}
				}
				if seedReader < 0 {
					t.Fatal("no uncached reader")
				}
				ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
				got, err = nodes[seedReader].GetRecord(ctx, rec.DHTKey())
				cancel()
				report.SeedValue = err == nil && got.Seq == rec.Seq
				if !report.SeedLookup || !report.SeedValue {
					t.Fatal("seed shutdown broke DHT")
				}
				restart(0)
				relayIndex := -1
				for i, n := range nodes {
					if n.ID == usedRelay {
						relayIndex = i
					}
				}
				if relayIndex < 0 {
					t.Fatal("relay not identified")
				}
				stop(relayIndex)
				start = time.Now()
				ctx, cancel = context.WithTimeout(context.Background(), 25*time.Second)
				err = sender.SendMessage(ctx, receiver.ID, "acceptance-after-failure")
				cancel()
				report.RecoveryMS = time.Since(start).Milliseconds()
				report.RelayRecovery = err == nil
				if err != nil {
					t.Fatal(err)
				}
				report.Events = sender.Tunnels.Events()
				receiver.AppMu.Lock()
				received := len(receiver.AppRecv)
				receiver.AppMu.Unlock()
				if received != 32 {
					t.Fatalf("received %d messages, expected 32", received)
				}
			})
		}
	}
}
func absInt(i int) int {
	if i < 0 {
		return -i
	}
	return i
}
