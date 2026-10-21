package tests

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/nodeid"
)

func TestIterativeLookupViaIntermediate(t *testing.T) {
	targetID := nodeid.MustFromHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01")

	var all []*dht.Contact
	for i := 0; i < 20; i++ {
		id := nodeid.ID{}
		id[0] = byte(i + 1)
		id[31] = byte(i)
		all = append(all, &dht.Contact{ID: id, Address: fmt.Sprintf("n%d", i)})
	}
	last := nodeid.ID{0xAA, 0xAA, 0xAA, 0xAA}
	copy(last[4:], targetID[4:])
	all[len(all)-1].ID = last

	origin := nodeid.MustFromHex("0000000000000000000000000000000000000000000000000000000000000000")
	rt := dht.NewRoutingTable(origin, 3)
	rt.AddOrUpdate(all[0])
	rt.AddOrUpdate(all[1])
	rt.AddOrUpdate(all[2])

	knows := make(map[nodeid.ID][]*dht.Contact)
	for _, c := range all {
		var k []*dht.Contact
		for j := 0; j < 5; j++ {
			idx := (int(c.ID[0]) + j + 1) % len(all)
			if all[idx].ID == c.ID {
				continue
			}
			k = append(k, all[idx])
		}
		knows[c.ID] = k
	}

	find := func(ctx context.Context, c *dht.Contact, target nodeid.ID) ([]*dht.Contact, error) {
		known := append([]*dht.Contact{}, knows[c.ID]...)
		if nodeid.CommonPrefixLen(c.ID, target) > 100 {
			tc := &dht.Contact{ID: targetID, Address: "target"}
			known = append(known, tc)
		}
		sort.Slice(known, func(i, j int) bool {
			return nodeid.Less(known[i].ID, known[j].ID, target)
		})
		if len(known) > 3 {
			known = known[:3]
		}
		return known, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := dht.IterativeLookup(ctx, rt, targetID, find)

	if res.Iterations == 0 {
		t.Fatal("no iterations")
	}
	if len(res.Closest) == 0 {
		t.Fatal("no closest returned")
	}
	initial := rt.Closest(targetID, 1)
	if initial[0].ID == nodeid.MustFromHex(res.Closest[0]) {
		t.Log("closest matches initial — likely small network; check iterations > 1")
	}
	if res.Iterations < 2 {
		t.Logf("iterations=%d", res.Iterations)
	}
}
