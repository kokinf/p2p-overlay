package tests

import (
	"sort"
	"testing"

	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/nodeid"
)

func TestXORDistance(t *testing.T) {
	a := nodeid.MustFromHex("0000000000000000000000000000000000000000000000000000000000000000")
	b := nodeid.MustFromHex("0000000000000000000000000000000000000000000000000000000000000001")
	c := nodeid.MustFromHex("8000000000000000000000000000000000000000000000000000000000000000")

	if nodeid.Distance(a, b) != b {
		t.Fatal("distance(a,b) != b")
	}
	if !nodeid.Less(b, c, a) {
		t.Fatal("b must be closer to a than c")
	}
	if nodeid.CommonPrefixLen(a, c) != 0 {
		t.Fatal("a,c must share 0 bits")
	}
	if nodeid.CommonPrefixLen(a, b) != 255 {
		t.Fatalf("a,b should share 255 bits, got %d", nodeid.CommonPrefixLen(a, b))
	}
}

func TestBucketCapacityAndSelf(t *testing.T) {
	self := nodeid.MustFromHex("0000000000000000000000000000000000000000000000000000000000000000")
	rt := dht.NewRoutingTable(self, 3)

	if _, err := rt.AddOrUpdate(&dht.Contact{ID: self, Address: "x"}); err == nil {
		t.Fatal("expected error adding self")
	}

	for i := 0; i < 5; i++ {
		id := nodeid.ID{0}
		id[0] = 0x80 | byte(i) // 0x80, 0x81, 0x82, 0x83, 0x84
		lru, err := rt.AddOrUpdate(&dht.Contact{ID: id, Address: "addr"})
		if i < 3 {
			if err != nil {
				t.Fatalf("add #%d failed: %v", i, err)
			}
			if lru != nil {
				t.Fatalf("unexpected LRU on add #%d", i)
			}
		} else {
			if err == nil || lru == nil {
				t.Fatalf("expected bucket full on add #%d", i)
			}
		}
	}
	if rt.Size() != 3 {
		t.Fatalf("expected size 3, got %d", rt.Size())
	}

	dup := nodeid.ID{0}
	dup[0] = 0x80
	if _, err := rt.AddOrUpdate(&dht.Contact{ID: dup, Address: "addr2"}); err != nil {
		t.Fatal(err)
	}
	if rt.Size() != 3 {
		t.Fatalf("duplicate increased size: %d", rt.Size())
	}
}

func TestBucketEvictionPolicy(t *testing.T) {
	self := nodeid.MustFromHex("0000000000000000000000000000000000000000000000000000000000000000")
	rt := dht.NewRoutingTable(self, 2)

	for i := 0; i < 2; i++ {
		id := nodeid.ID{0}
		id[0] = 0x80 | byte(i)
		if _, err := rt.AddOrUpdate(&dht.Contact{ID: id, Address: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	newID := nodeid.ID{0}
	newID[0] = 0x80 | 2
	lru, err := rt.AddOrUpdate(&dht.Contact{ID: newID, Address: "a"})
	if err == nil || lru == nil {
		t.Fatal("expected ErrBucketFull")
	}
	if lru.ID[0] != 0x80 {
		t.Fatalf("LRU должен быть id[0]=0x80, got 0x%02x", lru.ID[0])
	}
}

func TestClosestOrdering(t *testing.T) {
	self := nodeid.MustFromHex("0000000000000000000000000000000000000000000000000000000000000000")
	rt := dht.NewRoutingTable(self, 4)
	ids := []byte{0x80, 0x40, 0x20, 0x10, 0x08}
	for _, b := range ids {
		id := nodeid.ID{0}
		id[0] = b
		_, _ = rt.AddOrUpdate(&dht.Contact{ID: id, Address: "a"})
	}
	target := nodeid.ID{0}
	closest := rt.Closest(target, 3)
	if len(closest) != 3 {
		t.Fatalf("expected 3, got %d", len(closest))
	}
	if !sort.SliceIsSorted(closest, func(i, j int) bool {
		return nodeid.Less(closest[i].ID, closest[j].ID, target)
	}) {
		t.Fatal("closest not sorted by XOR distance")
	}
}
