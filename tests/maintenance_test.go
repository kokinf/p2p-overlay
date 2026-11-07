package tests

import (
	"testing"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/records"
)

func TestTTLCleanupAndRollback(t *testing.T) {
	store := records.NewStore()
	id, _ := identity.LoadOrCreate(t.TempDir())

	rec := &records.NodeRecord{
		NodeID:    id.NodeID(),
		PublicKey: id.PublicKey,
		Addresses: []string{"127.0.0.1:9000"},
		Seq:       1,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Second).Unix(),
	}
	rec.Sign(id)
	key := rec.DHTKey()

	_ = store.Put(key, rec)

	if _, ok := store.Get(key); !ok {
		t.Fatal("record should be found before TTL")
	}

	time.Sleep(1100 * time.Millisecond)

	if _, ok := store.Get(key); ok {
		t.Fatal("expired record should not be returned")
	}

	deleted := store.Cleanup()
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}
}

func TestAliasResolution(t *testing.T) {
	id1, _ := identity.LoadOrCreate(t.TempDir())
	id2, _ := identity.LoadOrCreate(t.TempDir())

	rawAlias := "  AliceNode "
	normAlias := "alicenode"

	rec1 := &records.NodeRecord{
		NodeID:    id1.NodeID(),
		PublicKey: id1.PublicKey,
		Addresses: []string{"127.0.0.1:9001"},
		Alias:     normAlias,
		Seq:       1,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
	}
	rec1.Sign(id1)

	rec2 := &records.NodeRecord{
		NodeID:    id2.NodeID(),
		PublicKey: id2.PublicKey,
		Addresses: []string{"127.0.0.1:9002"},
		Alias:     normAlias,
		Seq:       2,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
	}
	rec2.Sign(id2)

	store := records.NewStore()
	key := rec1.DHTKey()

	_ = store.Put(key, rec1)

	err := store.Put(key, rec2)
	if err != nil {
		t.Fatalf("failed to update with higher Seq: %v", err)
	}

	res, _ := store.Get(key)
	if res.NodeID != id2.NodeID() {
		t.Fatal("expected record from node 2 (highest Seq)")
	}

	if records.NormalizeAlias(rawAlias) != normAlias {
		t.Fatalf("normalize failed: got '%s', want '%s'", records.NormalizeAlias(rawAlias), normAlias)
	}
}
