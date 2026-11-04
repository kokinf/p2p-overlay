package tests

import (
	"testing"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/records"
)

func TestRecordValidationAndRollback(t *testing.T) {
	id, _ := identity.LoadOrCreate(t.TempDir())

	rec := &records.NodeRecord{
		NodeID:    id.NodeID(),
		PublicKey: id.PublicKey,
		Addresses: []string{"127.0.0.1:9000"},
		Seq:       1,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Minute).Unix(),
	}
	rec.Sign(id)

	if err := rec.Validate(); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}

	rec.Signature[0] ^= 0xFF
	if err := rec.Validate(); err == nil {
		t.Fatal("invalid signature accepted")
	}
	rec.Sign(id)

	store := records.NewStore()
	key := rec.DHTKey()

	if err := store.Put(key, rec); err != nil {
		t.Fatalf("first put failed: %v", err)
	}

	oldRec := *rec
	oldRec.Seq = 0
	oldRec.Sign(id)
	if err := store.Put(key, &oldRec); err != records.ErrSequenceRollback {
		t.Fatalf("expected rollback error, got: %v", err)
	}
}

func TestAliasNormalization(t *testing.T) {
	alias1 := "  MyNode "
	alias2 := "mynode"
	if records.NormalizeAlias(alias1) != alias2 {
		t.Fatal("alias not normalized correctly")
	}
}
