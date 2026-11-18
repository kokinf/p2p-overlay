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

func TestRecordNegativeClassesAndStoreIsolation(t *testing.T) {
	id, err := identity.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	valid := &records.NodeRecord{NodeID: id.NodeID(), PublicKey: id.PublicKey, Addresses: []string{"127.0.0.1:1234"}, Seq: 2, IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix()}
	valid.Sign(id)
	cases := map[string]func(*records.NodeRecord){"identity": func(r *records.NodeRecord) { r.NodeID[0] ^= 1 }, "signature": func(r *records.NodeRecord) { r.Signature[0] ^= 1 }, "expired": func(r *records.NodeRecord) { r.ExpiresAt = time.Now().Unix() - 1; r.Sign(id) }, "address": func(r *records.NodeRecord) { r.Addresses = []string{"127.0.0.1:0"}; r.Sign(id) }, "future": func(r *records.NodeRecord) {
		r.IssuedAt = time.Now().Add(time.Hour).Unix()
		r.ExpiresAt = r.IssuedAt + 60
		r.Sign(id)
	}, "alias-size": func(r *records.NodeRecord) { r.Alias = string(make([]byte, 129)); r.Sign(id) }}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := records.Clone(valid)
			mutate(r)
			if r.Validate() == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
	store := records.NewStore()
	key := valid.DHTKey()
	if err := store.Put(key, valid); err != nil {
		t.Fatal(err)
	}
	valid.Seq = 0
	valid.Addresses[0] = "bad"
	got, _ := store.Get(key)
	if got.Seq != 2 || got.Addresses[0] == "bad" {
		t.Fatal("caller mutated stored record")
	}
	got.Seq = 0
	got, _ = store.Get(key)
	if got.Seq != 2 {
		t.Fatal("reader mutated store")
	}
}
