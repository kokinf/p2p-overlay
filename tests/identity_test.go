package tests

import (
	"os"
	"path/filepath"
	"testing"

	"p2p-overlay/internal/identity"
)

func TestIdentityPersistent(t *testing.T) {
	dir := t.TempDir()
	id1, err := identity.LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	nid1 := id1.NodeID()

	id2, err := identity.LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	nid2 := id2.NodeID()

	if nid1 != nid2 {
		t.Fatalf("node id changed: %s vs %s", nid1, nid2)
	}
	if _, err := os.Stat(filepath.Join(dir, "identity.key")); err != nil {
		t.Fatalf("private key not stored: %v", err)
	}
}

func TestNodeIDFromPublicKey(t *testing.T) {
	dir := t.TempDir()
	id, err := identity.LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	computed, err := identity.NodeIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if computed != id.NodeID() {
		t.Fatalf("mismatch: %s vs %s", computed, id.NodeID())
	}
	other, _ := identity.LoadOrCreate(t.TempDir())
	if other.NodeID() == id.NodeID() {
		t.Fatal("two fresh nodes produced same id")
	}
}

func TestNodeIDSubstitutionDetected(t *testing.T) {
	a, _ := identity.LoadOrCreate(t.TempDir())
	b, _ := identity.LoadOrCreate(t.TempDir())
	computed, _ := identity.NodeIDFromPublicKey(b.PublicKey)
	if computed == a.NodeID() {
		t.Fatal("substituted public key produced matching id")
	}
}
