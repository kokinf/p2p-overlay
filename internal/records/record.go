package records

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
)

const (
	RecordTypeNode  = "node"
	RecordTypeAlias = "alias"
	MaxRecordSize   = 16 * 1024 // 16 KB
)

type NodeRecord struct {
	NodeID    nodeid.ID `msgpack:"node_id"`
	PublicKey []byte    `msgpack:"public_key"`
	Addresses []string  `msgpack:"addresses"`
	Alias     string    `msgpack:"alias,omitempty"`
	Seq       uint64    `msgpack:"seq"`
	IssuedAt  int64     `msgpack:"issued_at"`
	ExpiresAt int64     `msgpack:"expires_at"`
	Signature []byte    `msgpack:"signature"`
}

func NormalizeAlias(alias string) string {
	return strings.ToLower(strings.TrimSpace(alias))
}

func (r *NodeRecord) CanonicalBytes() []byte {
	var buf bytes.Buffer

	buf.Write(r.NodeID[:])
	buf.Write(r.PublicKey)

	aliasBytes := []byte(r.Alias)
	buf.WriteByte(byte(len(aliasBytes)))
	buf.Write(aliasBytes)

	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, r.Seq)
	buf.Write(seqBytes)

	timeBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(timeBytes[0:8], uint64(r.IssuedAt))
	binary.BigEndian.PutUint64(timeBytes[8:16], uint64(r.ExpiresAt))
	buf.Write(timeBytes)

	for _, addr := range r.Addresses {
		buf.WriteByte(byte(len(addr)))
		buf.WriteString(addr)
	}

	return buf.Bytes()
}

func (r *NodeRecord) Sign(id *identity.Identity) {
	r.Signature = id.Sign(r.CanonicalBytes())
}

func (r *NodeRecord) Validate() error {
	computedID, err := identity.NodeIDFromPublicKey(r.PublicKey)
	if err != nil {
		return err
	}
	if computedID != r.NodeID {
		return errors.New("node_id does not match public_key")
	}

	if len(r.Signature) != ed25519.SignatureSize {
		return errors.New("invalid signature size")
	}
	if !ed25519.Verify(ed25519.PublicKey(r.PublicKey), r.CanonicalBytes(), r.Signature) {
		return errors.New("invalid signature")
	}

	if len(r.Addresses) == 0 || len(r.Addresses) > 10 {
		return errors.New("invalid number of addresses")
	}

	now := time.Now().Unix()
	if r.ExpiresAt <= now {
		return errors.New("record expired")
	}
	if r.IssuedAt > now+300 {
		return errors.New("record issued in the future")
	}

	return nil
}

func (r *NodeRecord) DHTKey() nodeid.ID {
	var keyStr string
	if r.Alias != "" {
		keyStr = fmt.Sprintf("alias:%s", NormalizeAlias(r.Alias))
	} else {
		keyStr = fmt.Sprintf("node:%s", r.NodeID.String())
	}
	return sha256.Sum256([]byte(keyStr))
}
