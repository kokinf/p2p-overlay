package records

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
)

const (
	RecordTypeNode  = "node"
	RecordTypeAlias = "alias"
	MaxRecordSize   = 16 * 1024 // 16 KB
	MaxAddresses    = 10
	MaxAliasBytes   = 128
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
	if len(r.PublicKey) != ed25519.PublicKeySize {
		return errors.New("invalid public key size")
	}
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

	if len(r.Addresses) == 0 || len(r.Addresses) > MaxAddresses {
		return fmt.Errorf("invalid number of addresses: %d", len(r.Addresses))
	}
	for _, addr := range r.Addresses {
		if err := validateAddress(addr); err != nil {
			return fmt.Errorf("invalid address %q: %w", addr, err)
		}
	}

	if len(r.Alias) > MaxAliasBytes {
		return fmt.Errorf("alias too long: %d > %d", len(r.Alias), MaxAliasBytes)
	}

	now := time.Now().Unix()
	if r.ExpiresAt <= now {
		return errors.New("record expired")
	}
	if r.IssuedAt > now+300 {
		return errors.New("record issued in the future")
	}
	if r.ExpiresAt <= r.IssuedAt {
		return errors.New("expires_at must be after issued_at")
	}

	if size := len(r.CanonicalBytes()); size > MaxRecordSize {
		return fmt.Errorf("record exceeds MaxRecordSize: %d > %d", size, MaxRecordSize)
	}

	return nil
}

func validateAddress(addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return errors.New("empty host")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port out of range: %d", port)
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
