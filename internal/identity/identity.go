package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"p2p-overlay/internal/nodeid"
)

const (
	pubFile  = "identity.pub"
	privFile = "identity.key"
)

type Identity struct {
	PublicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
}

func LoadOrCreate(dir string) (*Identity, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	pubPath := filepath.Join(dir, pubFile)
	privPath := filepath.Join(dir, privFile)

	if _, err := os.Stat(pubPath); err == nil {
		return load(pubPath, privPath)
	}
	id, err := generate()
	if err != nil {
		return nil, err
	}
	if err := id.save(pubPath, privPath); err != nil {
		return nil, err
	}
	return id, nil
}

func generate() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{PublicKey: pub, privateKey: priv}, nil
}

func load(pubPath, privPath string) (*Identity, error) {
	pubRaw, err := os.ReadFile(pubPath)
	if err != nil {
		return nil, err
	}
	privRaw, err := os.ReadFile(privPath)
	if err != nil {
		return nil, err
	}
	pub, err := base64.StdEncoding.DecodeString(string(pubRaw))
	if err != nil {
		return nil, fmt.Errorf("decode pub: %w", err)
	}
	priv, err := base64.StdEncoding.DecodeString(string(privRaw))
	if err != nil {
		return nil, fmt.Errorf("decode priv: %w", err)
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("invalid public key size")
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid private key size")
	}
	return &Identity{PublicKey: ed25519.PublicKey(pub), privateKey: ed25519.PrivateKey(priv)}, nil
}

func (i *Identity) save(pubPath, privPath string) error {
	if err := os.WriteFile(pubPath,
		[]byte(base64.StdEncoding.EncodeToString(i.PublicKey)), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(privPath,
		[]byte(base64.StdEncoding.EncodeToString(i.privateKey)), 0o600); err != nil {
		return err
	}
	return nil
}

func (i *Identity) NodeID() nodeid.ID {
	return NodeIDFromPublicKeyMust(i.PublicKey)
}

func NodeIDFromPublicKey(pub []byte) (nodeid.ID, error) {
	if len(pub) != ed25519.PublicKeySize {
		return nodeid.ID{}, errors.New("invalid public key size")
	}
	h := sha256.Sum256(CanonicalEncodePublicKey(pub))
	return nodeid.ID(h), nil
}

func NodeIDFromPublicKeyMust(pub []byte) nodeid.ID {
	id, err := NodeIDFromPublicKey(pub)
	if err != nil {
		panic(err)
	}
	return id
}

func (i *Identity) Sign(msg []byte) []byte {
	return ed25519.Sign(i.privateKey, msg)
}

func Verify(pub ed25519.PublicKey, msg, sig []byte) bool {
	return ed25519.Verify(pub, msg, sig)
}
