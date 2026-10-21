package rpc

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"

	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/protocol"
)

type helloPayload struct {
	NodeID    []byte `msgpack:"node_id"`
	PublicKey []byte `msgpack:"public_key"`
	Nonce     []byte `msgpack:"nonce"`
	Signature []byte `msgpack:"signature"`
}

func (m *Manager) buildHello() ([]byte, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	pub := []byte(m.id.PublicKey)
	toSign := append(append([]byte{}, pub...), nonce...)
	sig := m.id.Sign(toSign)

	nid := m.id.NodeID()
	return m.codec.Marshal(helloPayload{
		NodeID:    nid[:],
		PublicKey: pub,
		Nonce:     nonce,
		Signature: sig,
	})
}

func (m *Manager) verifyHello(payload []byte) (nodeid.ID, error) {
	var p helloPayload
	if err := m.codec.Unmarshal(payload, &p); err != nil {
		return nodeid.ID{}, err
	}
	if len(p.PublicKey) != ed25519.PublicKeySize {
		return nodeid.ID{}, errors.New("bad pubkey size")
	}
	computed, err := identity.NodeIDFromPublicKey(p.PublicKey)
	if err != nil {
		return nodeid.ID{}, err
	}
	declared, err := nodeid.FromBytes(p.NodeID)
	if err != nil {
		return nodeid.ID{}, err
	}
	if computed != declared {
		return nodeid.ID{}, errors.New("node id does not match public key")
	}
	toVerify := append(append([]byte{}, p.PublicKey...), p.Nonce...)
	if !ed25519.Verify(ed25519.PublicKey(p.PublicKey), toVerify, p.Signature) {
		return nodeid.ID{}, errors.New("invalid hello signature")
	}
	return declared, nil
}

func (m *Manager) clientHandshake(c *conn, expected nodeid.ID, expectedSet bool) error {
	payload, err := m.buildHello()
	if err != nil {
		return err
	}
	if err := c.write(&protocol.Frame{
		Version: 1, Type: protocol.FrameHello, Payload: payload,
	}); err != nil {
		return err
	}
	resp, err := c.read(m.timeout)
	if err != nil {
		return err
	}
	if resp.Type != protocol.FrameHelloOK {
		return fmt.Errorf("expected HELLO_OK, got %d", resp.Type)
	}
	remote, err := m.verifyHello(resp.Payload)
	if err != nil {
		return err
	}
	if expectedSet && remote != expected {
		return fmt.Errorf("remote id mismatch: got %s, want %s", remote, expected)
	}
	c.remote = remote
	return nil
}

func (m *Manager) serverHandshake(c *conn) error {
	hello, err := c.read(m.timeout)
	if err != nil {
		return err
	}
	if hello.Type != protocol.FrameHello {
		return fmt.Errorf("expected HELLO, got %d", hello.Type)
	}
	remote, err := m.verifyHello(hello.Payload)
	if err != nil {
		return err
	}
	c.remote = remote
	payload, err := m.buildHello()
	if err != nil {
		return err
	}
	return c.write(&protocol.Frame{
		Version: 1, Type: protocol.FrameHelloOK, Payload: payload,
	})
}
