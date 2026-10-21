package dht

import (
	"time"

	"p2p-overlay/internal/nodeid"
)

type Contact struct {
	ID       nodeid.ID
	Address  string
	LastSeen time.Time
}

type ContactWire struct {
	NodeID  []byte `msgpack:"node_id" json:"node_id"`
	Address string `msgpack:"address" json:"address"`
}

func ToWire(c *Contact) ContactWire {
	return ContactWire{NodeID: c.ID[:], Address: c.Address}
}

func FromWire(w ContactWire) (*Contact, error) {
	id, err := nodeid.FromBytes(w.NodeID)
	if err != nil {
		return nil, err
	}
	return &Contact{ID: id, Address: w.Address, LastSeen: time.Now()}, nil
}
