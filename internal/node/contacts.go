package node

import (
	"fmt"
	"p2p-overlay/internal/dht"
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/rpc"
)

func (n *Node) validateContacts(wire []rpc.ContactWire, target nodeid.ID) ([]*dht.Contact, error) {
	if len(wire) > n.RT.K() {
		return nil, fmt.Errorf("contact response exceeds limit %d", n.RT.K())
	}
	seen := make(map[nodeid.ID]bool)
	out := make([]*dht.Contact, 0, len(wire))
	var previous nodeid.ID
	for i, w := range wire {
		id, err := nodeid.FromBytes(w.NodeID)
		if err != nil {
			return nil, err
		}
		if err := dht.ValidateAddress(w.Address); err != nil {
			return nil, err
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate contact")
		}
		seen[id] = true
		if i > 0 && nodeid.Less(id, previous, target) {
			return nil, fmt.Errorf("contacts not XOR sorted")
		}
		previous = id
		if id != n.ID {
			out = append(out, &dht.Contact{ID: id, Address: w.Address})
		}
	}
	return out, nil
}
