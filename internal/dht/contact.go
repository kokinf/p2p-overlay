package dht

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"p2p-overlay/internal/nodeid"
)

type Contact struct {
	Verification string
	Verified     bool
	ID           nodeid.ID
	Address      string
	LastSeen     time.Time
}

type ContactWire struct {
	NodeID  []byte `msgpack:"node_id" json:"node_id"`
	Address string `msgpack:"address" json:"address"`
}

func ToWire(c *Contact) ContactWire {
	return ContactWire{NodeID: c.ID[:], Address: c.Address}
}

func FromWire(w ContactWire) (*Contact, error) {
	if err := ValidateAddress(w.Address); err != nil {
		return nil, err
	}
	id, err := nodeid.FromBytes(w.NodeID)
	if err != nil {
		return nil, err
	}
	return &Contact{ID: id, Address: w.Address}, nil
}

func ValidateAddress(address string) error {
	if len(address) == 0 || len(address) > 255 || strings.ContainsAny(address, " \t\r\n\x00/\\") {
		return fmt.Errorf("invalid contact address")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("invalid contact address %q", address)
	}
	for _, ch := range port {
		if ch < '0' || ch > '9' {
			return fmt.Errorf("invalid contact port")
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid contact port")
	}
	ip := net.ParseIP(host)
	if ip != nil && (ip.IsUnspecified() || ip.IsMulticast() || ip.Equal(net.IPv4bcast)) {
		return fmt.Errorf("contact IP must identify a unicast destination")
	}
	if ip == nil {
		for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return fmt.Errorf("invalid contact hostname")
			}
			for _, ch := range label {
				if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-') {
					return fmt.Errorf("invalid contact hostname")
				}
			}
		}
	}
	return nil
}
