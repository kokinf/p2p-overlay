package tunnel

import (
	"p2p-overlay/internal/nodeid"
	"p2p-overlay/internal/rpc"
	"time"
)

type State int

const (
	StateBuilding State = iota
	StateActive
	StateDegraded
	StateClosing
	StateDead
)

func (s State) String() string {
	return [...]string{"BUILDING", "ACTIVE", "DEGRADED", "CLOSING", "DEAD"}[s]
}

type Hop struct {
	ID      nodeid.ID
	Address string
}

type Tunnel struct {
	ID         [16]byte
	Path       []Hop
	State      State
	ExpiresAt  time.Time
	CreatedAt  time.Time
	ReceiverID nodeid.ID
	E2EKey     []byte
	NextSeq    uint64
}

type RelayState struct {
	TunnelID  [16]byte
	HopIndex  int
	PrevID    nodeid.ID
	NextID    nodeid.ID
	NextAddr  string
	ExpiresAt time.Time
}

type ReceiverState struct {
	PrevID       nodeid.ID
	TunnelID     [16]byte
	SenderID     nodeid.ID
	SenderE2EPub []byte
	E2EKey       []byte
	ExpiresAt    time.Time
	SeenMsgs     map[string]bool
	Files        map[string]*FileReceiveState
}

type FileReceiveState struct {
	Meta     rpc.FileMetadataRequest
	Blocks   [][]byte
	Received int
}

type StateEvent struct {
	TunnelID [16]byte  `json:"tunnel_id"`
	From     State     `json:"from"`
	To       State     `json:"to"`
	At       time.Time `json:"at"`
}
