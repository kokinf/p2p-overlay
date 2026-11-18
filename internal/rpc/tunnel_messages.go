package rpc

type HopInfo struct {
	NodeID  []byte `msgpack:"node_id"`
	Address string `msgpack:"address"`
}

type TunnelBuildRequest struct {
	TunnelID  []byte    `msgpack:"tunnel_id"`
	Path      []HopInfo `msgpack:"path"`
	HopIndex  int       `msgpack:"hop_index"`
	SenderID  []byte    `msgpack:"sender_id"`
	E2EPub    []byte    `msgpack:"e2e_pub"`
	E2ESig    []byte    `msgpack:"e2e_sig"`
	MaxHops   int       `msgpack:"max_hops"`
	ExpiresAt int64     `msgpack:"expires_at"`
}

type TunnelBuildResponse struct {
	TunnelID       []byte `msgpack:"tunnel_id"`
	HopIndex       int    `msgpack:"hop_index"`
	Accepted       bool   `msgpack:"accepted"`
	Reason         string `msgpack:"reason,omitempty"`
	ReceiverID     []byte `msgpack:"receiver_id,omitempty"`
	ReceiverSig    []byte `msgpack:"receiver_sig"`
	ReceiverE2EPub []byte `msgpack:"receiver_e2e_pub,omitempty"`
}

type TunnelDataRequest struct {
	TunnelID   []byte `msgpack:"tunnel_id"`
	HopIndex   int    `msgpack:"hop_index"`
	Seq        uint64 `msgpack:"seq"`
	Nonce      []byte `msgpack:"nonce"`
	Ciphertext []byte `msgpack:"ct"`
}

type TunnelAckResponse struct {
	Tag      []byte `msgpack:"tag"`
	TunnelID []byte `msgpack:"tunnel_id"`
	Seq      uint64 `msgpack:"seq"`
	Success  bool   `msgpack:"success"`
}

type TunnelCloseRequest struct {
	TunnelID []byte `msgpack:"tunnel_id"`
	Reason   string `msgpack:"reason,omitempty"`
}

type AppMessageRequest struct {
	MessageID []byte `msgpack:"message_id"`
	Type      string `msgpack:"type"` // text, file_meta, file_block
	Payload   []byte `msgpack:"payload"`
}

type AppAckResponse struct {
	MessageID []byte `msgpack:"message_id"`
	Success   bool   `msgpack:"success"`
}

type FileMetadataRequest struct {
	FileID      []byte `msgpack:"file_id"`
	Filename    string `msgpack:"filename"`
	Size        int64  `msgpack:"size"`
	SHA256      []byte `msgpack:"sha256"`
	BlockSize   int    `msgpack:"block_size"`
	TotalBlocks int    `msgpack:"total_blocks"`
}

type FileBlockRequest struct {
	FileID   []byte `msgpack:"file_id"`
	BlockIdx int    `msgpack:"block_idx"`
	Data     []byte `msgpack:"data"`
}
