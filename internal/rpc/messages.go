package rpc

type PingRequest struct {
	Addr string `msgpack:"addr,omitempty" json:"addr,omitempty"`
}

type PingResponse struct{}

type FindNodeRequest struct {
	Target []byte `msgpack:"target" json:"target"`
	Addr   string `msgpack:"addr,omitempty" json:"addr,omitempty"`
}

type ContactWire struct {
	NodeID  []byte `msgpack:"node_id" json:"node_id"`
	Address string `msgpack:"address" json:"address"`
}

type FindNodeResponse struct {
	Contacts []ContactWire `msgpack:"contacts" json:"contacts"`
}

type StoreRequest struct {
	Key    []byte `msgpack:"key"`
	Record []byte `msgpack:"record"`
}

type StoreResponse struct {
	Success bool `msgpack:"success"`
}

type FindValueRequest struct {
	Key  []byte `msgpack:"key"`
	Addr string `msgpack:"addr,omitempty" json:"addr,omitempty"`
}

type FindValueResponse struct {
	Record   []byte        `msgpack:"record,omitempty"`
	Contacts []ContactWire `msgpack:"contacts,omitempty"`
}
