package rpc

type PingRequest struct{}

type PingResponse struct{}

type FindNodeRequest struct {
	Target []byte `msgpack:"target" json:"target"`
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
	Key []byte `msgpack:"key"`
}

type FindValueResponse struct {
	Record   []byte        `msgpack:"record,omitempty"`
	Contacts []ContactWire `msgpack:"contacts,omitempty"`
}
