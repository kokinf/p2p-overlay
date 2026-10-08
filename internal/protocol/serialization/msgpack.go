package serialization

import "github.com/vmihailenco/msgpack/v5"

type MsgPackCodec struct{}

func (MsgPackCodec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (MsgPackCodec) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

func (MsgPackCodec) Name() string { return "msgpack" }
