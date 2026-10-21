package serialization

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

func ByName(name string) Codec {
	switch name {
	case "json":
		return JSONCodec{}
	default:
		return MsgPackCodec{}
	}
}
