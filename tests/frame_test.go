package tests

import (
	"bytes"
	"testing"

	"p2p-overlay/internal/protocol"
)

func TestFrameEncodeDecode(t *testing.T) {
	f := &protocol.Frame{
		Version:   1,
		Type:      protocol.FramePing,
		RequestID: [16]byte{1, 2, 3},
		Payload:   []byte("hello"),
	}
	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	dec := protocol.NewStreamDecoder(&buf)
	got, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != f.Type || !bytes.Equal(got.Payload, f.Payload) {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestStreamDecoderPartialReads(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 100; i++ {
		f := &protocol.Frame{Version: 1, Type: protocol.FramePing, Payload: []byte("x")}
		_ = f.Encode(&buf)
	}
	dec := protocol.NewStreamDecoder(bytes.NewReader(buf.Bytes()))
	frames, err := dec.ReadAllFrames()
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 100 {
		t.Fatalf("got %d frames", len(frames))
	}
}

type shortFrameWriter struct{ data []byte }

func (w *shortFrameWriter) Write(p []byte) (int, error) {
	n := len(p)
	if n > 3 {
		n = 3
	}
	w.data = append(w.data, p[:n]...)
	return n, nil
}
func TestFrameShortWrites(t *testing.T) {
	w := &shortFrameWriter{}
	f := &protocol.Frame{Version: 1, Type: protocol.FramePing, Payload: []byte("payload")}
	if err := f.Encode(w); err != nil {
		t.Fatal(err)
	}
	if len(w.data) != protocol.FrameHeaderSize+7 {
		t.Fatalf("truncated write: %d", len(w.data))
	}
}
