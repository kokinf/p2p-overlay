package tests

import (
	"bytes"
	"io"
	"testing"

	"p2p-overlay/internal/protocol"
)

func TestDecoderChunkedStream(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 100; i++ {
		f := &protocol.Frame{
			Version: 1,
			Type:    protocol.FrameAppMessage,
			Payload: bytes.Repeat([]byte{byte(i)}, i+1),
		}
		if err := f.Encode(&buf); err != nil {
			t.Fatal(err)
		}
	}
	raw := buf.Bytes()

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for off := 0; off < len(raw); off += 7 {
			end := off + 7
			if end > len(raw) {
				end = len(raw)
			}
			if _, err := pw.Write(raw[off:end]); err != nil {
				return
			}
		}
	}()

	dec := protocol.NewStreamDecoder(pr)
	frames, err := dec.ReadAllFrames()
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 100 {
		t.Fatalf("expected 100 frames, got %d", len(frames))
	}
	for i, f := range frames {
		if len(f.Payload) != i+1 {
			t.Fatalf("frame %d: payload len %d", i, len(f.Payload))
		}
	}
}

func TestDecoderMultipleFramesOneRead(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		f := &protocol.Frame{Version: 1, Type: protocol.FramePing, Payload: []byte("x")}
		_ = f.Encode(&buf)
	}
	dec := protocol.NewStreamDecoder(bytes.NewReader(buf.Bytes()))
	frames, err := dec.ReadAllFrames()
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
}
