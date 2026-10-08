package tests

import (
	"bytes"
	"testing"

	"p2p-overlay/internal/protocol"
)

func TestPayloadTooLarge(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, protocol.FrameHeaderSize)
	header[0] = 1
	header[1] = byte(protocol.FramePing)
	header[20] = 0xFF
	header[21] = 0xFF
	header[22] = 0xFF
	header[23] = 0xFF
	buf.Write(header)

	_, err := protocol.DecodeFrameHeader(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for oversized payload")
	}
}

func TestPartialHeader(t *testing.T) {
	header := make([]byte, 10)
	_, err := protocol.DecodeFrameHeader(header)
	if err == nil {
		t.Fatal("expected error for partial header")
	}
}
