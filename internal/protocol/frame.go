package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	FrameHeaderSize = 24 // 1+1+2+16+4
	MaxFramePayload = 65536
)

type FrameType uint8

const (
	FramePing FrameType = iota + 1
	FramePong
	FrameFindNodeRequest
	FrameFindNodeResponse
	FrameStoreRequest
	FrameStoreResponse
	FrameFindValueRequest
	FrameFindValueResponse
	FrameTunnelBuild
	FrameTunnelBuildOK
	FrameTunnelBuildFail
	FrameTunnelData
	FrameTunnelAck
	FrameTunnelClose
	FrameAppMessage
	FrameAppAck
	FrameError
)

type Frame struct {
	Version       uint8
	Type          FrameType
	Flags         uint16
	RequestID     [16]byte
	PayloadLength uint32
	Payload       []byte
}

func (f *Frame) Encode(w io.Writer) error {
	if len(f.Payload) > MaxFramePayload {
		return errors.New("payload too large")
	}
	header := make([]byte, FrameHeaderSize)
	header[0] = f.Version
	header[1] = byte(f.Type)
	binary.BigEndian.PutUint16(header[2:4], f.Flags)
	copy(header[4:20], f.RequestID[:])
	binary.BigEndian.PutUint32(header[20:24], uint32(len(f.Payload)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(f.Payload)
	return err
}

func DecodeFrameHeader(header []byte) (*Frame, error) {
	if len(header) < FrameHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	f := &Frame{
		Version: header[0],
		Type:    FrameType(header[1]),
		Flags:   binary.BigEndian.Uint16(header[2:4]),
	}
	copy(f.RequestID[:], header[4:20])
	f.PayloadLength = binary.BigEndian.Uint32(header[20:24])
	if f.PayloadLength > MaxFramePayload {
		return nil, errors.New("payload length exceeds maximum")
	}
	return f, nil
}
