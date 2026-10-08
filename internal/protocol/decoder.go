package protocol

import (
	"bufio"
	"errors"
	"io"
)

type StreamDecoder struct {
	r *bufio.Reader
}

func NewStreamDecoder(r io.Reader) *StreamDecoder {
	return &StreamDecoder{r: bufio.NewReader(r)}
}

func (d *StreamDecoder) ReadFrame() (*Frame, error) {
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(d.r, header); err != nil {
		return nil, err
	}
	f, err := DecodeFrameHeader(header)
	if err != nil {
		return nil, err
	}
	if f.PayloadLength > 0 {
		f.Payload = make([]byte, f.PayloadLength)
		if _, err := io.ReadFull(d.r, f.Payload); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (d *StreamDecoder) ReadAllFrames() ([]*Frame, error) {
	var frames []*Frame
	for {
		f, err := d.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return frames, err
		}
		frames = append(frames, f)
	}
	return frames, nil
}
