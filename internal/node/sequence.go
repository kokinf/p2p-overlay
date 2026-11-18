package node

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"time"
)

func (n *Node) nextRecordSequence() (uint64, error) {
	n.sequenceMu.Lock()
	defer n.sequenceMu.Unlock()
	path := filepath.Join(n.Config.Node.StateDir, "record-sequence")
	var previous uint64
	b, err := os.ReadFile(path)
	if err == nil {
		if len(b) != 8 {
			return 0, errors.New("invalid record sequence state")
		}
		previous = binary.BigEndian.Uint64(b)
	} else if !os.IsNotExist(err) {
		return 0, err
	}
	next := uint64(time.Now().UnixNano())
	if next <= previous {
		next = previous + 1
	}
	if next == 0 {
		return 0, errors.New("record sequence exhausted")
	}
	f, err := os.CreateTemp(n.Config.Node.StateDir, ".record-sequence-")
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return 0, err
	}
	if _, err = f.Write(binary.BigEndian.AppendUint64(nil, next)); err != nil {
		f.Close()
		return 0, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	if err = f.Close(); err != nil {
		return 0, err
	}
	if err = os.Rename(tmp, path); err != nil {
		return 0, err
	}
	return next, nil
}
