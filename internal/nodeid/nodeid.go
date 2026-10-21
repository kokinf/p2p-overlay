package nodeid

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

const Size = 32

type ID [Size]byte

func (id ID) String() string { return hex.EncodeToString(id[:]) }

func FromBytes(b []byte) (ID, error) {
	if len(b) != Size {
		return ID{}, errors.New("invalid node id length")
	}
	var id ID
	copy(id[:], b)
	return id, nil
}

func MustFromHex(s string) ID {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	id, err := FromBytes(b)
	if err != nil {
		panic(err)
	}
	return id
}

func Hash(data []byte) ID { return sha256.Sum256(data) }

func Distance(a, b ID) ID {
	var d ID
	for i := 0; i < Size; i++ {
		d[i] = a[i] ^ b[i]
	}
	return d
}

func Compare(a, b ID) int {
	for i := 0; i < Size; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func Less(a, b, target ID) bool {
	return Compare(Distance(a, target), Distance(b, target)) < 0
}

func CommonPrefixLen(a, b ID) int {
	for i := 0; i < Size; i++ {
		x := a[i] ^ b[i]
		if x == 0 {
			continue
		}
		for bit := 7; bit >= 0; bit-- {
			if x&(1<<uint(bit)) != 0 {
				return i*8 + (7 - bit)
			}
		}
	}
	return Size * 8
}
