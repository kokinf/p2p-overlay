package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type SampleRecord struct {
	NodeID    string    `json:"node_id" msgpack:"node_id"`
	Addresses []string  `json:"addresses" msgpack:"addresses"`
	IssuedAt  time.Time `json:"issued_at" msgpack:"issued_at"`
	Signature []byte    `json:"signature" msgpack:"signature"`
}

func main() {
	rec := SampleRecord{
		NodeID:    "abc123",
		Addresses: []string{"10.0.0.1:9000", "10.0.0.2:9000"},
		IssuedAt:  time.Now(),
		Signature: make([]byte, 64),
	}
	jb, _ := json.Marshal(rec)
	mb, _ := msgpack.Marshal(rec)
	fmt.Printf("JSON:    %d bytes\n", len(jb))
	fmt.Printf("MsgPack: %d bytes\n", len(mb))
	fmt.Printf("Ratio:   %.2f\n", float64(len(mb))/float64(len(jb)))
}
