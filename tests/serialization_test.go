package tests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

type sampleRecord struct {
	NodeID    string   `json:"node_id" msgpack:"node_id"`
	Addresses []string `json:"addresses" msgpack:"addresses"`
	IssuedAt  int64    `json:"issued_at" msgpack:"issued_at"`
	Signature []byte   `json:"signature" msgpack:"signature"`
}

func TestSerializationComparison(t *testing.T) {
	rec := sampleRecord{
		NodeID:    "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		Addresses: []string{"10.0.0.1:9000", "10.0.0.2:9000"},
		IssuedAt:  1700000000,
		Signature: make([]byte, 64),
	}
	jb, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	mb, err := msgpack.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("JSON:    %d bytes", len(jb))
	t.Logf("MsgPack: %d bytes", len(mb))
	t.Logf("Ratio (msgpack/json): %.3f", float64(len(mb))/float64(len(jb)))

	outDir := filepath.Join("..", "results")
	if err := os.MkdirAll(outDir, 0o755); err == nil {
		content := "format,bytes,ratio_vs_json\n" +
			"json," + itoa(len(jb)) + ",1.000\n" +
			"msgpack," + itoa(len(mb)) + "," + ftoa(float64(len(mb))/float64(len(jb))) + "\n"
		_ = os.WriteFile(filepath.Join(outDir, "serialization.csv"), []byte(content), 0o644)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func ftoa(f float64) string {
	whole := int(f * 1000)
	s := itoa(whole)
	for len(s) < 4 {
		s = "0" + s
	}
	return s[:len(s)-3] + "." + s[len(s)-3:]
}
