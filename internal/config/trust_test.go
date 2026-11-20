package config

import (
	"strings"
	"testing"
)

func TestBootstrapPinsAndAdmissionConfig(t *testing.T) {
	c := &Config{DHT: DHTConfig{K: 3}, Bootstrap: BootstrapConfig{Peers: []string{"seed:9000"}}}
	if c.Validate() == nil {
		t.Fatal("unpinned bootstrap accepted by default")
	}
	c.Bootstrap.AllowUnpinned = true
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Bootstrap.AllowUnpinned = false
	c.Bootstrap.Pins = map[string]string{"seed:9000": strings.Repeat("ab", 32)}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Admission.Mode = "allowlist"
	if c.Validate() == nil {
		t.Fatal("empty allowlist accepted")
	}
	c.Admission.NodeIDs = []string{strings.Repeat("ab", 32)}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Bootstrap.Pins["seed:9000"] = "invalid"
	if c.Validate() == nil {
		t.Fatal("invalid pin accepted")
	}
}
