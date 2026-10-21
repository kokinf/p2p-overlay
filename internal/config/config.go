package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ID           string        `yaml:"id"`
	Port         int           `yaml:"port"`
	StateDir     string        `yaml:"state_dir"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type ProtocolConfig struct {
	Version         uint8  `yaml:"version"`
	MaxFramePayload int    `yaml:"max_frame_payload"`
	Codec           string `yaml:"codec"`
}

type DHTConfig struct {
	K                 int           `yaml:"k"`
	Alpha             int           `yaml:"alpha"`
	Replication       int           `yaml:"replication"`
	TTLSeconds        int           `yaml:"ttl_seconds"`
	RefreshInterval   time.Duration `yaml:"refresh_interval"`
	RepublishInterval time.Duration `yaml:"republish_interval"`
}

type BootstrapConfig struct {
	Scheme string   `yaml:"scheme"`
	Peers  []string `yaml:"peers"`
}

type Config struct {
	Node      NodeConfig      `yaml:"node"`
	Protocol  ProtocolConfig  `yaml:"protocol"`
	DHT       DHTConfig       `yaml:"dht"`
	Bootstrap BootstrapConfig `yaml:"bootstrap"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
