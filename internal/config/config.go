package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	Port         int           `yaml:"port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type ProtocolConfig struct {
	Version         uint8 `yaml:"version"`
	MaxFramePayload int   `yaml:"max_frame_payload"`
}

type Config struct {
	Node     NodeConfig     `yaml:"node"`
	Protocol ProtocolConfig `yaml:"protocol"`
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
