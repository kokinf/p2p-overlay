package config

import (
	"fmt"
	"os"
	"p2p-overlay/internal/nodeid"
	"time"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	AdvertiseAddress string        `yaml:"advertise_address"`
	ID               string        `yaml:"id"`
	Port             int           `yaml:"port"`
	StateDir         string        `yaml:"state_dir"`
	ReadTimeout      time.Duration `yaml:"read_timeout"`
	WriteTimeout     time.Duration `yaml:"write_timeout"`
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
	RelayStrategy     string        `yaml:"relay_strategy"` // random или rating
}

type TunnelConfig struct {
	MaxPerSubnet int           `yaml:"max_per_subnet"`
	Relays       int           `yaml:"relays"`
	PoolSize     int           `yaml:"pool_size"`
	TTL          time.Duration `yaml:"ttl"`
}

type BootstrapConfig struct {
	Pins          map[string]string `yaml:"pins"`
	AllowUnpinned bool              `yaml:"allow_unpinned"`
	Scheme        string            `yaml:"scheme"`
	Peers         []string          `yaml:"peers"`
}

type ExperimentConfig struct {
	Repeats   int           `yaml:"repeats"`
	Interval  time.Duration `yaml:"interval"`
	Message   string        `yaml:"message"`
	Enabled   bool          `yaml:"enabled"`
	Delay     time.Duration `yaml:"delay"`
	Targets   []string      `yaml:"targets"`
	OutputDir string        `yaml:"output_dir"`
}

type FileConfig struct {
	Enabled     *bool  `yaml:"enabled"`
	DownloadDir string `yaml:"download_dir"`
}

func (f FileConfig) IsEnabled() bool { return f.Enabled == nil || *f.Enabled }
func (f FileConfig) Directory() string {
	if f.DownloadDir == "" {
		return "downloads"
	}
	return f.DownloadDir
}

type AdmissionConfig struct {
	Mode    string   `yaml:"mode"`
	NodeIDs []string `yaml:"node_ids"`
}

type Config struct {
	Admission  AdmissionConfig  `yaml:"admission"`
	Files      FileConfig       `yaml:"files"`
	Tunnel     TunnelConfig     `yaml:"tunnel"`
	Node       NodeConfig       `yaml:"node"`
	Protocol   ProtocolConfig   `yaml:"protocol"`
	DHT        DHTConfig        `yaml:"dht"`
	Bootstrap  BootstrapConfig  `yaml:"bootstrap"`
	Experiment ExperimentConfig `yaml:"experiment"`
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

func (c *Config) Validate() error {
	if c.Admission.Mode != "" && c.Admission.Mode != "open" && c.Admission.Mode != "allowlist" {
		return fmt.Errorf("admission mode must be open or allowlist")
	}
	if c.Admission.Mode == "allowlist" && len(c.Admission.NodeIDs) == 0 {
		return fmt.Errorf("empty admission allowlist")
	}
	for _, id := range c.Admission.NodeIDs {
		if _, err := nodeid.ParseHex(id); err != nil {
			return fmt.Errorf("invalid admission NodeID: %w", err)
		}
	}
	for addr, id := range c.Bootstrap.Pins {
		if _, err := nodeid.ParseHex(id); err != nil {
			return fmt.Errorf("invalid bootstrap pin %s: %w", addr, err)
		}
	}
	for _, addr := range c.Bootstrap.Peers {
		if _, ok := c.Bootstrap.Pins[addr]; !ok && !c.Bootstrap.AllowUnpinned {
			return fmt.Errorf("bootstrap %s requires a NodeID pin; allow_unpinned is only for explicitly untrusted discovery", addr)
		}
	}
	if c.Tunnel.MaxPerSubnet < 0 || c.Tunnel.MaxPerSubnet > 4 {
		return fmt.Errorf("max_per_subnet must be 0..4")
	}
	if c.Experiment.Repeats < 0 || c.Experiment.Repeats > 1000 {
		return fmt.Errorf("experiment repeats must be 0..1000")
	}
	if c.DHT.RelayStrategy != "" && c.DHT.RelayStrategy != "random" && c.DHT.RelayStrategy != "rating" {
		return fmt.Errorf("invalid relay strategy")
	}
	if c.Protocol.Version != 0 && c.Protocol.Version != 1 {
		return fmt.Errorf("unsupported protocol version %d", c.Protocol.Version)
	}
	if c.Protocol.MaxFramePayload < 0 || c.Protocol.MaxFramePayload > 65536 {
		return fmt.Errorf("max_frame_payload must be <=65536")
	}
	if c.DHT.Alpha != 0 && c.DHT.Alpha != 3 {
		return fmt.Errorf("ALPHA must be 3")
	}
	if c.DHT.Replication != 0 && c.DHT.Replication != 3 {
		return fmt.Errorf("replication must be 3")
	}
	if c.DHT.K < 1 || c.DHT.K > 256 {
		return fmt.Errorf("invalid bucket capacity")
	}
	if c.Tunnel.Relays != 0 && (c.Tunnel.Relays < 2 || c.Tunnel.Relays > 4) {
		return fmt.Errorf("relay count must be 2..4")
	}
	if c.Tunnel.PoolSize < 0 || c.Tunnel.PoolSize > 16 {
		return fmt.Errorf("pool size must be 1..16")
	}
	return nil
}
