// Package config handles loading and validation of the replistore configuration.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
)

type BackendConfig struct {
	Name     string                 `yaml:"name"`
	Type     string                 `yaml:"type"` // e.g. "smb", "local"
	Address  string                 `yaml:"address"`
	Share    string                 `yaml:"share"`
	User     string                 `yaml:"user"`
	Password string                 `yaml:"password"`
	Domain   string                 `yaml:"domain"`
	Path     string                 `yaml:"path"` // For local filesystem backend
	Speed    int                    `yaml:"speed"`
	Tags     []string               `yaml:"tags"`
	Options  map[string]interface{} `yaml:"options"`
}

func (bc *BackendConfig) ToOptions() map[string]interface{} {
	opts := map[string]interface{}{}
	if bc.Address != "" {
		opts["address"] = bc.Address
	}
	if bc.Share != "" {
		opts["share"] = bc.Share
	}
	if bc.User != "" {
		opts["user"] = bc.User
	}
	if bc.Password != "" {
		opts["password"] = bc.Password
	}
	if bc.Domain != "" {
		opts["domain"] = bc.Domain
	}
	if bc.Path != "" {
		opts["path"] = bc.Path
	}
	opts["speed"] = bc.Speed
	if bc.Tags != nil {
		opts["tags"] = bc.Tags
	}
	for k, v := range bc.Options {
		opts[k] = v
	}
	return opts
}

type SelectorConfig struct {
	Type          string   `yaml:"type"`
	WriteAffinity []string `yaml:"write_affinity"`
}

type Config struct {
	MountPoint           string          `yaml:"mount_point"`
	StateDir             string          `yaml:"state_dir"`
	ReplicationFactor    int             `yaml:"replication_factor"`
	WriteQuorum          int             `yaml:"write_quorum"`
	CacheRefreshInterval string          `yaml:"cache_refresh_interval"`
	RepairInterval       string          `yaml:"repair_interval"`
	RepairConcurrency    int             `yaml:"repair_concurrency"`
	ListenAddr           string          `yaml:"listen_addr"`
	AdvertiseAddr        string          `yaml:"advertise_addr"`
	ClusterSecret        string          `yaml:"cluster_secret"`
	ExpectedClusterSize  int             `yaml:"expected_cluster_size"`
	MaxIODuration        string          `yaml:"max_io_duration"`
	Selector             SelectorConfig  `yaml:"selector"`
	Backends             []BackendConfig `yaml:"backends"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expandedData := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expandedData), &cfg); err != nil {
		return nil, err
	}

	if cfg.Selector.Type == "" {
		cfg.Selector.Type = "random"
	}

	for i := range cfg.Backends {
		if cfg.Backends[i].Type == "" {
			cfg.Backends[i].Type = "smb"
		}
		if cfg.Backends[i].Speed <= 0 {
			cfg.Backends[i].Speed = 10
		}
	}

	if cfg.ListenAddr != "" {
		if cfg.ExpectedClusterSize < 2 {
			return nil, errors.New("expected_cluster_size must be >= 2 when listen_addr is set: distributed locking requires a known cluster size for quorum")
		}
		if cfg.AdvertiseAddr == "" {
			return nil, errors.New("advertise_addr must be set when listen_addr is set: peers need a reachable host:port address for this node")
		}
		host, _, err := net.SplitHostPort(cfg.AdvertiseAddr)
		if err != nil || host == "" {
			return nil, fmt.Errorf("advertise_addr %q is not a valid host:port address", cfg.AdvertiseAddr)
		}
		if cfg.ClusterSecret == "" {
			return nil, errors.New("cluster_secret must be set when listen_addr is set: lock datagrams between nodes are authenticated with HMAC-SHA256 using this shared secret")
		}
		if len(cfg.ClusterSecret) < 16 {
			return nil, fmt.Errorf("cluster_secret must be at least 16 characters long (got %d)", len(cfg.ClusterSecret))
		}
	} else if cfg.ExpectedClusterSize <= 0 {
		// Single node, no clustering: the value is unused.
		cfg.ExpectedClusterSize = 1
	}

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}

	if cfg.WriteQuorum <= 0 || cfg.WriteQuorum > cfg.ReplicationFactor {
		cfg.WriteQuorum = cfg.ReplicationFactor
	}

	if cfg.RepairConcurrency <= 0 {
		cfg.RepairConcurrency = 2
	}

	if cfg.RepairInterval == "" {
		cfg.RepairInterval = "1h"
	}

	if cfg.MaxIODuration == "" {
		cfg.MaxIODuration = "1s"
	}

	if cfg.StateDir == "" {
		cfg.StateDir = "state"
	}

	return &cfg, nil
}
