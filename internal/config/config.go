package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type BackendConfig struct {
	Name     string `yaml:"name"`
	Address  string `yaml:"address"`
	Share    string `yaml:"share"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Domain   string `yaml:"domain"`
}

type Config struct {
	MountPoint           string          `yaml:"mount_point"`
	ReplicationFactor    int             `yaml:"replication_factor"`
	WriteQuorum          int             `yaml:"write_quorum"`
	CacheRefreshInterval string          `yaml:"cache_refresh_interval"`
	RepairInterval       string          `yaml:"repair_interval"`
	RepairConcurrency    int             `yaml:"repair_concurrency"`
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

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}

	if cfg.WriteQuorum <= 0 || cfg.WriteQuorum > cfg.ReplicationFactor {
		cfg.WriteQuorum = cfg.ReplicationFactor
	}

	if cfg.RepairConcurrency <= 0 {
		cfg.RepairConcurrency = 2
	}

	return &cfg, nil
}
