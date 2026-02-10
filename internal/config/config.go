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
	CacheRefreshInterval string          `yaml:"cache_refresh_interval"`
	Backends             []BackendConfig `yaml:"backends"`
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}

	return &cfg, nil
}
