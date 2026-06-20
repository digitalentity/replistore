// Package config handles loading and validation of the replistore configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

const (
	minClusterSize           = 2
	minClusterSecretLength   = 16
	defaultRepairConcurrency = 2
	defaultReplicationFactor = 2
)

var validate = validator.New()

func init() {
	_ = validate.RegisterValidation("duration", validateDuration)
	validate.RegisterStructValidation(configStructValidation, Config{})
}

func validateDuration(fl validator.FieldLevel) bool {
	val := fl.Field().String()
	if val == "" {
		return true
	}
	_, err := time.ParseDuration(val)

	return err == nil
}

type BackendConfig struct {
	Name     string         `mapstructure:"name"     validate:"required" yaml:"name"`
	Type     string         `mapstructure:"type"     yaml:"type"` // e.g. "smb", "local"
	Address  string         `mapstructure:"address"  yaml:"address"`
	Share    string         `mapstructure:"share"    yaml:"share"`
	User     string         `mapstructure:"user"     yaml:"user"`
	Password string         `mapstructure:"password" yaml:"password"`
	Domain   string         `mapstructure:"domain"   yaml:"domain"`
	Path     string         `mapstructure:"path"     yaml:"path"` // For local filesystem backend
	Speed    int            `mapstructure:"speed"    yaml:"speed"`
	Tags     []string       `mapstructure:"tags"     yaml:"tags"`
	Options  map[string]any `mapstructure:"options"  yaml:"options"`
}

func (bc *BackendConfig) ToOptions() map[string]any {
	opts := map[string]any{}
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
	maps.Copy(opts, bc.Options)

	return opts
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"  yaml:"level"`
	Format string `mapstructure:"format" yaml:"format"`
}

type SelectorConfig struct {
	Type          string   `mapstructure:"type"           yaml:"type"`
	WriteAffinity []string `mapstructure:"write_affinity" yaml:"write_affinity"`
}

type CacheConfig struct {
	RefreshInterval string `mapstructure:"refresh_interval" validate:"omitempty,duration" yaml:"refresh_interval"`
	StateDir        string `mapstructure:"state_dir"        yaml:"state_dir"`
}

type RepairConfig struct {
	Interval    string `mapstructure:"interval"    validate:"omitempty,duration" yaml:"interval"`
	Grace       string `mapstructure:"grace"       validate:"omitempty,duration" yaml:"grace"`
	Concurrency int    `mapstructure:"concurrency" yaml:"concurrency"`
}

type ClusterConfig struct {
	ListenAddr          string `mapstructure:"listen_addr"           yaml:"listen_addr"`
	AdvertiseAddr       string `mapstructure:"advertise_addr"        yaml:"advertise_addr"`
	Secret              string `mapstructure:"secret"                yaml:"secret"`
	ExpectedClusterSize int    `mapstructure:"expected_cluster_size" yaml:"expected_cluster_size"`
}

type APIConfig struct {
	Addr         string `mapstructure:"addr"          yaml:"addr"`
	APIToken     string `mapstructure:"api_token"     yaml:"api_token"`
	MetricsToken string `mapstructure:"metrics_token" yaml:"metrics_token"`
}

type ReplicationConfig struct {
	Factor      int `mapstructure:"factor"       yaml:"factor"`
	WriteQuorum int `mapstructure:"write_quorum" yaml:"write_quorum"`
}

type MountConfig struct {
	Path    string `mapstructure:"path"    validate:"required" yaml:"path"`
	Options string `mapstructure:"options" yaml:"options"`
}

type Config struct {
	Mount            MountConfig       `mapstructure:"mount"              yaml:"mount"`
	Replication      ReplicationConfig `mapstructure:"replication"        yaml:"replication"`
	WriteLeaseBuffer string            `mapstructure:"write_lease_buffer" validate:"omitempty,duration" yaml:"write_lease_buffer"`
	Cache            CacheConfig       `mapstructure:"cache"              yaml:"cache"`
	Repair           RepairConfig      `mapstructure:"repair"             yaml:"repair"`
	Cluster          ClusterConfig     `mapstructure:"cluster"            yaml:"cluster"`
	API              APIConfig         `mapstructure:"api"                yaml:"api"`
	Selector         SelectorConfig    `mapstructure:"selector"           yaml:"selector"`
	Logging          LoggingConfig     `mapstructure:"logging"            yaml:"logging"`
	Backends         []BackendConfig   `mapstructure:"backends"           validate:"dive"               yaml:"backends"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: configuration path supplied by caller
	if err != nil {
		return nil, err
	}

	expandedData := os.ExpandEnv(string(data))

	v := viper.New()
	v.SetConfigType("yaml")

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "text")
	v.SetDefault("selector.type", "random")
	v.SetDefault("replication.factor", defaultReplicationFactor)
	v.SetDefault("replication.write_quorum", 0)
	v.SetDefault("repair.concurrency", defaultRepairConcurrency)
	v.SetDefault("cache.refresh_interval", "5m")
	v.SetDefault("repair.interval", "1h")
	v.SetDefault("repair.grace", "4h")
	v.SetDefault("write_lease_buffer", "2s")
	v.SetDefault("cache.state_dir", "/var/lib/replistore")

	if err := v.ReadConfig(bytes.NewBufferString(expandedData)); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// Post-processing defaults
	for i := range cfg.Backends {
		if cfg.Backends[i].Type == "" {
			cfg.Backends[i].Type = "smb"
		}
		if cfg.Backends[i].Speed <= 0 {
			cfg.Backends[i].Speed = 10
		}
	}

	if cfg.Cluster.ListenAddr == "" && cfg.Cluster.ExpectedClusterSize <= 0 {
		cfg.Cluster.ExpectedClusterSize = 1
	}

	if cfg.Replication.Factor <= 0 {
		cfg.Replication.Factor = 1
	}

	if cfg.Replication.WriteQuorum <= 0 || cfg.Replication.WriteQuorum > cfg.Replication.Factor {
		cfg.Replication.WriteQuorum = cfg.Replication.Factor
	}

	// Validate config using go-playground/validator
	if err := validate.Struct(&cfg); err != nil {
		var valErrs validator.ValidationErrors
		if errors.As(err, &valErrs) {
			for _, fieldErr := range valErrs {
				switch fieldErr.Tag() {
				case "cluster_size_min":
					return nil, errors.New("expected_cluster_size must be >= 2 when listen_addr is set: distributed locking requires a known cluster size for quorum")
				case "advertise_addr_required":
					return nil, errors.New("advertise_addr must be set when listen_addr is set: peers need a reachable host:port address for this node")
				case "advertise_addr_invalid":
					return nil, fmt.Errorf("advertise_addr %q is not a valid host:port address", cfg.Cluster.AdvertiseAddr)
				case "secret_required":
					return nil, errors.New("cluster_secret must be set when listen_addr is set: lock datagrams between nodes are authenticated with HMAC-SHA256 using this shared secret")
				case "secret_min_len":
					return nil, fmt.Errorf("cluster_secret must be at least 16 characters long (got %d)", len(cfg.Cluster.Secret))
				default:
					return nil, fieldErr
				}
			}
		}

		return nil, err
	}

	return &cfg, nil
}

func configStructValidation(sl validator.StructLevel) {
	cfg := sl.Current().Interface().(Config)

	if cfg.Cluster.ListenAddr == "" {
		return
	}

	if cfg.Cluster.ExpectedClusterSize < minClusterSize {
		sl.ReportError(cfg.Cluster.ExpectedClusterSize, "expected_cluster_size", "Cluster.ExpectedClusterSize", "cluster_size_min", "")
	}

	if cfg.Cluster.AdvertiseAddr == "" {
		sl.ReportError(cfg.Cluster.AdvertiseAddr, "advertise_addr", "Cluster.AdvertiseAddr", "advertise_addr_required", "")
	} else {
		host, _, err := net.SplitHostPort(cfg.Cluster.AdvertiseAddr)
		if err != nil || host == "" {
			sl.ReportError(cfg.Cluster.AdvertiseAddr, "advertise_addr", "Cluster.AdvertiseAddr", "advertise_addr_invalid", "")
		}
	}

	if cfg.Cluster.Secret == "" {
		sl.ReportError(cfg.Cluster.Secret, "secret", "Cluster.Secret", "secret_required", "")
	} else if len(cfg.Cluster.Secret) < minClusterSecretLength {
		sl.ReportError(cfg.Cluster.Secret, "secret", "Cluster.Secret", "secret_min_len", "")
	}
}
