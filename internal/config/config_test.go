package config_test

import (
	"os"
	"testing"

	"github.com/digitalentity/replistore/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	content := `
mount_point: "/tmp/test"
replication_factor: 2
backends:
  - name: "b1"
    address: "1.2.3.4"
`
	tmpFile, err := os.CreateTemp("", "config.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	cfg, err := config.LoadConfig(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, "/tmp/test", cfg.MountPoint)
	assert.Equal(t, 2, cfg.ReplicationFactor)
	assert.Len(t, cfg.Backends, 1)
	assert.Equal(t, "b1", cfg.Backends[0].Name)
}

func TestLoadConfig_Defaults(t *testing.T) {
	content := `
mount_point: "/tmp/test"
`
	tmpFile, err := os.CreateTemp("", "config.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	cfg, err := config.LoadConfig(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, 1, cfg.ReplicationFactor) // Default
}
