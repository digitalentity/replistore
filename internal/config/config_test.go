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

func TestLoadConfig_ExpectedClusterSize(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()
		tmpFile, err := os.CreateTemp("", "config.yaml")
		assert.NoError(t, err)
		t.Cleanup(func() { os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(content)
		assert.NoError(t, err)
		tmpFile.Close()
		return tmpFile.Name()
	}

	t.Run("listen_addr set without expected_cluster_size returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount_point: "/tmp/test"
listen_addr: "0.0.0.0:7000"
`)
		_, err := config.LoadConfig(path)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected_cluster_size must be >= 2")
	})

	t.Run("listen_addr set with expected_cluster_size of 2 is ok", func(t *testing.T) {
		path := writeConfig(t, `
mount_point: "/tmp/test"
listen_addr: "0.0.0.0:7000"
expected_cluster_size: 2
`)
		cfg, err := config.LoadConfig(path)
		assert.NoError(t, err)
		assert.Equal(t, 2, cfg.ExpectedClusterSize)
	})

	t.Run("no listen_addr defaults expected_cluster_size to 1", func(t *testing.T) {
		path := writeConfig(t, `
mount_point: "/tmp/test"
`)
		cfg, err := config.LoadConfig(path)
		assert.NoError(t, err)
		assert.Equal(t, 1, cfg.ExpectedClusterSize)
	})
}
