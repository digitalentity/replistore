package config_test

import (
	"os"
	"testing"

	"github.com/digitalentity/replistore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	content := `
mount:
  path: "/tmp/test"
replication:
  factor: 2
backends:
  - name: "b1"
    address: "1.2.3.4"
`
	tmpFile, err := os.CreateTemp(t.TempDir(), "config.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	cfg, err := config.LoadConfig(tmpFile.Name())
	require.NoError(t, err)
	assert.Equal(t, "/tmp/test", cfg.Mount.Path)
	assert.Equal(t, 2, cfg.Replication.Factor)
	assert.Len(t, cfg.Backends, 1)
	assert.Equal(t, "b1", cfg.Backends[0].Name)
}

func TestLoadConfig_Defaults(t *testing.T) {
	content := `
mount:
  path: "/tmp/test"
`
	tmpFile, err := os.CreateTemp(t.TempDir(), "config.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	cfg, err := config.LoadConfig(tmpFile.Name())
	require.NoError(t, err)
	assert.Equal(t, 2, cfg.Replication.Factor) // Default
	assert.Equal(t, "1s", cfg.Mount.AttrValid) // Default
	assert.Nil(t, cfg.Mount.UID)
	assert.Nil(t, cfg.Mount.GID)
}

func TestLoadConfig_MountAttributes(t *testing.T) {
	content := `
mount:
  path: "/tmp/test"
  attr_valid: "5s"
  uid: 1001
  gid: 1002
`
	tmpFile, err := os.CreateTemp(t.TempDir(), "config.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	cfg, err := config.LoadConfig(tmpFile.Name())
	require.NoError(t, err)
	assert.Equal(t, "5s", cfg.Mount.AttrValid)
	require.NotNil(t, cfg.Mount.UID)
	assert.Equal(t, uint32(1001), *cfg.Mount.UID)
	require.NotNil(t, cfg.Mount.GID)
	assert.Equal(t, uint32(1002), *cfg.Mount.GID)
}

func TestLoadConfig_ExpectedClusterSize(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()
		tmpFile, err := os.CreateTemp(t.TempDir(), "config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(content)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		return tmpFile.Name()
	}

	t.Run("listen_addr set without expected_cluster_size returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
`)
		_, err := config.LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected_cluster_size must be >= 2")
	})

	t.Run("listen_addr set with expected_cluster_size of 2 is ok", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: "192.168.1.50:7000"
  secret: "a-valid-shared-secret"
  expected_cluster_size: 2
`)
		cfg, err := config.LoadConfig(path)
		require.NoError(t, err)
		assert.Equal(t, 2, cfg.Cluster.ExpectedClusterSize)
	})

	t.Run("no listen_addr defaults expected_cluster_size to 1", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
`)
		cfg, err := config.LoadConfig(path)
		require.NoError(t, err)
		assert.Equal(t, 1, cfg.Cluster.ExpectedClusterSize)
	})
}

func TestLoadConfig_AdvertiseAddr(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()
		tmpFile, err := os.CreateTemp(t.TempDir(), "config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(content)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		return tmpFile.Name()
	}

	t.Run("listen_addr set without advertise_addr returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  expected_cluster_size: 2
`)
		_, err := config.LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "advertise_addr must be set")
	})

	t.Run("advertise_addr without port returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: "192.168.1.50"
  expected_cluster_size: 2
`)
		_, err := config.LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid host:port")
	})

	t.Run("advertise_addr with empty host returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: ":7000"
  expected_cluster_size: 2
`)
		_, err := config.LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid host:port")
	})

	t.Run("valid advertise_addr is ok", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: "192.168.1.50:7000"
  secret: "a-valid-shared-secret"
  expected_cluster_size: 2
`)
		cfg, err := config.LoadConfig(path)
		require.NoError(t, err)
		assert.Equal(t, "192.168.1.50:7000", cfg.Cluster.AdvertiseAddr)
	})

	t.Run("no listen_addr does not require advertise_addr", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
`)
		_, err := config.LoadConfig(path)
		assert.NoError(t, err)
	})
}

func TestLoadConfig_ClusterSecret(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()
		tmpFile, err := os.CreateTemp(t.TempDir(), "config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(content)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		return tmpFile.Name()
	}

	t.Run("listen_addr set without cluster_secret returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: "192.168.1.50:7000"
  expected_cluster_size: 2
`)
		_, err := config.LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cluster_secret must be set")
	})

	t.Run("cluster_secret shorter than 16 characters returns error", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: "192.168.1.50:7000"
  secret: "too-short"
  expected_cluster_size: 2
`)
		_, err := config.LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 16 characters")
	})

	t.Run("valid cluster_secret is ok", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
cluster:
  listen_addr: "0.0.0.0:7000"
  advertise_addr: "192.168.1.50:7000"
  secret: "a-valid-shared-secret"
  expected_cluster_size: 2
`)
		cfg, err := config.LoadConfig(path)
		require.NoError(t, err)
		assert.Equal(t, "a-valid-shared-secret", cfg.Cluster.Secret)
	})

	t.Run("no listen_addr does not require cluster_secret", func(t *testing.T) {
		path := writeConfig(t, `
mount:
  path: "/tmp/test"
`)
		_, err := config.LoadConfig(path)
		assert.NoError(t, err)
	})
}
