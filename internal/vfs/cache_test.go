package vfs_test

import (
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
)

func TestCache_UpsertAndGet(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	meta := vfs.Metadata{
		Name:    "file.txt",
		Size:    1024,
		Mode:    0644,
		ModTime: now,
		IsDir:   false,
	}

	cache.Upsert("dir/file.txt", meta, "backend1")

	// Verify file exists
	node, ok := cache.Get("dir/file.txt")
	assert.True(t, ok)
	assert.Equal(t, "file.txt", node.Meta.Name)
	assert.Equal(t, int64(1024), node.Meta.Size)
	assert.Contains(t, node.Meta.Backends, "backend1")
	assert.Equal(t, "dir/file.txt", node.Meta.Path)

	// Verify intermediate directory
	dirNode, ok := cache.Get("dir")
	assert.True(t, ok)
	assert.True(t, dirNode.Meta.IsDir)
}

func TestCache_UpsertUpdates(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	meta1 := vfs.Metadata{
		Name:    "file.txt",
		Size:    100,
		Mode:    0644,
		ModTime: now,
		IsDir:   false,
	}
	cache.Upsert("file.txt", meta1, "backend1")

	meta2 := vfs.Metadata{
		Name:    "file.txt",
		Size:    200, // Larger size
		Mode:    0644,
		ModTime: now.Add(time.Hour), // Newer time
		IsDir:   false,
	}
	cache.Upsert("file.txt", meta2, "backend2")

	node, ok := cache.Get("file.txt")
	assert.True(t, ok)
	assert.Equal(t, int64(200), node.Meta.Size)
	assert.Equal(t, meta2.ModTime, node.Meta.ModTime)
	assert.ElementsMatch(t, []string{"backend1", "backend2"}, node.Meta.Backends)
}
