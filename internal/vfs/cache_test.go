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
	assert.ElementsMatch(t, []string{"backend2"}, node.Meta.Backends)
}

func TestCache_Reconcile(t *testing.T) {
	cache := vfs.NewCache()
	meta := vfs.Metadata{Name: "file.txt", IsDir: false}

	// 1. Setup: file on b1 and b2
	cache.Upsert("dir/file.txt", meta, "b1")
	cache.Upsert("dir/file.txt", meta, "b2")

	// 2. Reconcile b1 with file MISSING
	seenOnB1 := make(map[string]bool)
	cache.Reconcile("b1", seenOnB1)

	// Result: file should still be there because b2 has it
	node, ok := cache.Get("dir/file.txt")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b2"}, node.Meta.Backends)

	// 3. Reconcile b2 with file MISSING
	seenOnB2 := make(map[string]bool)
	cache.Reconcile("b2", seenOnB2)

	// Result: file and empty directory should be pruned
	_, ok = cache.Get("dir/file.txt")
	assert.False(t, ok)
	_, ok = cache.Get("dir")
	assert.False(t, ok)
}

func TestCache_FindDegraded(t *testing.T) {
	cache := vfs.NewCache()

	// 1. File on 1 backend (RF=3) -> degraded
	cache.Upsert("degraded.txt", vfs.Metadata{Name: "degraded.txt", IsDir: false}, "b1")

	// 2. File on 3 backends (RF=3) -> healthy
	cache.Upsert("healthy.txt", vfs.Metadata{Name: "healthy.txt", IsDir: false}, "b1")
	cache.Upsert("healthy.txt", vfs.Metadata{Name: "healthy.txt", IsDir: false}, "b2")
	cache.Upsert("healthy.txt", vfs.Metadata{Name: "healthy.txt", IsDir: false}, "b3")

	// 3. Directory -> should not be reported as degraded
	cache.Upsert("dir", vfs.Metadata{Name: "dir", IsDir: true}, "b1")

	degraded := cache.FindDegraded(3)
	assert.Len(t, degraded, 1)
	assert.Equal(t, "degraded.txt", degraded[0].Meta.Name)
}

func TestCache_UpsertLatestWins(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	// 1. Initial version on b1
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 100, ModTime: now}, "b1")

	// 2. Newer version on b2
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 100, ModTime: now.Add(time.Hour)}, "b2")

	node, _ := cache.Get("file.txt")
	assert.Equal(t, []string{"b2"}, node.Meta.Backends)
	assert.Equal(t, now.Add(time.Hour), node.Meta.ModTime)

	// 3. Stale version on b3 (ignored)
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 200, ModTime: now}, "b3")
	node, _ = cache.Get("file.txt")
	assert.Equal(t, []string{"b2"}, node.Meta.Backends)

	// 4. Same version on b4 (added)
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 100, ModTime: now.Add(time.Hour)}, "b4")
	node, _ = cache.Get("file.txt")
	assert.ElementsMatch(t, []string{"b2", "b4"}, node.Meta.Backends)

	// 5. Larger size at same time wins (added)
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 150, ModTime: now.Add(time.Hour)}, "b5")
	node, _ = cache.Get("file.txt")
	assert.Equal(t, []string{"b5"}, node.Meta.Backends)
	assert.Equal(t, int64(150), node.Meta.Size)
}
