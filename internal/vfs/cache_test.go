package vfs_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCache_FetchEntry(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()
	now := time.Now().Round(time.Second)

	// Mock backends
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	b3 := &bmock.MockBackend{NameVal: "b3"}

	path := "lazy/file.txt"

	// B1 has an older version
	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{
		Name:    "file.txt",
		Size:    100,
		ModTime: now.Add(-time.Hour),
	}, nil)

	// B2 has the latest version
	b2.On("Stat", mock.Anything, path).Return(backend.FileInfo{
		Name:    "file.txt",
		Size:    200,
		ModTime: now,
	}, nil)

	// B3 also has the latest version (same time/size)
	b3.On("Stat", mock.Anything, path).Return(backend.FileInfo{
		Name:    "file.txt",
		Size:    200,
		ModTime: now,
	}, nil)

	// No sidecars and no tombstones anywhere: all replicas report Gen 0
	// (legacy) and no deletion is recorded.
	for _, b := range []*bmock.MockBackend{b1, b2, b3} {
		b.On("OpenFile", mock.Anything, vfs.SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)
		b.On("OpenFile", mock.Anything, vfs.TombstonePath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)
	}

	backends := []backend.Backend{b1, b2, b3}

	node, err := cache.FetchEntry(ctx, path, backends)
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, int64(200), node.Meta.Size)
	assert.ElementsMatch(t, []string{"b2", "b3"}, node.Meta.Backends)
	assert.False(t, node.FullyIndexed)

	// Verify it's in cache
	cachedNode, ok := cache.Get(path)
	assert.True(t, ok)
	assert.Equal(t, node, cachedNode)
}

func TestCache_FetchEntry_NotFound(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	b1 := &bmock.MockBackend{NameVal: "b1"}
	path := "missing.txt"
	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{}, os.ErrNotExist)

	node, err := cache.FetchEntry(ctx, path, []backend.Backend{b1})
	assert.Error(t, err)
	assert.Nil(t, node)
}

func TestCache_FetchEntry_AllBackendsUnavailable(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	path := "missing.txt"
	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{}, errors.New("conn reset"))
	b2.On("Stat", mock.Anything, path).Return(backend.FileInfo{}, errors.New("conn reset"))

	node, err := cache.FetchEntry(ctx, path, []backend.Backend{b1, b2})
	assert.ErrorIs(t, err, vfs.ErrUnavailable)
	assert.NotErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, node)
}

func TestCache_FetchEntry_DefinitiveNotFoundWins(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	path := "missing.txt"
	// b1 gives a definitive "not found", b2 errors transiently
	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{}, os.ErrNotExist)
	b2.On("Stat", mock.Anything, path).Return(backend.FileInfo{}, errors.New("conn reset"))

	node, err := cache.FetchEntry(ctx, path, []backend.Backend{b1, b2})
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, node)
}

func TestCache_FetchDir_Partial(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	// Create parent directory in cache but NOT fully indexed
	cache.Upsert("lazy-dir/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	dirNode, _ := cache.Get("lazy-dir")
	assert.False(t, dirNode.FullyIndexed)

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("ReadDir", mock.Anything, "lazy-dir").Return([]backend.FileInfo{
		{Name: "file1.txt", Size: 10, IsDir: false},
		{Name: "subdir", IsDir: true},
	}, nil)

	err := cache.FetchDir(ctx, "lazy-dir", []backend.Backend{b1})
	assert.NoError(t, err)

	// Verify directory is now fully indexed
	assert.True(t, dirNode.FullyIndexed)

	// Verify children are in cache
	_, ok1 := cache.Get("lazy-dir/file1.txt")
	assert.True(t, ok1)
	_, ok2 := cache.Get("lazy-dir/subdir")
	assert.True(t, ok2)
}

func TestCache_FetchDir_AllBackendsUnavailable(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	cache.Upsert("lazy-dir/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	dirNode, _ := cache.Get("lazy-dir")
	assert.False(t, dirNode.FullyIndexed)

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	b1.On("ReadDir", mock.Anything, "lazy-dir").Return([]backend.FileInfo(nil), errors.New("conn reset"))
	b2.On("ReadDir", mock.Anything, "lazy-dir").Return([]backend.FileInfo(nil), errors.New("conn reset"))

	err := cache.FetchDir(ctx, "lazy-dir", []backend.Backend{b1, b2})
	assert.ErrorIs(t, err, vfs.ErrUnavailable)

	// Directory must NOT be marked fully indexed
	assert.False(t, dirNode.FullyIndexed)
}

func TestCache_FetchDir_PartialBackendFailure(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	cache.Upsert("lazy-dir/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	dirNode, _ := cache.Get("lazy-dir")
	assert.False(t, dirNode.FullyIndexed)

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	b1.On("ReadDir", mock.Anything, "lazy-dir").Return([]backend.FileInfo{
		{Name: "file1.txt", Size: 10, IsDir: false},
	}, nil)
	b2.On("ReadDir", mock.Anything, "lazy-dir").Return([]backend.FileInfo(nil), errors.New("conn reset"))

	err := cache.FetchDir(ctx, "lazy-dir", []backend.Backend{b1, b2})
	assert.NoError(t, err)

	// One backend answered, so the directory counts as fully indexed
	assert.True(t, dirNode.FullyIndexed)
	_, ok := cache.Get("lazy-dir/file1.txt")
	assert.True(t, ok)
}

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
	cache.Reconcile("b1", seenOnB1, time.Now())

	// Result: file should still be there because b2 has it
	node, ok := cache.Get("dir/file.txt")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b2"}, node.Meta.Backends)

	// 3. Reconcile b2 with file MISSING
	seenOnB2 := make(map[string]bool)
	cache.Reconcile("b2", seenOnB2, time.Now())

	// Result: file and empty directory should be pruned
	_, ok = cache.Get("dir/file.txt")
	assert.False(t, ok)
	_, ok = cache.Get("dir")
	assert.False(t, ok)
}

func TestCache_Reconcile_OpenHandles(t *testing.T) {
	cache := vfs.NewCache()
	meta := vfs.Metadata{Name: "file.txt", IsDir: false}

	// 1. Setup: file on b1
	cache.Upsert("dir/file.txt", meta, "b1")

	// Get node and increment OpenHandles to simulate an open file handle
	node, ok := cache.Get("dir/file.txt")
	assert.True(t, ok)
	node.OpenHandles = 1

	// 2. Reconcile b1 with file MISSING
	seenOnB1 := make(map[string]bool)
	cache.Reconcile("b1", seenOnB1, time.Now())

	// Result: file should NOT be pruned because it has an active open handle,
	// even though it no longer has any backends in metadata.
	node, ok = cache.Get("dir/file.txt")
	assert.True(t, ok)
	assert.Empty(t, node.Meta.Backends)

	// Decrement OpenHandles to 0
	node.OpenHandles = 0

	// 3. Reconcile again
	cache.Reconcile("b1", seenOnB1, time.Now())

	// Result: file and empty directory should now be pruned
	_, ok = cache.Get("dir/file.txt")
	assert.False(t, ok)
	_, ok = cache.Get("dir")
	assert.False(t, ok)
}

func TestCache_Reconcile_SweepRace(t *testing.T) {
	cache := vfs.NewCache()
	meta := vfs.Metadata{Name: "file.txt", IsDir: false}

	// 1. Setup: walk starts
	walkStart := time.Now()

	// Simulate concurrent creation *after* walkStart
	time.Sleep(2 * time.Millisecond) // ensure timestamp moves forward
	cache.Upsert("dir/file.txt", meta, "b1")

	// 2. Reconcile b1 with file MISSING in seenPaths (e.g. concurrent creation missed by walk)
	seenOnB1 := make(map[string]bool)
	cache.Reconcile("b1", seenOnB1, walkStart)

	// Result: file should NOT be pruned because it was updated after walkStart
	node, ok := cache.Get("dir/file.txt")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b1"}, node.Meta.Backends)

	// 3. Reconcile with walkStart AFTER creation
	walkStartAfter := time.Now()
	cache.Reconcile("b1", seenOnB1, walkStartAfter)

	// Result: file should now be pruned because creation is older than walkStart
	_, ok = cache.Get("dir/file.txt")
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

func TestCache_UpsertDirUnionsBackends(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	// Same directory on two backends with DIFFERENT mtimes: directories are
	// presence-sets, so both backends must be kept.
	cache.Upsert("shared", vfs.Metadata{Name: "shared", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, "b1")
	cache.Upsert("shared", vfs.Metadata{Name: "shared", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now.Add(-time.Hour)}, "b2")

	node, ok := cache.Get("shared")
	assert.True(t, ok)
	assert.True(t, node.Meta.IsDir)
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	// Newest mtime is kept for display.
	assert.Equal(t, now, node.Meta.ModTime)

	// A newer mtime must also not displace existing backends.
	cache.Upsert("shared", vfs.Metadata{Name: "shared", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now.Add(time.Hour)}, "b3")
	node, _ = cache.Get("shared")
	assert.ElementsMatch(t, []string{"b1", "b2", "b3"}, node.Meta.Backends)
	assert.Equal(t, now.Add(time.Hour), node.Meta.ModTime)
}

func TestCache_UpsertMultiDirUnionsBackends(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	cache.Upsert("shared", vfs.Metadata{Name: "shared", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, "b1")
	// Older mtime via UpsertMulti must still union, not be dropped as stale.
	cache.UpsertMulti("shared", vfs.Metadata{Name: "shared", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now.Add(-time.Hour)}, []string{"b2", "b3"})

	node, ok := cache.Get("shared")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b1", "b2", "b3"}, node.Meta.Backends)
	assert.Equal(t, now, node.Meta.ModTime)
}

func TestCache_FetchEntryDirUnionsBackends(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()
	now := time.Now().Round(time.Second)

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	path := "some/dir"
	// Directory mtimes differ per backend; both must be listed.
	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{
		Name:    "dir",
		IsDir:   true,
		Mode:    os.ModeDir | 0755,
		ModTime: now,
	}, nil)
	b2.On("Stat", mock.Anything, path).Return(backend.FileInfo{
		Name:    "dir",
		IsDir:   true,
		Mode:    os.ModeDir | 0755,
		ModTime: now.Add(-time.Hour),
	}, nil)

	b1.On("OpenFile", mock.Anything, vfs.SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist).Maybe()
	b1.On("OpenFile", mock.Anything, vfs.TombstonePath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist).Maybe()
	b2.On("OpenFile", mock.Anything, vfs.SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist).Maybe()
	b2.On("OpenFile", mock.Anything, vfs.TombstonePath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist).Maybe()

	node, err := cache.FetchEntry(ctx, path, []backend.Backend{b1, b2})
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.True(t, node.Meta.IsDir)
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
}

func TestCache_UpsertFileDirTypeConflict(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	// File on b1, directory on b2 at the same path: the directory wins and
	// the file's backend stays in the presence set.
	cache.Upsert("conflict", vfs.Metadata{Name: "conflict", Size: 10, Mode: 0644, ModTime: now}, "b1")
	cache.Upsert("conflict", vfs.Metadata{Name: "conflict", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now.Add(-time.Hour)}, "b2")

	node, ok := cache.Get("conflict")
	assert.True(t, ok)
	assert.True(t, node.Meta.IsDir)
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)

	// The reverse order: a later file upsert cannot demote a directory.
	cache.Upsert("conflict", vfs.Metadata{Name: "conflict", Size: 10, Mode: 0644, ModTime: now.Add(time.Hour)}, "b3")
	node, _ = cache.Get("conflict")
	assert.True(t, node.Meta.IsDir)
	assert.ElementsMatch(t, []string{"b1", "b2", "b3"}, node.Meta.Backends)
}

func TestCache_UpsertLatestWins(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now()

	// 1. Initial version on b1
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 100, ModTime: now}, "b1")

	// 2. Same size, newer mtime on b2: under gen-aware merging (both Gen 0)
	// equal size means same version — backends union, newest mtime kept.
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 100, ModTime: now.Add(time.Hour)}, "b2")

	node, _ := cache.Get("file.txt")
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	assert.Equal(t, now.Add(time.Hour), node.Meta.ModTime)

	// 3. Different size with stale mtime on b3 (ignored)
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 200, ModTime: now}, "b3")
	node, _ = cache.Get("file.txt")
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)

	// 4. Same version on b4 (added)
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 100, ModTime: now.Add(time.Hour)}, "b4")
	node, _ = cache.Get("file.txt")
	assert.ElementsMatch(t, []string{"b1", "b2", "b4"}, node.Meta.Backends)

	// 5. Larger size at same time wins (added)
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Size: 150, ModTime: now.Add(time.Hour)}, "b5")
	node, _ = cache.Get("file.txt")
	assert.Equal(t, []string{"b5"}, node.Meta.Backends)
	assert.Equal(t, int64(150), node.Meta.Size)
}

func TestCache_Warmup_SkipsReservedPaths(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("Walk", mock.Anything, "", mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(path string, info backend.FileInfo) error)
		now := time.Now()
		assert.NoError(t, fn("data.txt", backend.FileInfo{Name: "data.txt", Size: 10, ModTime: now}))
		assert.NoError(t, fn(".replistore", backend.FileInfo{Name: ".replistore", IsDir: true, ModTime: now}))
		assert.NoError(t, fn(".replistore/peers", backend.FileInfo{Name: "peers", IsDir: true, ModTime: now}))
		assert.NoError(t, fn(".replistore/peers/x.json", backend.FileInfo{Name: "x.json", Size: 42, ModTime: now}))
	}).Return(nil)

	cache.Warmup(ctx, []backend.Backend{b1})

	_, ok := cache.Get("data.txt")
	assert.True(t, ok, "regular file should be indexed")

	for _, p := range []string{".replistore", ".replistore/peers", ".replistore/peers/x.json"} {
		_, ok := cache.Get(p)
		assert.False(t, ok, "reserved path %s must not be in cache", p)
	}
	b1.AssertExpectations(t)
}

func TestCache_FetchEntry_ReservedPath(t *testing.T) {
	ctx := context.Background()
	cache := vfs.NewCache()

	// No Stat expectation: the backend must never be contacted.
	b1 := &bmock.MockBackend{NameVal: "b1"}

	node, err := cache.FetchEntry(ctx, ".replistore/peers/x.json", []backend.Backend{b1})
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, node)

	node, err = cache.FetchEntry(ctx, ".replistore", []backend.Backend{b1})
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, node)

	b1.AssertNotCalled(t, "Stat", mock.Anything, mock.Anything)
}

func TestIsReservedPath(t *testing.T) {
	assert.True(t, vfs.IsReservedPath(".replistore"))
	assert.True(t, vfs.IsReservedPath(".replistore/peers"))
	assert.True(t, vfs.IsReservedPath(".replistore/peers/x.json"))
	assert.False(t, vfs.IsReservedPath(""))
	assert.False(t, vfs.IsReservedPath(".replistore2"))
	assert.False(t, vfs.IsReservedPath("dir/.replistore"))
	assert.False(t, vfs.IsReservedPath("data.txt"))
}

func TestCache_SaveAndLoad(t *testing.T) {
	cache := vfs.NewCache()
	now := time.Now().Round(time.Second)
	cache.Upsert("dir1/file1.txt", vfs.Metadata{
		Name:    "file1.txt",
		Size:    100,
		ModTime: now,
	}, "b1")
	cache.LastReconciled = now.Add(-5 * time.Minute)

	tempDir := t.TempDir()
	cachePath := tempDir + "/cache.json"

	err := cache.SaveToFile(cachePath)
	assert.NoError(t, err)

	loadedCache := vfs.NewCache()
	err = loadedCache.LoadFromFile(cachePath)
	assert.NoError(t, err)

	origNode, origOk := cache.Get("dir1/file1.txt")
	assert.True(t, origOk)
	loadedNode, loadedOk := loadedCache.Get("dir1/file1.txt")
	assert.True(t, loadedOk)

	assert.Equal(t, origNode.Meta.Name, loadedNode.Meta.Name)
	assert.Equal(t, origNode.Meta.Size, loadedNode.Meta.Size)
	assert.Equal(t, origNode.Meta.Backends, loadedNode.Meta.Backends)
	assert.Equal(t, cache.LastReconciled, loadedCache.LastReconciled)
}
