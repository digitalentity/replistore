package fuse

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/digitalentity/replistore/internal/backend"
	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDir_Rename_Simple(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("old.txt", vfs.Metadata{Name: "old.txt", Path: "old.txt", Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "old.txt", "new.txt").Return(nil)
	// The old path is tombstoned and the new path gets a fresh sidecar.
	// The target path carries no tombstone.
	expectTombstoneWrite(b1, "old.txt")
	expectNoTombstone(b1, "new.txt")
	expectSidecarWrite(b1, "new.txt")

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := root.Rename(context.Background(), req, root)

	require.NoError(t, err)

	// Check cache
	_, ok := cache.Get("old.txt")
	assert.False(t, ok)
	node, ok := cache.Get("new.txt")
	assert.True(t, ok)
	assert.Equal(t, "new.txt", node.Meta.Name)
	assert.Equal(t, "new.txt", node.Meta.Path)

	b1.AssertExpectations(t)
}

func TestDir_Rename_OverExistingTarget(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("old.txt", vfs.Metadata{Name: "old.txt", Path: "old.txt", Backends: []string{"b1"}}, "b1")
	cache.Upsert("new.txt", vfs.Metadata{Name: "new.txt", Path: "new.txt", Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	// Replacing the target durably tombstones and removes it before the source
	// rename runs, so the target's replica is not leaked.
	expectTombstoneWrite(b1, "new.txt")
	b1.On("Remove", mock.Anything, "new.txt").Return(nil)

	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "old.txt", "new.txt").Return(nil)
	expectTombstoneWrite(b1, "old.txt")
	expectNoTombstone(b1, "new.txt")
	expectSidecarWrite(b1, "new.txt")

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := root.Rename(context.Background(), req, root)
	require.NoError(t, err)

	_, ok := cache.Get("old.txt")
	assert.False(t, ok)
	node, ok := cache.Get("new.txt")
	assert.True(t, ok)
	assert.Equal(t, "new.txt", node.Meta.Name)

	b1.AssertExpectations(t)
	// The target's replica must have been removed before the rename.
	b1.AssertCalled(t, "Remove", mock.Anything, "new.txt")
}

func TestDir_Rename_OverDirectoryTargetRejected(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("file.txt", vfs.Metadata{Name: "file.txt", Path: "file.txt", Backends: []string{"b1"}}, "b1")
	cache.Upsert("dir", vfs.Metadata{Name: "dir", Path: "dir", IsDir: true, Mode: os.ModeDir | 0755, Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	// Renaming a file onto an existing directory must fail (EISDIR) and touch
	// no backend data.
	req := &fuse.RenameRequest{OldName: "file.txt", NewName: "dir"}
	err := root.Rename(context.Background(), req, root)
	require.ErrorIs(t, err, syscall.EISDIR)

	_, ok := cache.Get("file.txt")
	assert.True(t, ok, "source must be untouched after a rejected rename")
	b1.AssertNotCalled(t, "Rename", mock.Anything, mock.Anything, mock.Anything)
	b1.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
}

func TestDir_Rename_CrossDir(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("dir1/old.txt", vfs.Metadata{Name: "old.txt", Path: "dir1/old.txt", Backends: []string{"b1"}}, "b1")
	cache.Upsert("dir2", vfs.Metadata{Name: "dir2", Path: "dir2", IsDir: true, Mode: os.ModeDir | 0755, Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	d1Node, _ := root.Lookup(context.Background(), "dir1")
	d1 := d1Node.(*Dir)
	d2Node, _ := root.Lookup(context.Background(), "dir2")
	d2 := d2Node.(*Dir)

	b1.On("MkdirAll", mock.Anything, "dir2", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "dir1/old.txt", "dir2/new.txt").Return(nil)
	expectTombstoneWrite(b1, "dir1/old.txt")
	expectNoTombstone(b1, "dir2/new.txt")
	expectSidecarWrite(b1, "dir2/new.txt")

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := d1.Rename(context.Background(), req, d2)

	require.NoError(t, err)

	// Check cache
	_, ok := cache.Get("dir1/old.txt")
	assert.False(t, ok)
	node, ok := cache.Get("dir2/new.txt")
	assert.True(t, ok)
	assert.Equal(t, "new.txt", node.Meta.Name)
	assert.Equal(t, "dir2/new.txt", node.Meta.Path)

	b1.AssertExpectations(t)
}

func TestDir_Rename_DirFansOutToAllBackends(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	// Directory listed on b1 only in the cache, but rename must fan out to
	// all configured backends.
	cache.Upsert("olddir", vfs.Metadata{Name: "olddir", Path: "olddir", IsDir: true, Mode: os.ModeDir | 0755}, "b1")
	node, _ := cache.Get("olddir")
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "olddir", "newdir").Return(nil)
	b1.On("Walk", mock.Anything, "olddir", mock.Anything).Return(nil).Maybe()
	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b2.On("Rename", mock.Anything, "olddir", "newdir").Return(nil)
	b2.On("Walk", mock.Anything, "olddir", mock.Anything).Return(nil).Maybe()

	expectNoTombstone(b1, "newdir")
	expectNoTombstone(b2, "newdir")
	expectTombstoneWrite(b1, "olddir")
	expectTombstoneWrite(b2, "olddir")
	expectSidecarWrite(b1, "newdir")
	expectSidecarWrite(b2, "newdir")

	req := &fuse.RenameRequest{OldName: "olddir", NewName: "newdir"}
	err := root.Rename(context.Background(), req, root)
	require.NoError(t, err)

	_, ok := cache.Get("olddir")
	assert.False(t, ok)
	newNode, ok := cache.Get("newdir")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b1", "b2"}, newNode.Meta.Backends)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestDir_Rename_DirNotExistIsSkipped(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("olddir", vfs.Metadata{Name: "olddir", Path: "olddir", IsDir: true, Mode: os.ModeDir | 0755}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "olddir", "newdir").Return(nil)
	b1.On("Walk", mock.Anything, "olddir", mock.Anything).Return(nil).Maybe()
	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	// Source dir simply doesn't exist on b2: neither success nor failure,
	// and no async orphan removal must be triggered.
	b2.On("Rename", mock.Anything, "olddir", "newdir").Return(os.ErrNotExist)
	b2.On("Walk", mock.Anything, "olddir", mock.Anything).Return(os.ErrNotExist).Maybe()

	expectNoTombstone(b1, "newdir")
	expectNoTombstone(b2, "newdir")
	expectTombstoneWrite(b1, "olddir")
	expectTombstoneWrite(b2, "olddir")
	expectSidecarWrite(b1, "newdir")

	req := &fuse.RenameRequest{OldName: "olddir", NewName: "newdir"}
	err := root.Rename(context.Background(), req, root)
	require.NoError(t, err)

	newNode, ok := cache.Get("newdir")
	assert.True(t, ok)
	assert.Equal(t, []string{"b1"}, newNode.Meta.Backends)

	// Give any (incorrect) async cleanup goroutine a chance to fire.
	time.Sleep(50 * time.Millisecond)
	b2.AssertNotCalled(t, "Remove", mock.Anything, "olddir")

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestDir_Rename_Quorum(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("old.txt", vfs.Metadata{Name: "old.txt", Path: "old.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("old.txt", vfs.Metadata{Name: "old.txt", Path: "old.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	// b1 succeeds, b2 fails
	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "old.txt", "new.txt").Return(nil)
	// The old-path tombstone goes to ALL backends (best-effort); the fresh
	// new-path sidecar only to the successful one. The target path carries no
	// tombstone on either backend.
	expectTombstoneWrite(b1, "old.txt")
	expectTombstoneWrite(b2, "old.txt")
	expectNoTombstone(b1, "new.txt")
	expectNoTombstone(b2, "new.txt")
	expectSidecarWrite(b1, "new.txt")

	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b2.On("Rename", mock.Anything, "old.txt", "new.txt").Return(os.ErrPermission)
	b2.On("Remove", mock.Anything, "old.txt").Return(nil)

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := root.Rename(context.Background(), req, root)

	// Should succeed because quorum (1) was reached
	require.NoError(t, err)

	// Backend list for new file should only contain b1
	node, _ := cache.Get("new.txt")
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)

	// Wait for the async cleanup goroutine to finish
	time.Sleep(50 * time.Millisecond)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestDir_Rename_DescendantReKeying(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("olddir", vfs.Metadata{Name: "olddir", Path: "olddir", IsDir: true, Mode: os.ModeDir | 0755, Backends: []string{"b1"}, DataGen: 2}, "b1")
	cache.Upsert("olddir/file.txt", vfs.Metadata{Name: "file.txt", Path: "olddir/file.txt", Backends: []string{"b1"}, DataGen: 5}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
		NodeID:            "node-test",
	}

	rootNode, _ := fs.Root()
	root := rootNode.(*Dir)

	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "olddir", "newdir").Return(nil)

	// Walk finds the descendant file
	b1.On("Walk", mock.Anything, "olddir", mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(string, backend.FileInfo) error)
		_ = fn("olddir/file.txt", backend.FileInfo{Name: "file.txt", Size: 100})
	}).Return(nil)

	expectNoTombstone(b1, "newdir")
	expectNoTombstone(b1, "newdir/file.txt")

	tcDirOld := expectTombstoneWrite(b1, "olddir")
	scDirNew := expectSidecarWrite(b1, "newdir")
	tcFileOld := expectTombstoneWrite(b1, "olddir/file.txt")
	scFileNew := expectSidecarWrite(b1, "newdir/file.txt")

	req := &fuse.RenameRequest{OldName: "olddir", NewName: "newdir"}
	err := root.Rename(context.Background(), req, root)
	require.NoError(t, err)

	// Verify old path tombstoned
	wDirOld, nDirOld := tcDirOld.get()
	assert.Equal(t, 1, nDirOld)
	assert.Equal(t, int64(3), wDirOld.DataGen)
	assert.True(t, wDirOld.Deleted)

	// Verify new path sidecar written
	wDirNew, nDirNew := scDirNew.get()
	assert.Equal(t, 1, nDirNew)
	assert.Equal(t, int64(3), wDirNew.DataGen)
	assert.False(t, wDirNew.Deleted)

	// Verify descendant old path tombstoned
	wFileOld, nFileOld := tcFileOld.get()
	assert.Equal(t, 1, nFileOld)
	assert.Equal(t, int64(6), wFileOld.DataGen)
	assert.True(t, wFileOld.Deleted)

	// Verify descendant new path sidecar written
	wFileNew, nFileNew := scFileNew.get()
	assert.Equal(t, 1, nFileNew)
	assert.Equal(t, int64(6), wFileNew.DataGen)
	assert.False(t, wFileNew.Deleted)

	// Verify cache updates
	_, ok := cache.Get("olddir")
	assert.False(t, ok)
	_, ok = cache.Get("olddir/file.txt")
	assert.False(t, ok)

	dirNode, ok := cache.Get("newdir")
	assert.True(t, ok)
	assert.Equal(t, int64(3), dirNode.Meta.DataGen)

	fileNode, ok := cache.Get("newdir/file.txt")
	assert.True(t, ok)
	assert.Equal(t, int64(6), fileNode.Meta.DataGen)

	b1.AssertExpectations(t)
}
