package fuse

import (
	"context"
	"os"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/test"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDir_Rename_Simple(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

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
	// The sidecar moves with the file.
	b1.On("MkdirAll", mock.Anything, ".replistore/meta", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, vfs.SidecarPath("old.txt"), vfs.SidecarPath("new.txt")).Return(nil)

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := root.Rename(context.Background(), req, root)

	assert.NoError(t, err)

	// Check cache
	_, ok := cache.Get("old.txt")
	assert.False(t, ok)
	node, ok := cache.Get("new.txt")
	assert.True(t, ok)
	assert.Equal(t, "new.txt", node.Meta.Name)
	assert.Equal(t, "new.txt", node.Meta.Path)

	b1.AssertExpectations(t)
}

func TestDir_Rename_CrossDir(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

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
	expectSidecarRename(b1, "dir1/old.txt", "dir2/new.txt")

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := d1.Rename(context.Background(), req, d2)

	assert.NoError(t, err)

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
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

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
	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b2.On("Rename", mock.Anything, "olddir", "newdir").Return(nil)

	req := &fuse.RenameRequest{OldName: "olddir", NewName: "newdir"}
	err := root.Rename(context.Background(), req, root)
	assert.NoError(t, err)

	_, ok := cache.Get("olddir")
	assert.False(t, ok)
	newNode, ok := cache.Get("newdir")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b1", "b2"}, newNode.Meta.Backends)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestDir_Rename_DirNotExistIsSkipped(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

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
	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	// Source dir simply doesn't exist on b2: neither success nor failure,
	// and no async orphan removal must be triggered.
	b2.On("Rename", mock.Anything, "olddir", "newdir").Return(os.ErrNotExist)

	req := &fuse.RenameRequest{OldName: "olddir", NewName: "newdir"}
	err := root.Rename(context.Background(), req, root)
	assert.NoError(t, err)

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
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

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
	// Sidecar rename happens only on the successful backend.
	expectSidecarRename(b1, "old.txt", "new.txt")

	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b2.On("Rename", mock.Anything, "old.txt", "new.txt").Return(os.ErrPermission)
	b2.On("Remove", mock.Anything, "old.txt").Return(nil)

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := root.Rename(context.Background(), req, root)

	// Should succeed because quorum (1) was reached
	assert.NoError(t, err)

	// Backend list for new file should only contain b1
	node, _ := cache.Get("new.txt")
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)

	// Wait for the async cleanup goroutine to finish
	time.Sleep(50 * time.Millisecond)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}
