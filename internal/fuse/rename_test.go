package fuse

import (
	"context"
	"os"
	"testing"

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
	
	b2.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b2.On("Rename", mock.Anything, "old.txt", "new.txt").Return(os.ErrPermission)

	req := &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}
	err := root.Rename(context.Background(), req, root)

	// Should succeed because quorum (1) was reached
	assert.NoError(t, err)

	// Backend list for new file should only contain b1
	node, _ := cache.Get("new.txt")
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}
