package fuse_test

import (
	"context"
	"os"
	"testing"

	"bazil.org/fuse"
	"github.com/digitalentity/replistore/internal/backend"
	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/test"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFS_Lookup(t *testing.T) {
	cache := vfs.NewCache()
	cache.Upsert("test.txt", vfs.Metadata{Name: "test.txt", Size: 100}, "b1")

	fs := &rfuse.RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{},
	}
	root, err := fs.Root()
	assert.NoError(t, err)

	dir := root.(*rfuse.Dir)
	node, err := dir.Lookup(context.Background(), "test.txt")
	assert.NoError(t, err)
	assert.NotNil(t, node)
}

func TestFS_ReadDirAll(t *testing.T) {
	cache := vfs.NewCache()
	cache.Upsert("file1", vfs.Metadata{Name: "file1", IsDir: false}, "b1")
	cache.Upsert("dir1", vfs.Metadata{Name: "dir1", IsDir: true}, "b1")

	fs := &rfuse.RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{},
	}
	root, _ := fs.Root()
	dir := root.(*rfuse.Dir)

	dirents, err := dir.ReadDirAll(context.Background())
	assert.NoError(t, err)
	assert.Len(t, dirents, 2)
}

func TestFS_Create(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	
	fs := &rfuse.RepliFS{
		Cache:             vfs.NewCache(),
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 1,
	}
	
	root, _ := fs.Root()
	dir := root.(*rfuse.Dir)

	mockFile := &test.MockFile{}
	// Expect creation on one backend (RF=1)
	// We don't know which one picked first, so we might need to be flexible or check logic.
	// The logic iterates the map, order is random.
	
	// Mock OpenFile on both to be safe, or we can check which one was called.
	// Using .Maybe()
	b1.On("OpenFile", "newfile.txt", mock.Anything, mock.Anything).Return(mockFile, nil).Maybe()
	b2.On("OpenFile", "newfile.txt", mock.Anything, mock.Anything).Return(mockFile, nil).Maybe()

	req := &fuse.CreateRequest{Name: "newfile.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	
	node, handle, err := dir.Create(context.Background(), req, resp)
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, handle)
}

func TestFS_Mkdir(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	
	fs := &rfuse.RepliFS{
		Cache:             vfs.NewCache(),
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
	}
	
	root, _ := fs.Root()
	dir := root.(*rfuse.Dir)

	b1.On("Mkdir", "newdir", mock.Anything).Return(nil)

	req := &fuse.MkdirRequest{Name: "newdir", Mode: 0755}
	node, err := dir.Mkdir(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, node)
	b1.AssertExpectations(t)
}

func TestFile_Read(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	mockFile := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("test.txt", vfs.Metadata{Name: "test.txt", Path: "test.txt", Backends: []string{"b1"}}, "b1")

	fs := &rfuse.RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1},
	}
	
	root, _ := fs.Root()
	dir := root.(*rfuse.Dir)
	node, _ := dir.Lookup(context.Background(), "test.txt")
	file := node.(*rfuse.File)

	b1.On("OpenFile", "test.txt", os.O_RDONLY, mock.Anything).Return(mockFile, nil)
	
	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	
	fileHandle := h.(*rfuse.FileHandle)
	
	mockFile.On("ReadAt", mock.Anything, int64(0)).Return(5, nil)
	
	readReq := &fuse.ReadRequest{Size: 5, Offset: 0}
	readResp := &fuse.ReadResponse{}
	err = fileHandle.Read(context.Background(), readReq, readResp)
	assert.NoError(t, err)
	assert.Len(t, readResp.Data, 5)
}

func TestFile_Write(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	mockFile := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("test.txt", vfs.Metadata{Name: "test.txt", Path: "test.txt", Backends: []string{"b1"}}, "b1")

	fs := &rfuse.RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1},
	}
	
	root, _ := fs.Root()
	dir := root.(*rfuse.Dir)
	node, _ := dir.Lookup(context.Background(), "test.txt")
	file := node.(*rfuse.File)

	b1.On("OpenFile", "test.txt", mock.Anything, mock.Anything).Return(mockFile, nil)
	
	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	
	fileHandle := h.(*rfuse.FileHandle)
	
	data := []byte("hello")
	mockFile.On("WriteAt", data, int64(0)).Return(5, nil)
	
	writeReq := &fuse.WriteRequest{Data: data, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(context.Background(), writeReq, writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 5, writeResp.Size)
}
