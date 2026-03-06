package fuse

import (
	"context"
	"fmt"
	"os"
	"testing"

	"bazil.org/fuse"
	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/test"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFS_Lookup(t *testing.T) {
	cache := vfs.NewCache()
	cache.Upsert("test.txt", vfs.Metadata{Name: "test.txt", Size: 100}, "b1")

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{},
		Selector: vfs.NewRandomSelector(nil),
	}
	root, err := fs.Root()
	assert.NoError(t, err)

	dir := root.(*Dir)
	node, err := dir.Lookup(context.Background(), "test.txt")
	assert.NoError(t, err)
	assert.NotNil(t, node)
}

func TestFS_ReadDirAll(t *testing.T) {
	cache := vfs.NewCache()
	cache.Upsert("file1", vfs.Metadata{Name: "file1", IsDir: false}, "b1")
	cache.Upsert("dir1", vfs.Metadata{Name: "dir1", IsDir: true}, "b1")

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{},
		Selector: vfs.NewRandomSelector(nil),
	}
	root, _ := fs.Root()
	dir := root.(*Dir)

	dirents, err := dir.ReadDirAll(context.Background())
	assert.NoError(t, err)
	assert.Len(t, dirents, 2)
}

func TestDir_Create(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	mockFile := &test.MockFile{}

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}
	
	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("OpenFile", "new.txt", os.O_CREATE|os.O_RDWR, os.FileMode(0644)).Return(mockFile, nil)

	req := &fuse.CreateRequest{Name: "new.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, handle)
	
	b1.AssertExpectations(t)
}

func TestFile_Write(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	mockFile := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("test.txt", vfs.Metadata{Name: "test.txt", Path: "test.txt", Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}
	
	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "test.txt")
	file := node.(*File)

	b1.On("OpenFile", "test.txt", mock.Anything, mock.Anything).Return(mockFile, nil)
	
	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	
	fileHandle := h.(*FileHandle)
	
	data := []byte("hello")
	mockFile.On("WriteAt", data, int64(0)).Return(5, nil)
	
	writeReq := &fuse.WriteRequest{Data: data, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(context.Background(), writeReq, writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 5, writeResp.Size)
}

func TestFile_Read_Failover(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	// The order of Upsert calls matters for backend list order in simple implementation.
	// Since Backends is a slice in Metadata, first one appended is first one used by Open (usually).
	cache.Upsert("failover.txt", vfs.Metadata{Name: "failover.txt", Path: "failover.txt", Backends: []string{"b1", "b2"}}, "b1")
	// Updating with b2
	cache.Upsert("failover.txt", vfs.Metadata{Name: "failover.txt", Path: "failover.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1, "b2": b2},
		Selector: vfs.NewFirstSelector(nil),
	}
	
	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "failover.txt")
	file := node.(*File)

	// Open selects b1 (first in list)
	b1.On("OpenFile", "failover.txt", os.O_RDONLY, mock.Anything).Return(mockFile1, nil)
	
	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	
	fileHandle := h.(*FileHandle)
	
	// First read on b1 fails
	mockFile1.On("ReadAt", mock.Anything, int64(0)).Return(0, fmt.Errorf("connection lost"))
	mockFile1.On("Close").Return(nil)

	// Failover logic should trigger:
	// 1. Close b1.
	// 2. Open b2.
	// 3. Read from b2.
	b2.On("OpenFile", "failover.txt", os.O_RDONLY, mock.Anything).Return(mockFile2, nil)
	mockFile2.On("ReadAt", mock.Anything, int64(0)).Return(5, nil)

	readReq := &fuse.ReadRequest{Size: 5, Offset: 0}
	readResp := &fuse.ReadResponse{}
	err = fileHandle.Read(context.Background(), readReq, readResp)
	assert.NoError(t, err)
	assert.Len(t, readResp.Data, 5)
	
	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}

func TestFile_Write_Quorum(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	// Initial state: file exists on both b1 and b2
	cache.Upsert("quorum.txt", vfs.Metadata{Name: "quorum.txt", Path: "quorum.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("quorum.txt", vfs.Metadata{Name: "quorum.txt", Path: "quorum.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1, // Quorum of 1 out of 2
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "quorum.txt")
	file := node.(*File)

	// Open for writing opens BOTH backends
	b1.On("OpenFile", "quorum.txt", mock.Anything, mock.Anything).Return(mockFile1, nil)
	b2.On("OpenFile", "quorum.txt", mock.Anything, mock.Anything).Return(mockFile2, nil)

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)

	fileHandle := h.(*FileHandle)

	data := []byte("hello")
	// b1 succeeds, b2 fails
	mockFile1.On("WriteAt", data, int64(0)).Return(5, nil)
	mockFile2.On("WriteAt", data, int64(0)).Return(0, fmt.Errorf("disk full"))
	mockFile2.On("Close").Return(nil)

	writeReq := &fuse.WriteRequest{Data: data, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(context.Background(), writeReq, writeResp)

	// Should succeed because quorum (1) was reached
	assert.NoError(t, err)
	assert.Equal(t, 5, writeResp.Size)

	// Metadata should be updated to only contain b1
	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}
