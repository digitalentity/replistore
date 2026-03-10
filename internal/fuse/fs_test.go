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

	b1.On("OpenFile", mock.Anything, "new.txt", os.O_CREATE|os.O_RDWR, os.FileMode(0644)).Return(mockFile, nil)

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

	b1.On("OpenFile", mock.Anything, "test.txt", mock.Anything, mock.Anything).Return(mockFile, nil)
	
	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	
	fileHandle := h.(*FileHandle)
	
	data := []byte("hello")
	mockFile.On("WriteAt", mock.Anything, data, int64(0)).Return(5, nil)
	
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
	cache.Upsert("failover.txt", vfs.Metadata{Name: "failover.txt", Path: "failover.txt", Backends: []string{"b1", "b2"}}, "b1")
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

	b1.On("OpenFile", mock.Anything, "failover.txt", os.O_RDONLY, mock.Anything).Return(mockFile1, nil)
	
	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	
	fileHandle := h.(*FileHandle)
	
	mockFile1.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Return(0, fmt.Errorf("connection lost"))
	mockFile1.On("Close").Return(nil)

	b2.On("OpenFile", mock.Anything, "failover.txt", os.O_RDONLY, mock.Anything).Return(mockFile2, nil)
	mockFile2.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Return(5, nil)

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
	cache.Upsert("quorum.txt", vfs.Metadata{Name: "quorum.txt", Path: "quorum.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("quorum.txt", vfs.Metadata{Name: "quorum.txt", Path: "quorum.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "quorum.txt")
	file := node.(*File)

	b1.On("OpenFile", mock.Anything, "quorum.txt", mock.Anything, mock.Anything).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "quorum.txt", mock.Anything, mock.Anything).Return(mockFile2, nil)

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)

	fileHandle := h.(*FileHandle)

	data := []byte("hello")
	mockFile1.On("WriteAt", mock.Anything, data, int64(0)).Return(5, nil)
	mockFile2.On("WriteAt", mock.Anything, data, int64(0)).Return(0, fmt.Errorf("disk full"))
	mockFile2.On("Close").Return(nil)

	writeReq := &fuse.WriteRequest{Data: data, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(context.Background(), writeReq, writeResp)

	assert.NoError(t, err)
	assert.Equal(t, 5, writeResp.Size)

	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}

func TestFile_Fsync(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("sync.txt", vfs.Metadata{Name: "sync.txt", Path: "sync.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("sync.txt", vfs.Metadata{Name: "sync.txt", Path: "sync.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "sync.txt")
	file := node.(*File)

	b1.On("OpenFile", mock.Anything, "sync.txt", mock.Anything, mock.Anything).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "sync.txt", mock.Anything, mock.Anything).Return(mockFile2, nil)

	h, _ := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	fileHandle := h.(*FileHandle)

	mockFile1.On("Sync", mock.Anything).Return(nil)
	mockFile2.On("Sync", mock.Anything).Return(fmt.Errorf("sync error"))
	mockFile2.On("Close").Return(nil)

	err := fileHandle.Fsync(context.Background(), &fuse.FsyncRequest{})
	assert.NoError(t, err)

	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}

func TestLookup_LazyTrigger(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	cache := vfs.NewCache()
	// Directory exists in cache but not fully indexed
	cache.Upsert("lazy/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	dirNode, _ := cache.Get("lazy")
	dirNode.FullyIndexed = false

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1},
		Selector: vfs.NewRandomSelector(nil),
	}
	root, _ := fs.Root()
	dir := root.(*Dir)
	lazyDir, _ := dir.Lookup(context.Background(), "lazy")

	b1.On("Stat", mock.Anything, "lazy/file.txt").Return(backend.FileInfo{
		Name: "file.txt",
		Size: 100,
	}, nil)

	node, err := (lazyDir.(*Dir)).Lookup(context.Background(), "file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, node)
	
	b1.AssertExpectations(t)
}

func TestReadDir_LazyTrigger(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	cache := vfs.NewCache()
	cache.Upsert("lazy-dir/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	dirNode, _ := cache.Get("lazy-dir")
	dirNode.FullyIndexed = false

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1},
		Selector: vfs.NewRandomSelector(nil),
	}
	root, _ := fs.Root()
	dir, _ := (root.(*Dir)).Lookup(context.Background(), "lazy-dir")

	b1.On("ReadDir", mock.Anything, "lazy-dir").Return([]backend.FileInfo{
		{Name: "file1.txt", Size: 10},
	}, nil)

	dirents, err := (dir.(*Dir)).ReadDirAll(context.Background())
	assert.NoError(t, err)
	assert.Len(t, dirents, 2) // dummy + file1.txt
	
	b1.AssertExpectations(t)
}

func TestMkdir_Quorum(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2, // Strict quorum
		Selector:          vfs.NewRandomSelector(nil),
	}
	
	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("Mkdir", mock.Anything, "new-dir", mock.Anything).Return(nil)
	b2.On("Mkdir", mock.Anything, "new-dir", mock.Anything).Return(fmt.Errorf("failed"))

	req := &fuse.MkdirRequest{Name: "new-dir"}
	_, err := dir.Mkdir(context.Background(), req)

	// Currently this might pass if the code doesn't check quorum for Mkdir
	// according to PROPOSAL.md. 
	// If it fails, then it's already fixed or I'm testing the fix.
	assert.Error(t, err)
	
	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestRemove_Quorum(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("remove.txt", vfs.Metadata{Name: "remove.txt", Path: "remove.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("remove.txt", vfs.Metadata{Name: "remove.txt", Path: "remove.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2, // Strict quorum
		Selector:          vfs.NewRandomSelector(nil),
	}
	
	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("Remove", mock.Anything, "remove.txt").Return(nil)
	b2.On("Remove", mock.Anything, "remove.txt").Return(fmt.Errorf("not found"))

	req := &fuse.RemoveRequest{Name: "remove.txt"}
	err := dir.Remove(context.Background(), req)

	// Should fail because quorum is 2 but only 1 succeeded
	assert.Error(t, err)
}
