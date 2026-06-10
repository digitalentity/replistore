package fuse

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

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

	b1.On("OpenFile", mock.Anything, "new.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile, nil)

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

func TestLookup_AllBackendsUnavailable(t *testing.T) {
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

	// Backend errors transiently — we must not report ENOENT
	b1.On("Stat", mock.Anything, "lazy/file.txt").Return(backend.FileInfo{}, fmt.Errorf("conn reset"))

	node, err := (lazyDir.(*Dir)).Lookup(context.Background(), "file.txt")
	assert.Equal(t, syscall.EIO, err)
	assert.Nil(t, node)

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
	b1.On("Remove", mock.Anything, "new-dir").Return(nil) // Rollback
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

func TestMkdir_AlreadyExistsCountsTowardQuorum(t *testing.T) {
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

	b1.On("Mkdir", mock.Anything, "existing-dir", mock.Anything).Return(nil)
	// b2 already has the directory (e.g. created by another cluster node).
	b2.On("Mkdir", mock.Anything, "existing-dir", mock.Anything).Return(os.ErrExist)
	b2.On("Stat", mock.Anything, "existing-dir").Return(backend.FileInfo{Name: "existing-dir", IsDir: true}, nil)

	req := &fuse.MkdirRequest{Name: "existing-dir", Mode: 0755}
	node, err := dir.Mkdir(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, node)

	child := node.(*Dir)
	assert.ElementsMatch(t, []string{"b1", "b2"}, child.node.Meta.Backends)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestMkdir_ExistsAsFileNotCounted(t *testing.T) {
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

	b1.On("Mkdir", mock.Anything, "blocked-dir", mock.Anything).Return(nil)
	b1.On("Remove", mock.Anything, "blocked-dir").Return(nil) // Rollback of the dir we created
	// b2 has a FILE in the way, not a directory.
	b2.On("Mkdir", mock.Anything, "blocked-dir", mock.Anything).Return(os.ErrExist)
	b2.On("Stat", mock.Anything, "blocked-dir").Return(backend.FileInfo{Name: "blocked-dir", IsDir: false}, nil)

	req := &fuse.MkdirRequest{Name: "blocked-dir", Mode: 0755}
	_, err := dir.Mkdir(context.Background(), req)

	assert.Error(t, err)

	// Rollback must not touch b2's pre-existing file.
	b2.AssertNotCalled(t, "Remove", mock.Anything, "blocked-dir")

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

func TestFile_Write_QuorumFailure(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("quorum_fail.txt", vfs.Metadata{Name: "quorum_fail.txt", Path: "quorum_fail.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("quorum_fail.txt", vfs.Metadata{Name: "quorum_fail.txt", Path: "quorum_fail.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "quorum_fail.txt")
	file := node.(*File)

	b1.On("OpenFile", mock.Anything, "quorum_fail.txt", mock.Anything, mock.Anything).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "quorum_fail.txt", mock.Anything, mock.Anything).Return(mockFile2, nil)

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

	assert.Error(t, err)

	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}

func TestDir_Create_QuorumFailure(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("OpenFile", mock.Anything, "new_fail.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "new_fail.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(nil, fmt.Errorf("permission denied"))

	mockFile1.On("Close").Return(nil)
	b1.On("Remove", mock.Anything, "new_fail.txt").Return(nil)

	req := &fuse.CreateRequest{Name: "new_fail.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Nil(t, handle)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
}

func TestDir_Create_AlreadyExistsOnBackends(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	// Every backend reports the file already exists.
	b1.On("OpenFile", mock.Anything, "exists.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(nil, os.ErrExist)
	b2.On("OpenFile", mock.Anything, "exists.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(nil, os.ErrExist)

	// FetchEntry should be triggered to merge the discovered file into the cache.
	info := backend.FileInfo{Name: "exists.txt", Size: 42, Mode: 0644, ModTime: time.Now()}
	b1.On("Stat", mock.Anything, "exists.txt").Return(info, nil)
	b2.On("Stat", mock.Anything, "exists.txt").Return(info, nil)

	req := &fuse.CreateRequest{Name: "exists.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.ErrorIs(t, err, syscall.EEXIST)
	assert.Nil(t, node)
	assert.Nil(t, handle)

	// No replicas were created, so nothing must be removed.
	b1.AssertNotCalled(t, "Remove", mock.Anything, "exists.txt")
	b2.AssertNotCalled(t, "Remove", mock.Anything, "exists.txt")

	// The discovered file must now be in the cache.
	cached, ok := cache.Get("exists.txt")
	assert.True(t, ok)
	assert.Equal(t, int64(42), cached.Meta.Size)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestFile_Open_HealDegraded(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	srcMockFile := &test.MockFile{}
	dstMockFile := &test.MockFile{}
	b1WriteFile := &test.MockFile{}
	b2WriteFile := &test.MockFile{}

	cache := vfs.NewCache()
	// Node has initially Backends = []string{"b1"}
	cache.Upsert("degraded.txt", vfs.Metadata{
		Name:     "degraded.txt",
		Path:     "degraded.txt",
		Mode:     0644,
		Backends: []string{"b1"},
	}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "degraded.txt")
	file := node.(*File)

	// Mock b1 (source backend) read open for heal
	b1.On("OpenFile", mock.Anything, "degraded.txt", os.O_RDONLY, mock.Anything).Return(srcMockFile, nil)

	// Mock srcMockFile read calls
	srcMockFile.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Return(5, nil).Run(func(args mock.Arguments) {
		buf := args.Get(1).([]byte)
		copy(buf, []byte("hello"))
	})
	srcMockFile.On("ReadAt", mock.Anything, mock.Anything, int64(5)).Return(0, io.EOF)
	srcMockFile.On("Close").Return(nil)

	// Mock b2 (target backend) write open for heal
	b2.On("OpenFile", mock.Anything, "degraded.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(dstMockFile, nil)
	dstMockFile.On("WriteAt", mock.Anything, []byte("hello"), int64(0)).Return(5, nil)
	dstMockFile.On("Close").Return(nil)

	// Mock final open for writing for both backends
	b1.On("OpenFile", mock.Anything, "degraded.txt", mock.Anything, mock.Anything).Return(b1WriteFile, nil)
	b2.On("OpenFile", mock.Anything, "degraded.txt", mock.Anything, mock.Anything).Return(b2WriteFile, nil)

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	assert.NotNil(t, h)

	// Verify that the file's VFS cache node now lists ["b1", "b2"] as backends
	file.node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	srcMockFile.AssertExpectations(t)
	dstMockFile.AssertExpectations(t)
}

func TestDir_Create_MkdirAllParent(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	mockFile := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("parent/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	parentNode, ok := cache.Get("parent")
	assert.True(t, ok)

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	dir := &Dir{fs: fs, node: parentNode}

	b1.On("MkdirAll", mock.Anything, "parent", os.FileMode(0755)).Return(nil)
	b1.On("OpenFile", mock.Anything, "parent/new.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile, nil)

	req := &fuse.CreateRequest{Name: "new.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, handle)

	b1.AssertExpectations(t)
}

func TestDir_Mkdir_MkdirAllParent(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("parent/dummy", vfs.Metadata{Name: "dummy"}, "b1")
	parentNode, ok := cache.Get("parent")
	assert.True(t, ok)

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
	}

	dir := &Dir{fs: fs, node: parentNode}

	b1.On("MkdirAll", mock.Anything, "parent", os.FileMode(0755)).Return(nil)
	b1.On("Mkdir", mock.Anything, "parent/new-sub-dir", os.FileMode(0755)).Return(nil)

	req := &fuse.MkdirRequest{Name: "new-sub-dir", Mode: 0755}
	node, err := dir.Mkdir(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, node)

	b1.AssertExpectations(t)
}

func TestFile_NodeFsync(t *testing.T) {
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
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "sync.txt")
	file := node.(*File)

	b1.On("OpenFile", mock.Anything, "sync.txt", os.O_RDWR, os.FileMode(0)).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "sync.txt", os.O_RDWR, os.FileMode(0)).Return(mockFile2, nil)

	mockFile1.On("Sync", mock.Anything).Return(nil)
	mockFile1.On("Close").Return(nil)

	mockFile2.On("Sync", mock.Anything).Return(nil)
	mockFile2.On("Close").Return(nil)

	err := file.Fsync(context.Background(), &fuse.FsyncRequest{})
	assert.NoError(t, err)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}
