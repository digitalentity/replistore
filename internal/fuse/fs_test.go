package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	gopath "path"
	"sync"
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

// sidecarCapture records sidecar writes registered via expectSidecarWrite.
type sidecarCapture struct {
	mu    sync.Mutex
	count int
	last  vfs.Sidecar
}

func (c *sidecarCapture) get() (vfs.Sidecar, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, c.count
}

// expectSidecarWrite registers permissive (.Maybe()) expectations for sidecar
// writes of dataPath on b — MkdirAll + OpenFile + WriteAt + Close — and
// captures what was written. Permissive so tests that aren't about sidecars
// stay minimal.
func expectSidecarWrite(b *test.MockBackend, dataPath string) *sidecarCapture {
	c := &sidecarCapture{}
	f := &test.MockFile{}
	sp := vfs.SidecarPath(dataPath)
	b.On("MkdirAll", mock.Anything, gopath.Dir(sp), os.FileMode(0755)).Return(nil).Maybe()
	b.On("OpenFile", mock.Anything, sp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(f, nil).Maybe()
	f.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.count++
		_ = json.Unmarshal(args.Get(1).([]byte), &c.last)
	}).Return(0, nil).Maybe()
	f.On("Close").Return(nil).Maybe()
	return c
}

// expectTombstoneWrite registers permissive (.Maybe()) expectations for
// tombstone writes of dataPath on b and captures what was written.
func expectTombstoneWrite(b *test.MockBackend, dataPath string) *sidecarCapture {
	c := &sidecarCapture{}
	f := &test.MockFile{}
	tp := vfs.TombstonePath(dataPath)
	b.On("MkdirAll", mock.Anything, gopath.Dir(tp), os.FileMode(0755)).Return(nil).Maybe()
	b.On("OpenFile", mock.Anything, tp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(f, nil).Maybe()
	f.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.count++
		_ = json.Unmarshal(args.Get(1).([]byte), &c.last)
	}).Return(0, nil).Maybe()
	f.On("Close").Return(nil).Maybe()
	return c
}

// expectSidecarRemove registers a permissive expectation for sidecar deletion
// of dataPath on b.
func expectSidecarRemove(b *test.MockBackend, dataPath string) {
	b.On("Remove", mock.Anything, vfs.SidecarPath(dataPath)).Return(nil).Maybe()
}

// expectNoTombstone makes b report no tombstone for dataPath (used by
// FetchEntry and the Create/Rename tombstone-generation reads).
func expectNoTombstone(b *test.MockBackend, dataPath string) {
	b.On("OpenFile", mock.Anything, vfs.TombstonePath(dataPath), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)
}

// expectTombstoneGen makes b serve a tombstone at the given generation for
// dataPath (used by the Create/Rename tombstone-generation reads).
func expectTombstoneGen(b *test.MockBackend, dataPath string, gen int64) {
	payload := []byte(fmt.Sprintf(`{"v":1,"gen":%d,"writer":"w","deleted":true}`, gen))
	f := &test.MockFile{}
	f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), payload)
	}).Return(len(payload), io.EOF)
	f.On("Close").Return(nil)
	b.On("OpenFile", mock.Anything, vfs.TombstonePath(dataPath), os.O_RDONLY, os.FileMode(0)).Return(f, nil)
}

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
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("OpenFile", mock.Anything, "new.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile, nil)
	expectNoTombstone(b1, "new.txt")
	sc := expectSidecarWrite(b1, "new.txt")

	req := &fuse.CreateRequest{Name: "new.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, handle)

	// Create must stamp generation 1 on the new replica and in the cache.
	written, count := sc.get()
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(1), written.Gen)
	assert.Equal(t, "node-test", written.Writer)

	file := node.(*File)
	file.node.Mu.RLock()
	assert.Equal(t, int64(1), file.node.Meta.Gen)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
}

// TestDir_Create_AboveTombstoneGen: creating at a path whose deletion is
// recorded by a tombstone must start the new lineage ABOVE the tombstone
// generation, otherwise the next sync/repair pass destroys the fresh file.
func TestDir_Create_AboveTombstoneGen(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("OpenFile", mock.Anything, "reborn.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "reborn.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile2, nil)

	// b1 carries a tombstone at gen 3 from a previous deletion; b2 has none.
	expectTombstoneGen(b1, "reborn.txt", 3)
	expectNoTombstone(b2, "reborn.txt")
	sc1 := expectSidecarWrite(b1, "reborn.txt")
	sc2 := expectSidecarWrite(b2, "reborn.txt")

	req := &fuse.CreateRequest{Name: "reborn.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, handle)

	// The new lineage must start at tombstone gen + 1 = 4, on every replica
	// and in the cache.
	w1, n1 := sc1.get()
	w2, n2 := sc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(4), w1.Gen)
	assert.Equal(t, int64(4), w2.Gen)

	file := node.(*File)
	file.node.Mu.RLock()
	assert.Equal(t, int64(4), file.node.Meta.Gen)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
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
	expectSidecarWrite(b1, "test.txt")

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

func TestFile_Open_ReadOnly_Failover(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("ro_failover.txt", vfs.Metadata{Name: "ro_failover.txt", Path: "ro_failover.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("ro_failover.txt", vfs.Metadata{Name: "ro_failover.txt", Path: "ro_failover.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1, "b2": b2},
		Selector: vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "ro_failover.txt")
	file := node.(*File)

	// The FirstSelector deterministically picks b1; it is down, so Open
	// must fail over to b2.
	b1.On("OpenFile", mock.Anything, "ro_failover.txt", os.O_RDONLY, mock.Anything).Return(nil, fmt.Errorf("connection refused"))
	b2.On("OpenFile", mock.Anything, "ro_failover.txt", os.O_RDONLY, mock.Anything).Return(mockFile2, nil)

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.NoError(t, err)

	fileHandle := h.(*FileHandle)
	assert.Len(t, fileHandle.backends, 1)
	assert.Contains(t, fileHandle.backends, "b2")

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestFile_Open_ReadOnly_AllBackendsFail(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("ro_fail.txt", vfs.Metadata{Name: "ro_fail.txt", Path: "ro_fail.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("ro_fail.txt", vfs.Metadata{Name: "ro_fail.txt", Path: "ro_fail.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1, "b2": b2},
		Selector: vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "ro_fail.txt")
	file := node.(*File)

	openErr := fmt.Errorf("connection refused")
	b1.On("OpenFile", mock.Anything, "ro_fail.txt", os.O_RDONLY, mock.Anything).Return(nil, openErr)
	b2.On("OpenFile", mock.Anything, "ro_fail.txt", os.O_RDONLY, mock.Anything).Return(nil, openErr)

	_, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.Equal(t, openErr, err)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
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
	expectSidecarWrite(b1, "quorum.txt")
	expectSidecarWrite(b2, "quorum.txt")

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)

	fileHandle := h.(*FileHandle)

	data := []byte("hello")
	mockFile1.On("WriteAt", mock.Anything, data, int64(0)).Return(5, nil)
	mockFile2.On("WriteAt", mock.Anything, data, int64(0)).Return(0, fmt.Errorf("disk full"))
	mockFile2.On("Close").Return(nil)

	// The stale partial replica must be removed from the failed backend
	// asynchronously.
	removed := make(chan struct{})
	b2.On("Remove", mock.Anything, "quorum.txt").Return(nil).Run(func(mock.Arguments) {
		close(removed)
	})

	writeReq := &fuse.WriteRequest{Data: data, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(context.Background(), writeReq, writeResp)

	assert.NoError(t, err)
	assert.Equal(t, 5, writeResp.Size)

	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("stale replica was not removed from failed backend")
	}

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	mockFile1.AssertExpectations(t)
	mockFile2.AssertExpectations(t)
}

func TestFile_Fsync_WithWriteHandle(t *testing.T) {
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

	// Exactly one open per backend for the data file: node-level Fsync must
	// sync the write handle's already-open files, not open fresh ones.
	b1.On("OpenFile", mock.Anything, "sync.txt", mock.Anything, mock.Anything).Return(mockFile1, nil).Once()
	b2.On("OpenFile", mock.Anything, "sync.txt", mock.Anything, mock.Anything).Return(mockFile2, nil).Once()
	expectSidecarWrite(b1, "sync.txt")
	expectSidecarWrite(b2, "sync.txt")

	h, _ := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NotNil(t, h)

	mockFile1.On("Sync", mock.Anything).Return(nil)
	mockFile2.On("Sync", mock.Anything).Return(fmt.Errorf("sync error"))
	mockFile2.On("Close").Return(nil)

	removed := make(chan struct{})
	b2.On("Remove", mock.Anything, "sync.txt").Return(nil).Run(func(mock.Arguments) {
		close(removed)
	})

	err := file.Fsync(context.Background(), &fuse.FsyncRequest{})
	assert.NoError(t, err)
	// 2 OpenFile calls per backend: the data file (once, at Open) and the
	// sidecar write that stamps the new generation. Fsync itself must not
	// open anything.
	b1.AssertNumberOfCalls(t, "OpenFile", 2)
	b2.AssertNumberOfCalls(t, "OpenFile", 2)

	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("stale replica was not removed from failed backend")
	}

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
	// FetchEntry reads the sidecar and tombstone after a successful file Stat;
	// neither exists for a legacy gen-0 replica.
	b1.On("OpenFile", mock.Anything, vfs.SidecarPath("lazy/file.txt"), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)
	expectNoTombstone(b1, "lazy/file.txt")

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

	expectNoTombstone(b1, "existing-dir")
	expectNoTombstone(b2, "existing-dir")
	expectSidecarWrite(b1, "existing-dir")
	expectSidecarWrite(b2, "existing-dir")

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

	// The tombstone quorum succeeds; the failure is in the data removal.
	expectTombstoneWrite(b1, "remove.txt")
	expectTombstoneWrite(b2, "remove.txt")
	b1.On("Remove", mock.Anything, "remove.txt").Return(nil)
	b2.On("Remove", mock.Anything, "remove.txt").Return(fmt.Errorf("not found"))

	req := &fuse.RemoveRequest{Name: "remove.txt"}
	err := dir.Remove(context.Background(), req)

	// Should fail because quorum is 2 but only 1 succeeded
	assert.Error(t, err)
}

func TestRemove_DirFansOutToAllBackends(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	// Directory is only listed on b1 in the cache, but it may exist on b2 too
	// (directory mtimes differ per backend), so Remove must fan out to all
	// configured backends.
	cache.Upsert("subdir", vfs.Metadata{Name: "subdir", Path: "subdir", IsDir: true, Mode: os.ModeDir | 0755}, "b1")
	node, _ := cache.Get("subdir")
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2, // Strict quorum
		Selector:          vfs.NewRandomSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("Remove", mock.Anything, "subdir").Return(nil)
	// The directory being already absent on b2 counts as success for delete.
	b2.On("Remove", mock.Anything, "subdir").Return(os.ErrNotExist)

	expectTombstoneWrite(b1, "subdir")
	expectTombstoneWrite(b2, "subdir")
	expectSidecarRemove(b1, "subdir")
	expectSidecarRemove(b2, "subdir")

	req := &fuse.RemoveRequest{Name: "subdir", Dir: true}
	err := dir.Remove(context.Background(), req)
	assert.NoError(t, err)

	_, ok := cache.Get("subdir")
	assert.False(t, ok)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestRemove_FileNotExistCountsAsSuccess(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("gone.txt", vfs.Metadata{Name: "gone.txt", Path: "gone.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("gone.txt", vfs.Metadata{Name: "gone.txt", Path: "gone.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2, // Strict quorum
		Selector:          vfs.NewRandomSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	expectTombstoneWrite(b1, "gone.txt")
	expectTombstoneWrite(b2, "gone.txt")
	b1.On("Remove", mock.Anything, "gone.txt").Return(nil)
	// Idempotent delete: already-absent file counts towards quorum.
	b2.On("Remove", mock.Anything, "gone.txt").Return(os.ErrNotExist)
	expectSidecarRemove(b1, "gone.txt")
	expectSidecarRemove(b2, "gone.txt")

	req := &fuse.RemoveRequest{Name: "gone.txt"}
	err := dir.Remove(context.Background(), req)
	assert.NoError(t, err)

	_, ok := cache.Get("gone.txt")
	assert.False(t, ok)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
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
	expectSidecarWrite(b1, "quorum_fail.txt")
	expectSidecarWrite(b2, "quorum_fail.txt")

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)

	fileHandle := h.(*FileHandle)

	data := []byte("hello")
	mockFile1.On("WriteAt", mock.Anything, data, int64(0)).Return(5, nil)
	mockFile2.On("WriteAt", mock.Anything, data, int64(0)).Return(0, fmt.Errorf("disk full"))
	mockFile2.On("Close").Return(nil)

	removed := make(chan struct{})
	b2.On("Remove", mock.Anything, "quorum_fail.txt").Return(nil).Run(func(mock.Arguments) {
		close(removed)
	})

	writeReq := &fuse.WriteRequest{Data: data, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(context.Background(), writeReq, writeResp)

	assert.Error(t, err)

	file.node.Mu.RLock()
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("stale replica was not removed from failed backend")
	}

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
	// FetchEntry reads sidecars and tombstones after successful file Stats;
	// none exist here.
	b1.On("OpenFile", mock.Anything, vfs.SidecarPath("exists.txt"), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)
	b2.On("OpenFile", mock.Anything, vfs.SidecarPath("exists.txt"), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)
	expectNoTombstone(b1, "exists.txt")
	expectNoTombstone(b2, "exists.txt")

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

	// The source replica's mtime must be preserved on the healed copy.
	srcModTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	b1.On("Stat", mock.Anything, "degraded.txt").Return(backend.FileInfo{Name: "degraded.txt", Size: 5, ModTime: srcModTime}, nil)
	b2.On("Chtimes", mock.Anything, "degraded.txt", srcModTime, srcModTime).Return(nil)

	// Mock final open for writing for both backends
	b1.On("OpenFile", mock.Anything, "degraded.txt", mock.Anything, mock.Anything).Return(b1WriteFile, nil)
	b2.On("OpenFile", mock.Anything, "degraded.txt", mock.Anything, mock.Anything).Return(b2WriteFile, nil)

	// The post-heal generation bump writes sidecars to ALL backends.
	sc1 := expectSidecarWrite(b1, "degraded.txt")
	sc2 := expectSidecarWrite(b2, "degraded.txt")

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	assert.NotNil(t, h)

	// Verify that the file's VFS cache node now lists ["b1", "b2"] as backends
	file.node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, file.node.Meta.Backends)
	assert.Equal(t, int64(1), file.node.Meta.Gen)
	file.node.Mu.RUnlock()

	// One generation bump, stamped on every replica including the healed one.
	w1, n1 := sc1.get()
	w2, n2 := sc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(1), w1.Gen)
	assert.Equal(t, int64(1), w2.Gen)

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
	expectNoTombstone(b1, "parent/new.txt")
	expectSidecarWrite(b1, "parent/new.txt")

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

	expectNoTombstone(b1, "parent/new-sub-dir")
	expectSidecarWrite(b1, "parent/new-sub-dir")

	req := &fuse.MkdirRequest{Name: "new-sub-dir", Mode: 0755}
	node, err := dir.Mkdir(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, node)

	b1.AssertExpectations(t)
}

// TestFile_Fsync_FallbackNoHandles exercises the no-open-write-handle path:
// Fsync opens each replica read-only and syncs it.
func TestFile_Fsync_FallbackNoHandles(t *testing.T) {
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

	b1.On("OpenFile", mock.Anything, "sync.txt", os.O_RDONLY, os.FileMode(0)).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "sync.txt", os.O_RDONLY, os.FileMode(0)).Return(mockFile2, nil)

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

func TestFile_Setattr_Truncate(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("trunc.txt", vfs.Metadata{Name: "trunc.txt", Path: "trunc.txt", Size: 100, Mode: 0644, Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("trunc.txt", vfs.Metadata{Name: "trunc.txt", Path: "trunc.txt", Size: 100, Mode: 0644, Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "trunc.txt")
	file := node.(*File)

	b1.On("Truncate", mock.Anything, "trunc.txt", int64(10)).Return(nil)
	b2.On("Truncate", mock.Anything, "trunc.txt", int64(10)).Return(nil)
	sc1 := expectSidecarWrite(b1, "trunc.txt")
	sc2 := expectSidecarWrite(b2, "trunc.txt")

	req := &fuse.SetattrRequest{Valid: fuse.SetattrSize, Size: 10}
	resp := &fuse.SetattrResponse{}
	err := file.Setattr(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.Equal(t, uint64(10), resp.Attr.Size)

	file.node.Mu.RLock()
	assert.Equal(t, int64(10), file.node.Meta.Size)
	assert.ElementsMatch(t, []string{"b1", "b2"}, file.node.Meta.Backends)
	assert.Equal(t, int64(1), file.node.Meta.Gen)
	file.node.Mu.RUnlock()

	// Truncate bumps the generation on every surviving replica.
	w1, n1 := sc1.get()
	w2, n2 := sc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(1), w1.Gen)
	assert.Equal(t, int64(1), w2.Gen)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestFile_Setattr_Truncate_QuorumFailure(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("trunc_fail.txt", vfs.Metadata{Name: "trunc_fail.txt", Path: "trunc_fail.txt", Size: 100, Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("trunc_fail.txt", vfs.Metadata{Name: "trunc_fail.txt", Path: "trunc_fail.txt", Size: 100, Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "trunc_fail.txt")
	file := node.(*File)

	b1.On("Truncate", mock.Anything, "trunc_fail.txt", int64(0)).Return(nil)
	b2.On("Truncate", mock.Anything, "trunc_fail.txt", int64(0)).Return(fmt.Errorf("disk error"))

	req := &fuse.SetattrRequest{Valid: fuse.SetattrSize, Size: 0}
	resp := &fuse.SetattrResponse{}
	err := file.Setattr(context.Background(), req, resp)

	assert.Error(t, err)

	// Size must be unchanged on quorum failure.
	file.node.Mu.RLock()
	assert.Equal(t, int64(100), file.node.Meta.Size)
	file.node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestFile_Setattr_Truncate_PartialFailureEvictsBackend(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("trunc_part.txt", vfs.Metadata{Name: "trunc_part.txt", Path: "trunc_part.txt", Size: 100, Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("trunc_part.txt", vfs.Metadata{Name: "trunc_part.txt", Path: "trunc_part.txt", Size: 100, Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "trunc_part.txt")
	file := node.(*File)

	b1.On("Truncate", mock.Anything, "trunc_part.txt", int64(5)).Return(nil)
	b2.On("Truncate", mock.Anything, "trunc_part.txt", int64(5)).Return(fmt.Errorf("disk error"))
	// The generation bump only goes to the surviving backend.
	expectSidecarWrite(b1, "trunc_part.txt")

	// The wrong-length replica must be removed from the failed backend
	// asynchronously.
	removed := make(chan struct{})
	b2.On("Remove", mock.Anything, "trunc_part.txt").Return(nil).Run(func(mock.Arguments) {
		close(removed)
	})

	req := &fuse.SetattrRequest{Valid: fuse.SetattrSize, Size: 5}
	resp := &fuse.SetattrResponse{}
	err := file.Setattr(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.Equal(t, uint64(5), resp.Attr.Size)

	file.node.Mu.RLock()
	assert.Equal(t, int64(5), file.node.Meta.Size)
	assert.Equal(t, []string{"b1"}, file.node.Meta.Backends)
	file.node.Mu.RUnlock()

	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("stale replica was not removed from failed backend")
	}

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestFile_Setattr_NonSizeIsNoop(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("attrs.txt", vfs.Metadata{Name: "attrs.txt", Path: "attrs.txt", Size: 42, Mode: 0644, Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "attrs.txt")
	file := node.(*File)

	req := &fuse.SetattrRequest{Valid: fuse.SetattrMode | fuse.SetattrMtime, Mode: 0600, Mtime: time.Now()}
	resp := &fuse.SetattrResponse{}
	err := file.Setattr(context.Background(), req, resp)

	assert.NoError(t, err)
	assert.Equal(t, uint64(42), resp.Attr.Size)
	assert.Equal(t, os.FileMode(0644), resp.Attr.Mode)

	// No backend calls at all for non-size setattr.
	b1.AssertExpectations(t)
	b1.AssertNotCalled(t, "Truncate", mock.Anything, mock.Anything, mock.Anything)
}

func TestFile_Open_AppendNotSupported(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	cache.Upsert("append.txt", vfs.Metadata{Name: "append.txt", Path: "append.txt", Backends: []string{"b1"}}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "append.txt")
	file := node.(*File)

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenWriteOnly | fuse.OpenAppend}, &fuse.OpenResponse{})
	assert.ErrorIs(t, err, syscall.ENOTSUP)
	assert.Nil(t, h)

	b1.AssertNotCalled(t, "OpenFile", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestReservedPath_LookupReturnsENOENT(t *testing.T) {
	// No backend expectations: lookup of the reserved dir must not hit backends.
	b1 := &test.MockBackend{NameVal: "b1"}

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:    cache,
		Backends: map[string]backend.Backend{"b1": b1},
		Selector: vfs.NewRandomSelector(nil),
	}
	root, _ := fs.Root()
	dir := root.(*Dir)

	node, err := dir.Lookup(context.Background(), ".replistore")
	assert.ErrorIs(t, err, syscall.ENOENT)
	assert.Nil(t, node)

	b1.AssertNotCalled(t, "Stat", mock.Anything, mock.Anything)
}

func TestReservedPath_MkdirReturnsEACCES(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

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

	node, err := dir.Mkdir(context.Background(), &fuse.MkdirRequest{Name: ".replistore", Mode: os.ModeDir | 0755})
	assert.ErrorIs(t, err, syscall.EACCES)
	assert.Nil(t, node)

	b1.AssertNotCalled(t, "Mkdir", mock.Anything, mock.Anything, mock.Anything)
}

func TestReservedPath_CreateReturnsEACCES(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}

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

	node, handle, err := dir.Create(context.Background(), &fuse.CreateRequest{Name: ".replistore", Mode: 0644}, &fuse.CreateResponse{})
	assert.ErrorIs(t, err, syscall.EACCES)
	assert.Nil(t, node)
	assert.Nil(t, handle)

	b1.AssertNotCalled(t, "OpenFile", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestFile_Open_WriteBumpsGeneration(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("gen.txt", vfs.Metadata{Name: "gen.txt", Path: "gen.txt", Backends: []string{"b1", "b2"}, Gen: 3}, "b1")
	cache.Upsert("gen.txt", vfs.Metadata{Name: "gen.txt", Path: "gen.txt", Backends: []string{"b1", "b2"}, Gen: 3}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2,
		Selector:          vfs.NewFirstSelector(nil),
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)
	node, _ := dir.Lookup(context.Background(), "gen.txt")
	file := node.(*File)

	// Seed the cached generation; Upsert merge semantics are out of scope.
	file.node.Mu.Lock()
	file.node.Meta.Gen = 3
	file.node.Mu.Unlock()

	b1.On("OpenFile", mock.Anything, "gen.txt", mock.Anything, mock.Anything).Return(mockFile1, nil)
	b2.On("OpenFile", mock.Anything, "gen.txt", mock.Anything, mock.Anything).Return(mockFile2, nil)
	sc1 := expectSidecarWrite(b1, "gen.txt")
	sc2 := expectSidecarWrite(b2, "gen.txt")

	h, err := file.Open(context.Background(), &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.NoError(t, err)
	assert.NotNil(t, h)

	// One bump per write session: gen 3 -> 4, written to every handle backend.
	w1, n1 := sc1.get()
	w2, n2 := sc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(4), w1.Gen)
	assert.Equal(t, int64(4), w2.Gen)
	assert.Equal(t, "node-test", w1.Writer)

	file.node.Mu.RLock()
	assert.Equal(t, int64(4), file.node.Meta.Gen)
	file.node.Mu.RUnlock()
}

func TestRemove_WritesTombstonesToAllBackends(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	// The file's replicas live only on b1, but the tombstone must reach ALL
	// configured backends so the deletion sticks everywhere.
	cache.Upsert("kill.txt", vfs.Metadata{Name: "kill.txt", Path: "kill.txt", Backends: []string{"b1"}, Gen: 3}, "b1")
	node, _ := cache.Get("kill.txt")
	node.Mu.Lock()
	node.Meta.Gen = 3
	node.Mu.Unlock()

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1, // data removal can only succeed on b1
		Selector:          vfs.NewRandomSelector(nil),
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	tc1 := expectTombstoneWrite(b1, "kill.txt")
	tc2 := expectTombstoneWrite(b2, "kill.txt")
	b1.On("Remove", mock.Anything, "kill.txt").Return(nil)
	expectSidecarRemove(b1, "kill.txt")

	err := dir.Remove(context.Background(), &fuse.RemoveRequest{Name: "kill.txt"})
	assert.NoError(t, err)

	// Tombstones carry the deletion generation (node gen + 1) on EVERY backend.
	w1, n1 := tc1.get()
	w2, n2 := tc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(4), w1.Gen)
	assert.Equal(t, int64(4), w2.Gen)
	assert.True(t, w1.Deleted)
	assert.True(t, w2.Deleted)
	assert.Equal(t, "node-test", w1.Writer)

	_, ok := cache.Get("kill.txt")
	assert.False(t, ok)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestRemove_TombstoneQuorumFailureKeepsData(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("safe.txt", vfs.Metadata{Name: "safe.txt", Path: "safe.txt", Backends: []string{"b1", "b2"}}, "b1")
	cache.Upsert("safe.txt", vfs.Metadata{Name: "safe.txt", Path: "safe.txt", Backends: []string{"b1", "b2"}}, "b2")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       2, // Strict quorum
		Selector:          vfs.NewRandomSelector(nil),
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	// b1's tombstone write succeeds, b2's fails at the mkdir: 1/2 < quorum.
	expectTombstoneWrite(b1, "safe.txt")
	b2.On("MkdirAll", mock.Anything, gopath.Dir(vfs.TombstonePath("safe.txt")), os.FileMode(0755)).Return(fmt.Errorf("backend down"))

	err := dir.Remove(context.Background(), &fuse.RemoveRequest{Name: "safe.txt"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tombstone")

	// Without a durable deletion record, NO data may be destroyed.
	b1.AssertNotCalled(t, "Remove", mock.Anything, "safe.txt")
	b2.AssertNotCalled(t, "Remove", mock.Anything, "safe.txt")

	// The file must still be in the cache.
	_, ok := cache.Get("safe.txt")
	assert.True(t, ok)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestRename_TombstonesOldPathAndWritesFreshSidecar(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("old.txt", vfs.Metadata{Name: "old.txt", Path: "old.txt", Backends: []string{"b1"}, Gen: 3}, "b1")
	node, _ := cache.Get("old.txt")
	node.Mu.Lock()
	node.Meta.Gen = 3
	node.Mu.Unlock()

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	// The data rename only happens on the backend holding the file.
	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "old.txt", "new.txt").Return(nil)

	// Old path is tombstoned on ALL backends (best-effort), the new path gets
	// a fresh sidecar on the successful backends, and the orphaned old sidecar
	// is cleaned up.
	tc1 := expectTombstoneWrite(b1, "old.txt")
	tc2 := expectTombstoneWrite(b2, "old.txt")
	sc1 := expectSidecarWrite(b1, "new.txt")
	expectSidecarRemove(b1, "old.txt")
	// The target path carries no tombstone on either backend.
	expectNoTombstone(b1, "new.txt")
	expectNoTombstone(b2, "new.txt")

	err := dir.Rename(context.Background(), &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}, root)
	assert.NoError(t, err)

	// Tombstones at the deletion generation (gen + 1) everywhere.
	w1, n1 := tc1.get()
	w2, n2 := tc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(4), w1.Gen)
	assert.Equal(t, int64(4), w2.Gen)
	assert.True(t, w1.Deleted)
	assert.True(t, w2.Deleted)

	// Fresh sidecar at the new path starts the new lineage at gen + 1.
	ws, ns := sc1.get()
	assert.Equal(t, 1, ns)
	assert.Equal(t, int64(4), ws.Gen)
	assert.False(t, ws.Deleted)
	assert.Equal(t, "node-test", ws.Writer)

	// The data sidecar must NOT be moved by rename anymore.
	b1.AssertNotCalled(t, "Rename", mock.Anything, vfs.SidecarPath("old.txt"), vfs.SidecarPath("new.txt"))

	// The cache reflects the new path and the bumped generation.
	_, ok := cache.Get("old.txt")
	assert.False(t, ok)
	renamed, ok := cache.Get("new.txt")
	assert.True(t, ok)
	renamed.Mu.RLock()
	assert.Equal(t, int64(4), renamed.Meta.Gen)
	renamed.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

// TestRename_OntoTombstonedTargetStartsAboveTombstone: renaming onto a target
// path whose deletion is recorded by a tombstone must start the new-path
// lineage above that tombstone, while the OLD path's tombstone stays in the
// source lineage (source gen + 1).
func TestRename_OntoTombstonedTargetStartsAboveTombstone(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	cache := vfs.NewCache()
	cache.Upsert("old.txt", vfs.Metadata{Name: "old.txt", Path: "old.txt", Backends: []string{"b1"}}, "b1")
	node, _ := cache.Get("old.txt")
	node.Mu.Lock()
	node.Meta.Gen = 2 // source lineage at gen 2
	node.Mu.Unlock()

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("MkdirAll", mock.Anything, "", os.FileMode(0755)).Return(nil)
	b1.On("Rename", mock.Anything, "old.txt", "new.txt").Return(nil)

	tc1 := expectTombstoneWrite(b1, "old.txt")
	tc2 := expectTombstoneWrite(b2, "old.txt")
	sc1 := expectSidecarWrite(b1, "new.txt")
	expectSidecarRemove(b1, "old.txt")

	// The target path was previously deleted at gen 5 (tombstone on b2 only).
	expectNoTombstone(b1, "new.txt")
	expectTombstoneGen(b2, "new.txt", 5)

	err := dir.Rename(context.Background(), &fuse.RenameRequest{OldName: "old.txt", NewName: "new.txt"}, root)
	assert.NoError(t, err)

	// Old-path tombstone stays in the source lineage: source gen + 1 = 3.
	w1, n1 := tc1.get()
	w2, n2 := tc2.get()
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, int64(3), w1.Gen)
	assert.Equal(t, int64(3), w2.Gen)
	assert.True(t, w1.Deleted)
	assert.True(t, w2.Deleted)

	// New-path sidecar starts above the target tombstone: max(2, 5) + 1 = 6.
	ws, ns := sc1.get()
	assert.Equal(t, 1, ns)
	assert.Equal(t, int64(6), ws.Gen)
	assert.False(t, ws.Deleted)

	// The cache node carries the new-path generation.
	renamed, ok := cache.Get("new.txt")
	assert.True(t, ok)
	renamed.Mu.RLock()
	assert.Equal(t, int64(6), renamed.Meta.Gen)
	renamed.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestFile_OpenHandles(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	mockFile := &test.MockFile{}
	mockFile.On("Close").Return(nil).Maybe()

	cache := vfs.NewCache()
	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1},
		ReplicationFactor: 1,
		WriteQuorum:       1,
		Selector:          vfs.NewRandomSelector(nil),
		NodeID:            "node-test",
	}

	root, _ := fs.Root()
	dir := root.(*Dir)

	b1.On("OpenFile", mock.Anything, "new.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644)).Return(mockFile, nil)
	expectNoTombstone(b1, "new.txt")
	_ = expectSidecarWrite(b1, "new.txt")

	// 1. Create file: OpenHandles should become 1
	req := &fuse.CreateRequest{Name: "new.txt", Mode: 0644}
	resp := &fuse.CreateResponse{}
	node, handle, err := dir.Create(context.Background(), req, resp)

	assert.NoError(t, err)
	file := node.(*File)
	assert.Equal(t, int32(1), file.node.OpenHandles)

	// 2. Release handle: OpenHandles should become 0
	err = handle.(*FileHandle).Release(context.Background(), nil)
	assert.NoError(t, err)
	assert.Equal(t, int32(0), file.node.OpenHandles)

	// 3. Open existing file for read-only: OpenHandles should become 1
	b1.On("OpenFile", mock.Anything, "new.txt", os.O_RDONLY, os.FileMode(0)).Return(mockFile, nil)
	openReq := &fuse.OpenRequest{Flags: fuse.OpenReadOnly}
	openResp := &fuse.OpenResponse{}
	h2, err := file.Open(context.Background(), openReq, openResp)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), file.node.OpenHandles)

	// Release second handle: OpenHandles should become 0
	err = h2.(*FileHandle).Release(context.Background(), nil)
	assert.NoError(t, err)
	assert.Equal(t, int32(0), file.node.OpenHandles)

	b1.AssertExpectations(t)
}
