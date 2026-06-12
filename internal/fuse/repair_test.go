package fuse

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/test"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRepairManager_RepairNode(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	// File only on b1, RF=2
	cache.Upsert("repair.txt", vfs.Metadata{Name: "repair.txt", Path: "repair.txt", Backends: []string{"b1"}, Mode: 0644}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	mgr := NewRepairManager(fs, time.Hour, 1)

	node, _ := cache.Get("repair.txt")

	// Expecting read from b1
	b1.On("OpenFile", mock.Anything, "repair.txt", os.O_RDONLY, mock.Anything).Return(mockFile1, nil)

	data := []byte("repair data")
	mockFile1.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		buf := args.Get(1).([]byte)
		copy(buf, data)
	}).Return(len(data), io.EOF)
	mockFile1.On("Close").Return(nil)

	// Expecting write to b2
	b2.On("OpenFile", mock.Anything, "repair.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(mockFile2, nil)
	mockFile2.On("WriteAt", mock.Anything, data, int64(0)).Return(len(data), nil)
	mockFile2.On("Close").Return(nil)

	// The source replica's mtime must be preserved on the target after the copy.
	srcModTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	b1.On("Stat", mock.Anything, "repair.txt").Return(backend.FileInfo{Name: "repair.txt", Size: int64(len(data)), ModTime: srcModTime}, nil)
	b2.On("Chtimes", mock.Anything, "repair.txt", srcModTime, srcModTime).Return(nil)

	// The source's sidecar (gen 7) must be replicated to the target.
	srcSidecar := &test.MockFile{}
	scPayload := []byte(`{"v":1,"gen":7,"writer":"node-x","deleted":false}`)
	b1.On("OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_RDONLY, os.FileMode(0)).Return(srcSidecar, nil)
	srcSidecar.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), scPayload)
	}).Return(len(scPayload), io.EOF)
	srcSidecar.On("Close").Return(nil)
	scTarget := expectSidecarWrite(b2, "repair.txt")

	err := mgr.repairNode(context.Background(), node)
	assert.NoError(t, err)

	// Metadata should now include both b1 and b2
	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	node.Mu.RUnlock()

	// The target received the source's generation, not a new one.
	written, count := scTarget.get()
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(7), written.Gen)
	assert.Equal(t, "node-x", written.Writer)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	srcSidecar.AssertExpectations(t)
}

func TestRepairManager_RepairNode_ChtimesErrorStillSucceeds(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	mockFile1 := &test.MockFile{}
	mockFile2 := &test.MockFile{}

	cache := vfs.NewCache()
	cache.Upsert("repair.txt", vfs.Metadata{Name: "repair.txt", Path: "repair.txt", Backends: []string{"b1"}, Mode: 0644}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	mgr := NewRepairManager(fs, time.Hour, 1)

	node, _ := cache.Get("repair.txt")

	b1.On("OpenFile", mock.Anything, "repair.txt", os.O_RDONLY, mock.Anything).Return(mockFile1, nil)

	data := []byte("repair data")
	mockFile1.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		buf := args.Get(1).([]byte)
		copy(buf, data)
	}).Return(len(data), io.EOF)
	mockFile1.On("Close").Return(nil)

	b2.On("OpenFile", mock.Anything, "repair.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(mockFile2, nil)
	mockFile2.On("WriteAt", mock.Anything, data, int64(0)).Return(len(data), nil)
	mockFile2.On("Close").Return(nil)

	// Source Stat fails, so the cached (zero) mtime is used; Chtimes errors out.
	b1.On("Stat", mock.Anything, "repair.txt").Return(backend.FileInfo{}, io.ErrUnexpectedEOF)
	b2.On("Chtimes", mock.Anything, "repair.txt", mock.Anything, mock.Anything).Return(io.ErrUnexpectedEOF)

	// Legacy file: the source has no sidecar, so none is written to the target.
	b1.On("OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)

	err := mgr.repairNode(context.Background(), node)
	assert.NoError(t, err)

	// Repair must still count as successful: b2 is added to the backend list.
	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	node.Mu.RUnlock()

	// No sidecar write may reach the target for a legacy file.
	b2.AssertNotCalled(t, "OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644))

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestOffsetReader_Read(t *testing.T) {
	mockFile := &test.MockFile{}
	data := []byte("hello world")

	mockFile.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		buf := args.Get(1).([]byte)
		copy(buf, data[:5])
	}).Return(5, nil)

	mockFile.On("ReadAt", mock.Anything, mock.Anything, int64(5)).Run(func(args mock.Arguments) {
		buf := args.Get(1).([]byte)
		copy(buf, data[5:])
	}).Return(6, nil)

	reader := &offsetReader{ctx: context.Background(), f: mockFile}

	p1 := make([]byte, 5)
	n, err := reader.Read(p1)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("hello"), p1)

	p2 := make([]byte, 6)
	n, err = reader.Read(p2)
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte(" world"), p2)
}

func TestOffsetWriter_Write(t *testing.T) {
	mockFile := &test.MockFile{}

	mockFile.On("WriteAt", mock.Anything, []byte("hello"), int64(0)).Return(5, nil)
	mockFile.On("WriteAt", mock.Anything, []byte(" world"), int64(5)).Return(6, nil)

	writer := &offsetWriter{ctx: context.Background(), f: mockFile}

	n, err := writer.Write([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = writer.Write([]byte(" world"))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
}

// mockTombstoneTree makes b's tombstone tree enumerate (and serve) a tombstone
// at the given generation for each path; an empty map means no tombstone tree.
func mockTombstoneTree(b *test.MockBackend, tombs map[string]int64) {
	if len(tombs) == 0 {
		b.On("Walk", mock.Anything, ".replistore/tombstones", mock.Anything).Return(os.ErrNotExist)
		return
	}
	b.On("Walk", mock.Anything, ".replistore/tombstones", mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(string, backend.FileInfo) error)
		for path := range tombs {
			_ = fn(vfs.TombstonePath(path), backend.FileInfo{Name: path + ".json"})
		}
	}).Return(nil)
	for path, gen := range tombs {
		payload := []byte(fmt.Sprintf(`{"v":1,"gen":%d,"writer":"w","deleted":true}`, gen))
		f := &test.MockFile{}
		f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
			copy(args.Get(1).([]byte), payload)
		}).Return(len(payload), io.EOF)
		f.On("Close").Return(nil)
		b.On("OpenFile", mock.Anything, vfs.TombstonePath(path), os.O_RDONLY, os.FileMode(0)).Return(f, nil)
	}
}

// mockSidecarGen makes b serve a sidecar with the given generation for path.
func mockSidecarGen(b *test.MockBackend, path string, gen int64) {
	payload := []byte(fmt.Sprintf(`{"v":1,"gen":%d,"writer":"w","deleted":false}`, gen))
	f := &test.MockFile{}
	f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), payload)
	}).Return(len(payload), io.EOF)
	f.On("Close").Return(nil)
	b.On("OpenFile", mock.Anything, vfs.SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(f, nil)
}

func newTombstoneTestManager(b1, b2 *test.MockBackend) *RepairManager {
	fs := &RepliFS{
		Cache:             vfs.NewCache(),
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		WriteQuorum:       1,
		Selector:          vfs.NewFirstSelector(nil),
	}
	return NewRepairManager(fs, time.Hour, 1)
}

func TestEnforceTombstones_DeletesZombie(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mgr := newTombstoneTestManager(b1, b2)

	// b1 still holds the deleted file at gen 2; the tombstone records gen 3.
	mockTombstoneTree(b1, map[string]int64{"z.txt": 3})
	mockTombstoneTree(b2, nil)
	b1.On("Stat", mock.Anything, "z.txt").Return(backend.FileInfo{Name: "z.txt", Size: 5}, nil)
	mockSidecarGen(b1, "z.txt", 2)
	b2.On("Stat", mock.Anything, "z.txt").Return(backend.FileInfo{}, os.ErrNotExist)

	// Zombie data and meta sidecar must be removed from b1; with every backend
	// responding the now-converged tombstone is garbage-collected everywhere.
	b1.On("Remove", mock.Anything, "z.txt").Return(nil)
	b1.On("Remove", mock.Anything, vfs.SidecarPath("z.txt")).Return(nil)
	b1.On("Remove", mock.Anything, vfs.TombstonePath("z.txt")).Return(nil)
	b2.On("Remove", mock.Anything, vfs.TombstonePath("z.txt")).Return(os.ErrNotExist)

	mgr.enforceTombstones(context.Background())

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestEnforceTombstones_GCsWhenAbsentEverywhere(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mgr := newTombstoneTestManager(b1, b2)

	mockTombstoneTree(b1, map[string]int64{"gone.txt": 3})
	mockTombstoneTree(b2, map[string]int64{"gone.txt": 3})
	b1.On("Stat", mock.Anything, "gone.txt").Return(backend.FileInfo{}, os.ErrNotExist)
	b2.On("Stat", mock.Anything, "gone.txt").Return(backend.FileInfo{}, os.ErrNotExist)

	b1.On("Remove", mock.Anything, vfs.TombstonePath("gone.txt")).Return(nil)
	b2.On("Remove", mock.Anything, vfs.TombstonePath("gone.txt")).Return(nil)

	mgr.enforceTombstones(context.Background())

	// No data was present, so no data removal may happen.
	b1.AssertNotCalled(t, "Remove", mock.Anything, "gone.txt")
	b2.AssertNotCalled(t, "Remove", mock.Anything, "gone.txt")

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestEnforceTombstones_KeepsTombstoneOnBackendError(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mgr := newTombstoneTestManager(b1, b2)

	mockTombstoneTree(b1, map[string]int64{"limbo.txt": 3})
	mockTombstoneTree(b2, nil)
	b1.On("Stat", mock.Anything, "limbo.txt").Return(backend.FileInfo{}, os.ErrNotExist)
	// b2 gives no authoritative answer: the tombstone must survive, since b2
	// may still hold a zombie replica.
	b2.On("Stat", mock.Anything, "limbo.txt").Return(backend.FileInfo{}, io.ErrUnexpectedEOF)

	mgr.enforceTombstones(context.Background())

	b1.AssertNotCalled(t, "Remove", mock.Anything, vfs.TombstonePath("limbo.txt"))
	b2.AssertNotCalled(t, "Remove", mock.Anything, vfs.TombstonePath("limbo.txt"))

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestEnforceTombstones_RemovesObsoleteTombstone(t *testing.T) {
	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	mgr := newTombstoneTestManager(b1, b2)

	// b1's replica is at gen 5, newer than the recorded deletion at gen 3:
	// the tombstone is obsolete and must be retired everywhere, the data kept.
	mockTombstoneTree(b1, map[string]int64{"reborn.txt": 3})
	mockTombstoneTree(b2, nil)
	b1.On("Stat", mock.Anything, "reborn.txt").Return(backend.FileInfo{Name: "reborn.txt", Size: 9}, nil)
	mockSidecarGen(b1, "reborn.txt", 5)
	b2.On("Stat", mock.Anything, "reborn.txt").Return(backend.FileInfo{}, os.ErrNotExist)

	b1.On("Remove", mock.Anything, vfs.TombstonePath("reborn.txt")).Return(nil)
	b2.On("Remove", mock.Anything, vfs.TombstonePath("reborn.txt")).Return(os.ErrNotExist)

	mgr.enforceTombstones(context.Background())

	// The newer-generation data must not be touched.
	b1.AssertNotCalled(t, "Remove", mock.Anything, "reborn.txt")
	b1.AssertNotCalled(t, "Remove", mock.Anything, vfs.SidecarPath("reborn.txt"))

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}
