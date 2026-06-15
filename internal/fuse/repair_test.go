package fuse

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRepairManager_RepairNode(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	mockFile1 := &bmock.MockFile{}
	mockFile2 := &bmock.MockFile{}

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
	srcSidecar := &bmock.MockFile{}
	scPayload := []byte(`{"v":1,"gen":7,"writer":"node-x","deleted":false}`)
	b1.On("OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_RDONLY, os.FileMode(0)).Return(srcSidecar, nil)
	srcSidecar.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), scPayload)
	}).Return(len(scPayload), io.EOF)
	srcSidecar.On("Close").Return(nil)
	scTarget := expectSidecarWrite(b2, "repair.txt")
	// The source's sidecar had no content sum, so repair records the sum of
	// the bytes it just read on the source as well.
	scSource := expectSidecarWrite(b1, "repair.txt")

	err := mgr.repairNode(context.Background(), node)
	assert.NoError(t, err)

	// Metadata should now include both b1 and b2
	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	node.Mu.RUnlock()

	// The target received the source's generation, not a new one, plus the
	// content sum of the copied bytes.
	wantSum := "sha256:" + fmt.Sprintf("%x", sha256.Sum256(data))
	written, count := scTarget.get()
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(7), written.Gen)
	assert.Equal(t, "node-x", written.Writer)
	assert.Equal(t, wantSum, written.Sum)

	// The source's sidecar was updated with the same sum (it was empty).
	srcWritten, srcCount := scSource.get()
	assert.Equal(t, 1, srcCount)
	assert.Equal(t, int64(7), srcWritten.Gen)
	assert.Equal(t, wantSum, srcWritten.Sum)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	srcSidecar.AssertExpectations(t)
}

func TestRepairManager_RepairNode_ChtimesErrorStillSucceeds(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	mockFile1 := &bmock.MockFile{}
	mockFile2 := &bmock.MockFile{}

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

	// No sidecar write may reach the target for a legacy file, and no content
	// sum is recorded on the source either (there is no sidecar to put it in).
	b2.AssertNotCalled(t, "OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644))
	b1.AssertNotCalled(t, "OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644))

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestRepairManager_RepairNode_DetectsDivergentReplicas(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}

	mockFile1 := &bmock.MockFile{}
	mockFile2 := &bmock.MockFile{}

	cache := vfs.NewCache()
	// The cache only knows about the b1 replica, but b2 also holds the file
	// (a stale same-generation replica with different content).
	cache.Upsert("repair.txt", vfs.Metadata{Name: "repair.txt", Path: "repair.txt", Backends: []string{"b1"}, Mode: 0644}, "b1")

	fs := &RepliFS{
		Cache:             cache,
		Backends:          map[string]backend.Backend{"b1": b1, "b2": b2},
		ReplicationFactor: 2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	mgr := NewRepairManager(fs, time.Hour, 1)

	node, _ := cache.Get("repair.txt")

	data := []byte("repair data")
	srcSum := "sha256:" + fmt.Sprintf("%x", sha256.Sum256(data))
	divergentSum := "sha256:" + fmt.Sprintf("%x", sha256.Sum256([]byte("different content")))

	b1.On("OpenFile", mock.Anything, "repair.txt", os.O_RDONLY, mock.Anything).Return(mockFile1, nil)
	mockFile1.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), data)
	}).Return(len(data), io.EOF)
	mockFile1.On("Close").Return(nil)

	// Source sidecar: gen 7 with the (matching) content sum.
	srcSidecar := &bmock.MockFile{}
	srcPayload := []byte(fmt.Sprintf(`{"v":1,"gen":7,"writer":"node-x","deleted":false,"sum":"%s"}`, srcSum))
	b1.On("OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_RDONLY, os.FileMode(0)).Return(srcSidecar, nil)
	srcSidecar.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), srcPayload)
	}).Return(len(srcPayload), io.EOF)
	srcSidecar.On("Close").Return(nil)

	// Target already holds the file at the SAME generation with a DIFFERENT
	// non-empty sum: divergence must be detected before the overwrite.
	b2.On("Stat", mock.Anything, "repair.txt").Return(backend.FileInfo{Name: "repair.txt", Size: 17}, nil)
	tgtSidecar := &bmock.MockFile{}
	tgtPayload := []byte(fmt.Sprintf(`{"v":1,"gen":7,"writer":"node-y","deleted":false,"sum":"%s"}`, divergentSum))
	b2.On("OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_RDONLY, os.FileMode(0)).Return(tgtSidecar, nil)
	tgtSidecar.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), tgtPayload)
	}).Return(len(tgtPayload), io.EOF)
	tgtSidecar.On("Close").Return(nil)

	// Repair still overwrites the target from the source.
	b2.On("OpenFile", mock.Anything, "repair.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(mockFile2, nil)
	mockFile2.On("WriteAt", mock.Anything, data, int64(0)).Return(len(data), nil)
	mockFile2.On("Close").Return(nil)

	srcModTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	b1.On("Stat", mock.Anything, "repair.txt").Return(backend.FileInfo{Name: "repair.txt", Size: int64(len(data)), ModTime: srcModTime}, nil)
	b2.On("Chtimes", mock.Anything, "repair.txt", srcModTime, srcModTime).Return(nil)

	scTarget := expectSidecarWrite(b2, "repair.txt")

	err := mgr.repairNode(context.Background(), node)
	assert.NoError(t, err)

	// The divergence was counted exactly once.
	assert.Equal(t, int64(1), atomic.LoadInt64(&mgr.divergenceCount))

	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	node.Mu.RUnlock()

	// The target's sidecar now carries the source's generation and sum.
	written, count := scTarget.get()
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(7), written.Gen)
	assert.Equal(t, srcSum, written.Sum)

	// The source's sum already matched what was read, so it is not rewritten.
	b1.AssertNotCalled(t, "OpenFile", mock.Anything, vfs.SidecarPath("repair.txt"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644))

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	srcSidecar.AssertExpectations(t)
	tgtSidecar.AssertExpectations(t)
}

func TestOffsetReader_Read(t *testing.T) {
	mockFile := &bmock.MockFile{}
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
	mockFile := &bmock.MockFile{}

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
func mockTombstoneTree(b *bmock.MockBackend, tombs map[string]int64) {
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
		f := &bmock.MockFile{}
		f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
			copy(args.Get(1).([]byte), payload)
		}).Return(len(payload), io.EOF)
		f.On("Close").Return(nil)
		b.On("OpenFile", mock.Anything, vfs.TombstonePath(path), os.O_RDONLY, os.FileMode(0)).Return(f, nil)
	}
}

// mockSidecarGen makes b serve a sidecar with the given generation for path.
func mockSidecarGen(b *bmock.MockBackend, path string, gen int64) {
	payload := []byte(fmt.Sprintf(`{"v":1,"gen":%d,"writer":"w","deleted":false}`, gen))
	f := &bmock.MockFile{}
	f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), payload)
	}).Return(len(payload), io.EOF)
	f.On("Close").Return(nil)
	b.On("OpenFile", mock.Anything, vfs.SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(f, nil)
}

func newTombstoneTestManager(b1, b2 *bmock.MockBackend) *RepairManager {
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
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
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
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
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
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
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
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
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

func TestRepairManager_PruneNode(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	b3 := &bmock.MockBackend{NameVal: "b3"}

	cache := vfs.NewCache()
	cache.Upsert("prune.txt", vfs.Metadata{Name: "prune.txt", Path: "prune.txt", Backends: []string{"b1", "b2", "b3"}, Mode: 0644}, "b1")
	cache.Upsert("prune.txt", vfs.Metadata{Name: "prune.txt", Path: "prune.txt", Backends: []string{"b1", "b2", "b3"}, Mode: 0644}, "b2")
	cache.Upsert("prune.txt", vfs.Metadata{Name: "prune.txt", Path: "prune.txt", Backends: []string{"b1", "b2", "b3"}, Mode: 0644}, "b3")

	backends := map[string]backend.Backend{"b1": b1, "b2": b2, "b3": b3}

	fs := &RepliFS{
		Cache:             cache,
		Backends:          backends,
		ReplicationFactor: 2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	mgr := NewRepairManager(fs, time.Hour, 1)

	node, _ := cache.Get("prune.txt")

	b3.On("Remove", mock.Anything, "prune.txt").Return(nil)
	b3.On("Remove", mock.Anything, vfs.SidecarPath("prune.txt")).Return(nil)

	err := mgr.pruneNode(context.Background(), node)
	assert.NoError(t, err)

	b1.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
	b2.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)

	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
	b3.AssertExpectations(t)
}

func TestRepairManager_PruneNode_FailureKeepsMetadata(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b2 := &bmock.MockBackend{NameVal: "b2"}
	b3 := &bmock.MockBackend{NameVal: "b3"}

	cache := vfs.NewCache()
	cache.Upsert("prune.txt", vfs.Metadata{Name: "prune.txt", Path: "prune.txt", Backends: []string{"b1", "b2", "b3"}, Mode: 0644}, "b1")
	cache.Upsert("prune.txt", vfs.Metadata{Name: "prune.txt", Path: "prune.txt", Backends: []string{"b1", "b2", "b3"}, Mode: 0644}, "b2")
	cache.Upsert("prune.txt", vfs.Metadata{Name: "prune.txt", Path: "prune.txt", Backends: []string{"b1", "b2", "b3"}, Mode: 0644}, "b3")

	backends := map[string]backend.Backend{"b1": b1, "b2": b2, "b3": b3}

	fs := &RepliFS{
		Cache:             cache,
		Backends:          backends,
		ReplicationFactor: 2,
		Selector:          vfs.NewFirstSelector(nil),
	}

	mgr := NewRepairManager(fs, time.Hour, 1)

	node, _ := cache.Get("prune.txt")

	b3.On("Remove", mock.Anything, "prune.txt").Return(io.ErrUnexpectedEOF)

	err := mgr.pruneNode(context.Background(), node)
	assert.NoError(t, err)

	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2", "b3"}, node.Meta.Backends)
	node.Mu.RUnlock()

	b3.AssertExpectations(t)
}
