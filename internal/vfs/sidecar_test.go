package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	gopath "path"
	"strings"
	"testing"

	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMetaPath(t *testing.T) {
	// The key is the SHA-256 of the data path, hex-encoded, sharded two levels
	// deep under meta/. It is deterministic and lives under the reserved tree.
	p := MetaPath("dir/f.txt")
	assert.Equal(t, p, MetaPath("dir/f.txt"), "must be deterministic")
	assert.True(t, strings.HasPrefix(p, metaDir+"/"))
	assert.True(t, strings.HasSuffix(p, ".json"))

	// Layout: meta/<h0>/<h1>/<64hex>.json — two 2-char shard levels.
	rel := strings.TrimSuffix(strings.TrimPrefix(p, metaDir+"/"), ".json")
	parts := strings.Split(rel, "/")
	assert.Len(t, parts, 3)
	assert.Len(t, parts[0], 2)
	assert.Len(t, parts[1], 2)
	assert.Len(t, parts[2], 64)
	assert.Equal(t, parts[2][0:2], parts[0])
	assert.Equal(t, parts[2][2:4], parts[1])

	// Distinct paths map to distinct keys.
	assert.NotEqual(t, MetaPath("a"), MetaPath("b"))
}

func TestSidecar_WriteReadRoundTrip(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	wf := &bmock.MockFile{}

	key := MetaPath("dir/f.txt")

	var written []byte
	b.On("MkdirAll", mock.Anything, gopath.Dir(key), os.FileMode(0755)).Return(nil)
	b.On("OpenFile", mock.Anything, key, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(wf, nil)
	wf.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		written = append([]byte(nil), args.Get(1).([]byte)...)
	}).Return(0, nil)
	wf.On("Close").Return(nil)

	err := WriteSidecar(context.Background(), b, "dir/f.txt", Sidecar{DataGen: 42, Writer: "node-a"})
	require.NoError(t, err)
	assert.NotEmpty(t, written)

	// The format version and data path must be stamped in internally.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(written, &raw))
	assert.EqualValues(t, 1, raw["v"])
	assert.Equal(t, "dir/f.txt", raw["path"])
	assert.Equal(t, false, raw["deleted"])

	rf := &bmock.MockFile{}
	b.On("OpenFile", mock.Anything, key, os.O_RDONLY, os.FileMode(0)).Return(rf, nil)
	rf.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), written)
	}).Return(len(written), io.EOF) // EOF with n>0 must be treated as success
	rf.On("Close").Return(nil)

	sc, err := ReadMeta(context.Background(), b, "dir/f.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(42), sc.DataGen)
	assert.Equal(t, "node-a", sc.Writer)
	assert.Equal(t, "dir/f.txt", sc.Path)
	assert.Equal(t, 1, sc.V)
	assert.False(t, sc.Deleted)

	b.AssertExpectations(t)
	wf.AssertExpectations(t)
	rf.AssertExpectations(t)
}

func TestReadMeta_MissingSurfacesNotExist(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	b.On("OpenFile", mock.Anything, MetaPath("missing.txt"), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)

	_, err := ReadMeta(context.Background(), b, "missing.txt")
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.True(t, os.IsNotExist(err))

	b.AssertExpectations(t)
}

func TestReadMeta_BadVersionRejected(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	rf := &bmock.MockFile{}

	payload := []byte(`{"v":2,"data_gen":5,"writer":"x","deleted":false}`)
	b.On("OpenFile", mock.Anything, MetaPath("f.txt"), os.O_RDONLY, os.FileMode(0)).Return(rf, nil)
	rf.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), payload)
	}).Return(len(payload), io.EOF)
	rf.On("Close").Return(nil)

	_, err := ReadMeta(context.Background(), b, "f.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported format version")

	b.AssertExpectations(t)
	rf.AssertExpectations(t)
}

func TestRemoveMeta_NotExistIsSuccess(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	b.On("Remove", mock.Anything, MetaPath("gone.txt")).Return(os.ErrNotExist)

	require.NoError(t, RemoveMeta(context.Background(), b, "gone.txt"))
	b.AssertExpectations(t)
}

func TestRemoveMeta_PropagatesOtherErrors(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	bErr := errors.New("backend down")
	b.On("Remove", mock.Anything, MetaPath("f.txt")).Return(bErr)

	require.ErrorIs(t, RemoveMeta(context.Background(), b, "f.txt"), bErr)
	b.AssertExpectations(t)
}

// TestTombstone_SharesKeyWithSidecar verifies a tombstone is the same document
// as the sidecar (same key) with Deleted forced true.
func TestTombstone_SharesKeyWithSidecar(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	wf := &bmock.MockFile{}

	key := MetaPath("dir/f.txt")
	assert.Equal(t, key, MetaPath("dir/f.txt"), "sidecar and tombstone share one key")

	var written []byte
	b.On("MkdirAll", mock.Anything, gopath.Dir(key), os.FileMode(0755)).Return(nil)
	b.On("OpenFile", mock.Anything, key, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(wf, nil)
	wf.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		written = append([]byte(nil), args.Get(1).([]byte)...)
	}).Return(0, nil)
	wf.On("Close").Return(nil)

	// Deleted is deliberately left false: WriteTombstone must force it true.
	err := WriteTombstone(context.Background(), b, "dir/f.txt", Sidecar{DataGen: 7, Writer: "node-a"})
	assert.NoError(t, err)

	var raw map[string]any
	assert.NoError(t, json.Unmarshal(written, &raw))
	assert.EqualValues(t, 1, raw["v"])
	assert.Equal(t, true, raw["deleted"])
	assert.Equal(t, "dir/f.txt", raw["path"])

	b.AssertExpectations(t)
	wf.AssertExpectations(t)
}

// TestWriteSidecar_ClearsDeleted verifies a live write supersedes a prior
// tombstone: WriteSidecar must force Deleted false even if the caller passes it
// true.
func TestWriteSidecar_ClearsDeleted(t *testing.T) {
	b := &bmock.MockBackend{NameVal: "b1"}
	wf := &bmock.MockFile{}

	var written []byte
	b.On("MkdirAll", mock.Anything, mock.Anything, os.FileMode(0755)).Return(nil)
	b.On("OpenFile", mock.Anything, MetaPath("f.txt"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(wf, nil)
	wf.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		written = append([]byte(nil), args.Get(1).([]byte)...)
	}).Return(0, nil)
	wf.On("Close").Return(nil)

	err := WriteSidecar(context.Background(), b, "f.txt", Sidecar{DataGen: 9, Deleted: true})
	assert.NoError(t, err)

	var raw map[string]any
	assert.NoError(t, json.Unmarshal(written, &raw))
	assert.Equal(t, false, raw["deleted"])
}
