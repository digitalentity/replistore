package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/digitalentity/replistore/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSidecarPath(t *testing.T) {
	assert.Equal(t, ".replistore/meta/file.txt.json", SidecarPath("file.txt"))
	assert.Equal(t, ".replistore/meta/a/b/c.dat.json", SidecarPath("a/b/c.dat"))
}

func TestSidecar_WriteReadRoundTrip(t *testing.T) {
	b := &test.MockBackend{NameVal: "b1"}
	wf := &test.MockFile{}

	var written []byte
	b.On("MkdirAll", mock.Anything, ".replistore/meta/dir", os.FileMode(0755)).Return(nil)
	b.On("OpenFile", mock.Anything, ".replistore/meta/dir/f.txt.json", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(wf, nil)
	wf.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		written = append([]byte(nil), args.Get(1).([]byte)...)
	}).Return(0, nil)
	wf.On("Close").Return(nil)

	err := WriteSidecar(context.Background(), b, "dir/f.txt", Sidecar{Gen: 42, Writer: "node-a"})
	assert.NoError(t, err)
	assert.NotEmpty(t, written)

	// The format version must be set internally.
	var raw map[string]any
	assert.NoError(t, json.Unmarshal(written, &raw))
	assert.EqualValues(t, 1, raw["v"])

	rf := &test.MockFile{}
	b.On("OpenFile", mock.Anything, ".replistore/meta/dir/f.txt.json", os.O_RDONLY, os.FileMode(0)).Return(rf, nil)
	rf.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), written)
	}).Return(len(written), io.EOF) // EOF with n>0 must be treated as success
	rf.On("Close").Return(nil)

	sc, err := ReadSidecar(context.Background(), b, "dir/f.txt")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), sc.Gen)
	assert.Equal(t, "node-a", sc.Writer)
	assert.Equal(t, 1, sc.V)
	assert.False(t, sc.Deleted)

	b.AssertExpectations(t)
	wf.AssertExpectations(t)
	rf.AssertExpectations(t)
}

func TestReadSidecar_MissingSurfacesNotExist(t *testing.T) {
	b := &test.MockBackend{NameVal: "b1"}
	b.On("OpenFile", mock.Anything, ".replistore/meta/missing.txt.json", os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)

	_, err := ReadSidecar(context.Background(), b, "missing.txt")
	assert.True(t, errors.Is(err, os.ErrNotExist))
	assert.True(t, os.IsNotExist(err))

	b.AssertExpectations(t)
}

func TestReadSidecar_BadVersionRejected(t *testing.T) {
	b := &test.MockBackend{NameVal: "b1"}
	rf := &test.MockFile{}

	payload := []byte(`{"v":2,"gen":5,"writer":"x","deleted":false}`)
	b.On("OpenFile", mock.Anything, ".replistore/meta/f.txt.json", os.O_RDONLY, os.FileMode(0)).Return(rf, nil)
	rf.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), payload)
	}).Return(len(payload), io.EOF)
	rf.On("Close").Return(nil)

	_, err := ReadSidecar(context.Background(), b, "f.txt")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported format version")

	b.AssertExpectations(t)
	rf.AssertExpectations(t)
}

func TestRemoveSidecar_NotExistIsSuccess(t *testing.T) {
	b := &test.MockBackend{NameVal: "b1"}
	b.On("Remove", mock.Anything, ".replistore/meta/gone.txt.json").Return(os.ErrNotExist)

	assert.NoError(t, RemoveSidecar(context.Background(), b, "gone.txt"))
	b.AssertExpectations(t)
}

func TestRemoveSidecar_PropagatesOtherErrors(t *testing.T) {
	b := &test.MockBackend{NameVal: "b1"}
	bErr := errors.New("backend down")
	b.On("Remove", mock.Anything, ".replistore/meta/f.txt.json").Return(bErr)

	assert.ErrorIs(t, RemoveSidecar(context.Background(), b, "f.txt"), bErr)
	b.AssertExpectations(t)
}
