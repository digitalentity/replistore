package fuse

import (
	"context"
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
	b1.On("OpenFile", "repair.txt", os.O_RDONLY, mock.Anything).Return(mockFile1, nil)
	
	data := []byte("repair data")
	mockFile1.On("ReadAt", mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, data)
	}).Return(len(data), io.EOF)
	mockFile1.On("Close").Return(nil)

	// Expecting write to b2
	b2.On("OpenFile", "repair.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).Return(mockFile2, nil)
	mockFile2.On("WriteAt", data, int64(0)).Return(len(data), nil)
	mockFile2.On("Close").Return(nil)

	err := mgr.repairNode(context.Background(), node)
	assert.NoError(t, err)

	// Metadata should now include both b1 and b2
	node.Mu.RLock()
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	node.Mu.RUnlock()

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestOffsetReader_Read(t *testing.T) {
	mockFile := &test.MockFile{}
	data := []byte("hello world")
	
	mockFile.On("ReadAt", mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, data[:5])
	}).Return(5, nil)
	
	mockFile.On("ReadAt", mock.Anything, int64(5)).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, data[5:])
	}).Return(6, nil)

	reader := &offsetReader{f: mockFile}
	
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
	
	mockFile.On("WriteAt", []byte("hello"), int64(0)).Return(5, nil)
	mockFile.On("WriteAt", []byte(" world"), int64(5)).Return(6, nil)

	writer := &offsetWriter{f: mockFile}
	
	n, err := writer.Write([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = writer.Write([]byte(" world"))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
}
