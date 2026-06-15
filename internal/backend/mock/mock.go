package mock

import (
	"context"
	"os"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/stretchr/testify/mock"
)

type MockBackend struct {
	mock.Mock
	NameVal      string
	SpeedVal     int
	TagsVal      []string
	FreeSpaceVal uint64
}

func (m *MockBackend) GetName() string {
	return m.NameVal
}

func (m *MockBackend) GetSpeed() int {
	return m.SpeedVal
}

func (m *MockBackend) GetTags() []string {
	return m.TagsVal
}

func (m *MockBackend) GetFreeSpace() (uint64, error) {
	args := m.Called()
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockBackend) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBackend) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBackend) ReadDir(ctx context.Context, path string) ([]backend.FileInfo, error) {
	args := m.Called(ctx, path)
	return args.Get(0).([]backend.FileInfo), args.Error(1)
}

func (m *MockBackend) Stat(ctx context.Context, path string) (backend.FileInfo, error) {
	args := m.Called(ctx, path)
	return args.Get(0).(backend.FileInfo), args.Error(1)
}

func (m *MockBackend) Walk(ctx context.Context, path string, fn func(path string, info backend.FileInfo) error) error {
	args := m.Called(ctx, path, fn)
	return args.Error(0)
}

func (m *MockBackend) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (backend.File, error) {
	args := m.Called(ctx, path, flag, perm)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(backend.File), args.Error(1)
}

func (m *MockBackend) Mkdir(ctx context.Context, path string, perm os.FileMode) error {
	args := m.Called(ctx, path, perm)
	return args.Error(0)
}

func (m *MockBackend) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	args := m.Called(ctx, path, perm)
	return args.Error(0)
}

func (m *MockBackend) Remove(ctx context.Context, path string) error {
	args := m.Called(ctx, path)
	return args.Error(0)
}

func (m *MockBackend) Rename(ctx context.Context, oldPath, newPath string) error {
	args := m.Called(ctx, oldPath, newPath)
	return args.Error(0)
}

func (m *MockBackend) Chtimes(ctx context.Context, path string, atime, mtime time.Time) error {
	args := m.Called(ctx, path, atime, mtime)
	return args.Error(0)
}

func (m *MockBackend) Truncate(ctx context.Context, path string, size int64) error {
	args := m.Called(ctx, path, size)
	return args.Error(0)
}

type MockFile struct {
	mock.Mock
}

func (m *MockFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	args := m.Called(ctx, b, off)
	return args.Int(0), args.Error(1)
}

func (m *MockFile) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	args := m.Called(ctx, b, off)
	return args.Int(0), args.Error(1)
}

func (m *MockFile) Sync(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockFile) Close() error {
	args := m.Called()
	return args.Error(0)
}
