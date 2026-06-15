// Package backend defines the interfaces and shared types for replica backends.
package backend

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type FileInfo struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
	IsDir   bool
}

type File interface {
	ReadAt(ctx context.Context, b []byte, off int64) (int, error)
	WriteAt(ctx context.Context, b []byte, off int64) (int, error)
	Sync(ctx context.Context) error
	Close() error
}

type Backend interface {
	GetName() string
	GetSpeed() int
	GetTags() []string
	GetFreeSpace() (uint64, error)
	Connect() error
	Ping(ctx context.Context) error
	ReadDir(ctx context.Context, path string) ([]FileInfo, error)
	Stat(ctx context.Context, path string) (FileInfo, error)
	Walk(ctx context.Context, path string, fn func(path string, info FileInfo) error) error
	OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (File, error)
	Mkdir(ctx context.Context, path string, perm os.FileMode) error
	MkdirAll(ctx context.Context, path string, perm os.FileMode) error
	Remove(ctx context.Context, path string) error
	Rename(ctx context.Context, oldPath, newPath string) error
	Chtimes(ctx context.Context, path string, atime, mtime time.Time) error
	Truncate(ctx context.Context, path string, size int64) error
	// Close releases any connection/session resources held by the backend.
	// It is safe to call on an already-closed backend.
	Close() error
}

type Factory func(name string, options map[string]interface{}) (Backend, error)

var (
	factoriesMu sync.RWMutex
	factories   = map[string]Factory{}
)

func Register(typeName string, factory Factory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	factories[strings.ToLower(typeName)] = factory
}

func Create(typeName string, name string, options map[string]interface{}) (Backend, error) {
	factoriesMu.RLock()
	factory, ok := factories[strings.ToLower(typeName)]
	factoriesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown backend type: %s", typeName)
	}
	return factory(name, options)
}
