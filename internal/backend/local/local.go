// Package local implements a local directory backend for replica storage.
package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
)

type LocalBackend struct {
	Name  string
	Path  string
	Speed int
	Tags  []string
}

func NewLocalBackend(name, path string, speed int, tags []string) *LocalBackend {
	return &LocalBackend{
		Name:  name,
		Path:  path,
		Speed: speed,
		Tags:  tags,
	}
}

func init() {
	backend.Register("local", func(name string, options map[string]any) (backend.Backend, error) {
		path, _ := options["path"].(string)
		if path == "" {
			return nil, errors.New("local backend requires 'path' option")
		}
		speed := 10
		if speedVal, ok := options["speed"].(float64); ok {
			speed = int(speedVal)
		} else if speedVal, ok := options["speed"].(int); ok {
			speed = speedVal
		}
		var tags []string
		if tList, ok := options["tags"].([]any); ok {
			for _, t := range tList {
				if s, ok := t.(string); ok {
					tags = append(tags, s)
				}
			}
		} else if tList, ok := options["tags"].([]string); ok {
			tags = tList
		}

		return NewLocalBackend(name, path, speed, tags), nil
	})
}

func (b *LocalBackend) GetSpeed() int {
	return b.Speed
}

func (b *LocalBackend) GetTags() []string {
	return b.Tags
}

func (b *LocalBackend) GetFreeSpace() (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(b.Path, &stat); err != nil {
		return 0, err
	}

	if stat.Bsize < 0 {
		return 0, fmt.Errorf("local backend: negative block size %d", stat.Bsize)
	}

	return stat.Bavail * uint64(stat.Bsize), nil //nolint:gosec // checked non-negative
}

func (b *LocalBackend) GetName() string {
	return b.Name
}

// Close is a no-op: the local backend holds no persistent connection.
func (b *LocalBackend) Close() error {
	return nil
}

func (b *LocalBackend) Connect() error {
	fi, err := os.Stat(b.Path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(b.Path, 0750); err != nil {
				return fmt.Errorf("local backend root path %q does not exist and could not be created: %w", b.Path, err)
			}

			return nil
		}

		return fmt.Errorf("local backend root path %q access error: %w", b.Path, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("local backend root path %q is not a directory", b.Path)
	}

	return nil
}

func (b *LocalBackend) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := os.Stat(b.Path)

	return err
}

func (b *LocalBackend) resolve(subPath string) string {
	subPath = strings.ReplaceAll(subPath, "\\", "/")
	cleaned := filepath.Clean("/" + subPath)

	return filepath.Join(b.Path, cleaned)
}

func (b *LocalBackend) ReadDir(ctx context.Context, path string) ([]backend.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	realPath := b.resolve(path)
	entries, err := os.ReadDir(realPath)
	if err != nil {
		return nil, err
	}
	results := []backend.FileInfo{}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		results = append(results, backend.FileInfo{
			Name:    info.Name(),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		})
	}

	return results, nil
}

func (b *LocalBackend) Stat(ctx context.Context, path string) (backend.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return backend.FileInfo{}, err
	}
	realPath := b.resolve(path)
	info, err := os.Stat(realPath)
	if err != nil {
		return backend.FileInfo{}, err
	}

	return backend.FileInfo{
		Name:    info.Name(),
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
	}, nil
}

func (b *LocalBackend) Walk(ctx context.Context, path string, fn func(path string, info backend.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realPath := b.resolve(path)

	return filepath.WalkDir(realPath, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(b.Path, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || rel == "" {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		return fn(rel, backend.FileInfo{
			Name:    info.Name(),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		})
	})
}

//nolint:ireturn // backend.File is an interface returned by implementation
func (b *LocalBackend) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (backend.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	realPath := b.resolve(path)
	f, err := os.OpenFile(realPath, flag, perm) //nolint:gosec // G304: path is resolved inside local backend root
	if err != nil {
		return nil, err
	}

	return &localFile{f}, nil
}

func (b *LocalBackend) Mkdir(ctx context.Context, path string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realPath := b.resolve(path)

	return os.Mkdir(realPath, perm)
}

func (b *LocalBackend) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realPath := b.resolve(path)

	return os.MkdirAll(realPath, perm)
}

func (b *LocalBackend) Remove(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realPath := b.resolve(path)

	return os.Remove(realPath)
}

func (b *LocalBackend) Rename(ctx context.Context, oldPath, newPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	oldRealPath := b.resolve(oldPath)
	newRealPath := b.resolve(newPath)

	return os.Rename(oldRealPath, newRealPath)
}

func (b *LocalBackend) Chtimes(ctx context.Context, path string, atime, mtime time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realPath := b.resolve(path)

	return os.Chtimes(realPath, atime, mtime)
}

func (b *LocalBackend) Truncate(ctx context.Context, path string, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realPath := b.resolve(path)

	return os.Truncate(realPath, size)
}

type localFile struct {
	*os.File
}

func (f *localFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	return f.File.ReadAt(b, off)
}

func (f *localFile) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	return f.File.WriteAt(b, off)
}

func (f *localFile) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return f.File.Sync()
}

func (f *localFile) Close() error {
	return f.File.Close()
}
