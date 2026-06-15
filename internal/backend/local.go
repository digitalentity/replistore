package backend

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type LocalBackend struct {
	Name string
	Path string
}

func NewLocalBackend(name, path string) *LocalBackend {
	return &LocalBackend{
		Name: name,
		Path: path,
	}
}

func init() {
	Register("local", func(name string, options map[string]interface{}) (Backend, error) {
		path, _ := options["path"].(string)
		if path == "" {
			return nil, fmt.Errorf("local backend requires 'path' option")
		}
		return NewLocalBackend(name, path), nil
	})
}

func (b *LocalBackend) GetName() string {
	return b.Name
}

func (b *LocalBackend) Connect() error {
	fi, err := os.Stat(b.Path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(b.Path, 0755); err != nil {
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

func (b *LocalBackend) ReadDir(ctx context.Context, path string) ([]FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	realPath := b.resolve(path)
	entries, err := os.ReadDir(realPath)
	if err != nil {
		return nil, err
	}
	results := []FileInfo{}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		results = append(results, FileInfo{
			Name:    info.Name(),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		})
	}
	return results, nil
}

func (b *LocalBackend) Stat(ctx context.Context, path string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	realPath := b.resolve(path)
	info, err := os.Stat(realPath)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		Name:    info.Name(),
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
	}, nil
}

func (b *LocalBackend) Walk(ctx context.Context, path string, fn func(path string, info FileInfo) error) error {
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

		return fn(rel, FileInfo{
			Name:    info.Name(),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		})
	})
}

func (b *LocalBackend) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	realPath := b.resolve(path)
	f, err := os.OpenFile(realPath, flag, perm)
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
