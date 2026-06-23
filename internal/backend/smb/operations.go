package smb

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"go.kvsh.ch/smb2"
)

// spaceQueryTimeout bounds the Statfs round-trip behind GetFreeSpace and
// GetTotalSpace. These methods carry no caller context (the Backend interface
// omits one), so a hung share would otherwise block the metrics scrape forever.
const spaceQueryTimeout = 5 * time.Second

func toSMBPath(p string) string {
	// Anchor and clean so ".." segments cannot escape the share root, mirroring
	// the local backend's resolve. SMB paths are share-relative, so the leading
	// slash is dropped after cleaning and separators are converted to backslash.
	cleaned := path.Clean("/" + strings.ReplaceAll(p, "\\", "/"))
	cleaned = strings.TrimPrefix(cleaned, "/")
	s := strings.ReplaceAll(cleaned, "/", "\\")
	if s == "" {
		return "."
	}

	return s
}

func toFileInfo(fi os.FileInfo) backend.FileInfo {
	return backend.FileInfo{
		Name:    fi.Name(),
		Size:    fi.Size(),
		Mode:    fi.Mode(),
		ModTime: fi.ModTime(),
		IsDir:   fi.IsDir(),
	}
}

func (b *SMBBackend) GetFreeSpace() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), spaceQueryTimeout)
	defer cancel()
	var free uint64
	err := b.execute(ctx, func(share *smb2.Share) error {
		info, err := share.Statfs(".")
		if err != nil {
			return err
		}
		free = info.AvailableBlockCount() * info.BlockSize()

		return nil
	})

	return free, err
}

func (b *SMBBackend) GetTotalSpace() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), spaceQueryTimeout)
	defer cancel()
	var total uint64
	err := b.execute(ctx, func(share *smb2.Share) error {
		info, err := share.Statfs(".")
		if err != nil {
			return err
		}
		total = info.TotalBlockCount() * info.BlockSize()

		return nil
	})

	return total, err
}

func (b *SMBBackend) Ping(ctx context.Context) error {
	return b.execute(ctx, func(share *smb2.Share) error {
		_, err := share.Stat(".")

		return err
	})
}

func (b *SMBBackend) ReadDir(ctx context.Context, path string) ([]backend.FileInfo, error) {
	var results []backend.FileInfo
	err := b.execute(ctx, func(share *smb2.Share) error {
		entries, err := share.ReadDir(toSMBPath(path))
		if err != nil {
			return err
		}
		results = make([]backend.FileInfo, 0, len(entries))
		for _, e := range entries {
			results = append(results, toFileInfo(e))
		}

		return nil
	})

	return results, err
}

func (b *SMBBackend) Stat(ctx context.Context, path string) (backend.FileInfo, error) {
	var info backend.FileInfo
	err := b.execute(ctx, func(share *smb2.Share) error {
		res, err := share.Stat(toSMBPath(path))
		if err != nil {
			return err
		}
		info = toFileInfo(res)

		return nil
	})

	return info, err
}

//nolint:ireturn // backend.File is an interface returned by implementation
func (b *SMBBackend) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (backend.File, error) {
	var f *smb2.File
	err := b.execute(ctx, func(share *smb2.Share) error {
		// share is already bound to ctx via getShare. Each later call on the
		// returned handle rebinds to its own caller ctx via File.WithContext, so
		// the open ctx need not outlive this call.
		var err error
		f, err = share.OpenFile(toSMBPath(path), flag, perm)

		return err
	})
	if err != nil {
		return nil, err
	}

	return &smbFile{File: f}, nil
}

func (b *SMBBackend) Mkdir(ctx context.Context, path string, perm os.FileMode) error {
	return b.execute(ctx, func(share *smb2.Share) error {
		return share.Mkdir(toSMBPath(path), perm)
	})
}

func (b *SMBBackend) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	if path == "" || path == "." {
		return nil
	}

	return b.execute(ctx, func(share *smb2.Share) error {
		return share.MkdirAll(toSMBPath(path), perm)
	})
}

func (b *SMBBackend) Remove(ctx context.Context, path string) error {
	return b.execute(ctx, func(share *smb2.Share) error {
		return share.Remove(toSMBPath(path))
	})
}

func (b *SMBBackend) Rename(ctx context.Context, oldPath, newPath string) error {
	return b.execute(ctx, func(share *smb2.Share) error {
		return share.Rename(toSMBPath(oldPath), toSMBPath(newPath))
	})
}

func (b *SMBBackend) Chtimes(ctx context.Context, path string, atime, mtime time.Time) error {
	return b.execute(ctx, func(share *smb2.Share) error {
		return share.Chtimes(toSMBPath(path), atime, mtime)
	})
}

func (b *SMBBackend) Truncate(ctx context.Context, path string, size int64) error {
	return b.execute(ctx, func(share *smb2.Share) error {
		return share.Truncate(toSMBPath(path), size)
	})
}

func (b *SMBBackend) Walk(ctx context.Context, path string, fn func(path string, info backend.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var entries []os.FileInfo
	err := b.execute(ctx, func(share *smb2.Share) error {
		var err error
		entries, err = share.ReadDir(toSMBPath(path))

		return err
	})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fullPath := strings.TrimPrefix(path+"/"+entry.Name(), "/")
		if err := fn(fullPath, toFileInfo(entry)); err != nil {
			return err
		}

		if entry.IsDir() {
			if err := b.Walk(ctx, fullPath, fn); err != nil {
				return err
			}
		}
	}

	return nil
}
