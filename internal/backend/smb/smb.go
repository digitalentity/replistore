// Package smb implements an SMB2/3 share backend for replica storage.
package smb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/hirochachacha/go-smb2"
)

const defaultDialTimeout = 5 * time.Second

type SMBBackend struct {
	Name     string
	Address  string
	Share    string
	User     string
	Password string
	Domain   string
	Speed    int
	Tags     []string

	mu      sync.Mutex
	session *smb2.Session
	share   *smb2.Share
	conn    net.Conn
}

func NewSMBBackend(name, addr, share, user, pass, domain string, speed int, tags []string) *SMBBackend {
	return &SMBBackend{
		Name:     name,
		Address:  addr,
		Share:    share,
		User:     user,
		Password: pass,
		Domain:   domain,
		Speed:    speed,
		Tags:     tags,
	}
}

func init() {
	backend.Register("smb", func(name string, options map[string]any) (backend.Backend, error) {
		addr, _ := options["address"].(string)
		share, _ := options["share"].(string)
		user, _ := options["user"].(string)
		pass, _ := options["password"].(string)
		domain, _ := options["domain"].(string)
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

		return NewSMBBackend(name, addr, share, user, pass, domain, speed, tags), nil
	})
}

func (b *SMBBackend) GetSpeed() int {
	return b.Speed
}

func (b *SMBBackend) GetTags() []string {
	return b.Tags
}

func (b *SMBBackend) GetFreeSpace() (uint64, error) {
	share, err := b.getShare()
	if err != nil {
		return 0, err
	}
	info, err := share.Statfs(".")
	if err != nil {
		return 0, err
	}

	return info.AvailableBlockCount() * info.BlockSize(), nil
}

func (b *SMBBackend) GetName() string {
	return b.Name
}

func (b *SMBBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeLocked()

	return nil
}

func (b *SMBBackend) closeLocked() {
	if b.share != nil {
		_ = b.share.Umount()
		b.share = nil
	}
	if b.session != nil {
		_ = b.session.Logoff()
		b.session = nil
	}
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
}

func (b *SMBBackend) Connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.connectLocked()
}

//nolint:contextcheck // runs independently of request context
func (b *SMBBackend) connectLocked() error {
	b.closeLocked()

	dialer := &net.Dialer{Timeout: defaultDialTimeout}
	conn, err := dialer.DialContext(context.Background(), "tcp", b.Address)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	b.conn = conn

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     b.User,
			Password: b.Password,
			Domain:   b.Domain,
		},
	}

	s, err := d.Dial(conn)
	if err != nil {
		_ = conn.Close()
		b.conn = nil

		return fmt.Errorf("smb dial failed: %w", err)
	}
	b.session = s

	fs, err := s.Mount(b.Share)
	if err != nil {
		_ = s.Logoff()
		b.session = nil
		_ = conn.Close()
		b.conn = nil

		return fmt.Errorf("mount failed: %w", err)
	}
	b.share = fs

	return nil
}

func (b *SMBBackend) ensureConnected() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.share == nil {
		return b.connectLocked()
	}

	return nil
}

func (b *SMBBackend) getShare() (*smb2.Share, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.share == nil {
		return nil, errors.New("not connected")
	}

	return b.share, nil
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		//nolint:exhaustive // Only matching connection-related errnos
		switch errno {
		case syscall.EPIPE, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ECONNREFUSED:
			return true
		default:
		}
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "eof") ||
		strings.Contains(errStr, "closed connection") {
		return true
	}

	return false
}

func (b *SMBBackend) execute(op func() error) error {
	if err := b.ensureConnected(); err != nil {
		return err
	}

	err := op()
	if err != nil && isConnectionError(err) {
		_ = b.Close()
		if reconnectErr := b.ensureConnected(); reconnectErr != nil {
			return err
		}

		return op()
	}

	return err
}

func toSMBPath(path string) string {
	s := strings.ReplaceAll(path, "/", "\\")
	if s == "" {
		return "."
	}

	return s
}

func (b *SMBBackend) Ping(ctx context.Context) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err = share.Stat(".")

		return err
	})
}

func (b *SMBBackend) ReadDir(ctx context.Context, path string) ([]backend.FileInfo, error) {
	var results []backend.FileInfo
	err := b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)
		entries, err := share.ReadDir(smbPath)
		if err != nil {
			return err
		}
		results = nil // clear if retry
		for _, e := range entries {
			results = append(results, backend.FileInfo{
				Name:    e.Name(),
				Size:    e.Size(),
				Mode:    e.Mode(),
				ModTime: e.ModTime(),
				IsDir:   e.IsDir(),
			})
		}

		return nil
	})

	return results, err
}

func (b *SMBBackend) Stat(ctx context.Context, path string) (backend.FileInfo, error) {
	var fi backend.FileInfo
	err := b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)
		res, err := share.Stat(smbPath)
		if err != nil {
			return err
		}
		fi = backend.FileInfo{
			Name:    res.Name(),
			Size:    res.Size(),
			Mode:    res.Mode(),
			ModTime: res.ModTime(),
			IsDir:   res.IsDir(),
		}

		return nil
	})

	return fi, err
}

//nolint:ireturn // backend.File is an interface returned by implementation
func (b *SMBBackend) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (backend.File, error) {
	var file backend.File
	err := b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)
		f, err := share.OpenFile(smbPath, flag, perm)
		if err != nil {
			return err
		}
		file = &smbFile{f}

		return nil
	})

	return file, err
}

func (b *SMBBackend) Mkdir(ctx context.Context, path string, perm os.FileMode) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)

		return share.Mkdir(smbPath, perm)
	})
}

func (b *SMBBackend) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	if path == "" || path == "." {
		return nil
	}

	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)

		return share.MkdirAll(smbPath, perm)
	})
}

func (b *SMBBackend) Remove(ctx context.Context, path string) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)

		return share.Remove(smbPath)
	})
}

func (b *SMBBackend) Rename(ctx context.Context, oldPath, newPath string) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		oldSMBPath := toSMBPath(oldPath)
		newSMBPath := toSMBPath(newPath)

		return share.Rename(oldSMBPath, newSMBPath)
	})
}

func (b *SMBBackend) Chtimes(ctx context.Context, path string, atime, mtime time.Time) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)

		return share.Chtimes(smbPath, atime, mtime)
	})
}

func (b *SMBBackend) Truncate(ctx context.Context, path string, size int64) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		smbPath := toSMBPath(path)

		return share.Truncate(smbPath, size)
	})
}

func (b *SMBBackend) Walk(ctx context.Context, path string, fn func(path string, info backend.FileInfo) error) error {
	return b.execute(func() error {
		share, err := b.getShare()
		if err != nil {
			return err
		}

		return b.walk(ctx, share, path, fn)
	})
}

func (b *SMBBackend) walk(ctx context.Context, share *smb2.Share, path string, fn func(path string, info backend.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	smbPath := toSMBPath(path)
	entries, err := share.ReadDir(smbPath)
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
		fi := backend.FileInfo{
			Name:    entry.Name(),
			Size:    entry.Size(),
			Mode:    entry.Mode(),
			ModTime: entry.ModTime(),
			IsDir:   entry.IsDir(),
		}
		if err := fn(fullPath, fi); err != nil {
			return err
		}

		if entry.IsDir() {
			if err := b.walk(ctx, share, fullPath, fn); err != nil {
				return err
			}
		}
	}

	return nil
}

type smbFile struct {
	*smb2.File
}

func (f *smbFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	return f.File.ReadAt(b, off)
}

func (f *smbFile) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	return f.File.WriteAt(b, off)
}

func (f *smbFile) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return f.File.Sync()
}
