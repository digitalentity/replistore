package backend

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hirochachacha/go-smb2"
)

type FileInfo struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
	IsDir   bool
}

type File interface {
	ReadAt(b []byte, off int64) (int, error)
	WriteAt(b []byte, off int64) (int, error)
	Sync() error
	Close() error
}

type Backend interface {
	GetName() string
	Ping() error
	ReadDir(path string) ([]FileInfo, error)
	Stat(path string) (FileInfo, error)
	Walk(ctx context.Context, path string, fn func(path string, info FileInfo) error) error
	OpenFile(path string, flag int, perm os.FileMode) (File, error)
	Mkdir(path string, perm os.FileMode) error
	MkdirAll(path string, perm os.FileMode) error
	Remove(path string) error
	Rename(oldPath, newPath string) error
}

type SMBBackend struct {
	Name     string
	Address  string
	Share    string
	User     string
	Password string
	Domain   string

	session *smb2.Session
	share   *smb2.Share
	conn    net.Conn
}

func NewSMBBackend(name, addr, share, user, pass, domain string) *SMBBackend {
	return &SMBBackend{
		Name:     name,
		Address:  addr,
		Share:    share,
		User:     user,
		Password: pass,
		Domain:   domain,
	}
}

func (b *SMBBackend) GetName() string {
	return b.Name
}

func (b *SMBBackend) Ping() error {
	if b.share == nil {
		return fmt.Errorf("not connected")
	}
	_, err := b.share.Stat(".")
	return err
}

func (b *SMBBackend) Connect() error {
	conn, err := net.DialTimeout("tcp", b.Address, 5*time.Second)
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
		conn.Close()
		return fmt.Errorf("smb dial failed: %w", err)
	}
	b.session = s

	fs, err := s.Mount(b.Share)
	if err != nil {
		s.Logoff()
		conn.Close()
		return fmt.Errorf("mount failed: %w", err)
	}
	b.share = fs

	return nil
}

func (b *SMBBackend) Close() {
	if b.share != nil {
		b.share.Umount()
	}
	if b.session != nil {
		b.session.Logoff()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}

func toSMBPath(path string) string {
	s := strings.ReplaceAll(path, "/", "\\")
	if s == "" {
		return "."
	}
	return s
}

func (b *SMBBackend) ReadDir(path string) ([]FileInfo, error) {
	if b.share == nil {
		return nil, fmt.Errorf("not connected")
	}

	smbPath := toSMBPath(path)
	entries, err := b.share.ReadDir(smbPath)
	if err != nil {
		return nil, err
	}

	var results []FileInfo
	for _, e := range entries {
		results = append(results, FileInfo{
			Name:    e.Name(),
			Size:    e.Size(),
			Mode:    e.Mode(),
			ModTime: e.ModTime(),
			IsDir:   e.IsDir(),
		})
	}
	return results, nil
}

func (b *SMBBackend) Stat(path string) (FileInfo, error) {
	smbPath := toSMBPath(path)
	fi, err := b.share.Stat(smbPath)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		Name:    fi.Name(),
		Size:    fi.Size(),
		Mode:    fi.Mode(),
		ModTime: fi.ModTime(),
		IsDir:   fi.IsDir(),
	}, nil
}

func (b *SMBBackend) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	smbPath := toSMBPath(path)
	return b.share.OpenFile(smbPath, flag, perm)
}

func (b *SMBBackend) Mkdir(path string, perm os.FileMode) error {
	smbPath := toSMBPath(path)
	return b.share.Mkdir(smbPath, perm)
}

func (b *SMBBackend) MkdirAll(path string, perm os.FileMode) error {
	if path == "" || path == "." {
		return nil
	}
	
	smbPath := toSMBPath(path)
	return b.share.MkdirAll(smbPath, perm)
}

func (b *SMBBackend) Remove(path string) error {
	smbPath := toSMBPath(path)
	return b.share.Remove(smbPath)
}

func (b *SMBBackend) Rename(oldPath, newPath string) error {
	oldSMBPath := toSMBPath(oldPath)
	newSMBPath := toSMBPath(newPath)
	return b.share.Rename(oldSMBPath, newSMBPath)
}

// Walk performs a recursive scan of the backend and returns all files/folders
func (b *SMBBackend) Walk(ctx context.Context, path string, fn func(path string, info FileInfo) error) error {
	entries, err := b.ReadDir(path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fullPath := strings.TrimPrefix(path+"/"+entry.Name, "/")
		if err := fn(fullPath, entry); err != nil {
			return err
		}

		if entry.IsDir {
			if err := b.Walk(ctx, fullPath, fn); err != nil {
				return err
			}
		}
	}
	return nil
}
