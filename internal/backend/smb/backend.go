// Package smb implements an SMB2/3 share backend for replica storage.
package smb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"go.kvsh.ch/smb2"
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

func (b *SMBBackend) GetAddress() string {
	return b.Address
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

func (b *SMBBackend) Connect(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.connectLocked(ctx)
}

// connectLocked establishes a fresh connection, session and share, bounding the
// whole handshake (TCP dial, SMB negotiate/session-setup and tree connect) by
// ctx so a caller's deadline is honored instead of stalling for the dial
// timeout. The resulting session and share are rebound to context.Background so
// their lifetime — and later Umount/Logoff and per-call I/O — does not inherit a
// caller deadline that has since expired. Callers must hold b.mu.
//
//nolint:contextcheck // the session/share rebind to Background is by design: the connection outlives the request ctx.
func (b *SMBBackend) connectLocked(ctx context.Context) error {
	b.closeLocked()

	if ctx == nil {
		ctx = context.Background()
	}

	dialer := &net.Dialer{Timeout: defaultDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", b.Address)
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

	// DialConn honors ctx for the handshake but stores context.Background on the
	// returned session, so the session outlives ctx.
	s, err := d.DialConn(ctx, conn, b.Address)
	if err != nil {
		_ = conn.Close()
		b.conn = nil

		return fmt.Errorf("smb dial failed: %w", err)
	}
	b.session = s

	// Mount uses one context for both the tree-connect RPC and the returned
	// share's base context. Bind it to ctx so the RPC honors the deadline, then
	// rebind the share to Background so later Umount and per-call ops are not
	// tied to a caller deadline that has since expired.
	fs, err := s.WithContext(ctx).Mount(b.Share)
	if err != nil {
		_ = s.Logoff()
		b.session = nil
		_ = conn.Close()
		b.conn = nil

		return fmt.Errorf("mount failed: %w", err)
	}
	b.share = fs.WithContext(context.Background())

	return nil
}

func (b *SMBBackend) ensureConnected(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.share == nil {
		return b.connectLocked(ctx)
	}

	return nil
}

func (b *SMBBackend) getShare(ctx context.Context) (*smb2.Share, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.share == nil {
		return nil, errors.New("not connected")
	}

	return b.share.WithContext(ctx), nil
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
