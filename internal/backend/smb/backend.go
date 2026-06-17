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
	"github.com/cloudsoda/go-smb2"
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

	mu sync.Mutex
	// ctx is the backend lifecycle context captured at Connect. It governs work
	// that runs outside any single request: reconnect dials in connectLocked and
	// the RPC lifetime of open *smb2.File handles (see serviceCtx and OpenFile).
	// It is not a per-request context. Guarded by mu.
	//nolint:containedctx // by design: backend lifecycle ctx, not a request ctx.
	ctx     context.Context
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
	b.ctx = ctx

	return b.connectLocked()
}

// serviceCtx returns the lifecycle context captured at Connect, or
// context.Background if Connect has not run. Safe for concurrent use; callers
// must not already hold b.mu.
func (b *SMBBackend) serviceCtx() context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx == nil {
		return context.Background()
	}

	return b.ctx
}

//nolint:contextcheck // runs independently of request context
func (b *SMBBackend) connectLocked() error {
	b.closeLocked()

	ctx := b.ctx
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

	s, err := d.DialConn(ctx, conn, b.Address)
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

// resetConnLocked tears down the connection state without a graceful SMB
// teardown. Closing the raw conn is enough to unblock any in-flight I/O; the
// share and session handles are dropped rather than Umount/Logoff'd, as those
// would attempt network round-trips on a connection we are abandoning, which
// could itself hang on the very failure we are reacting to. Callers must hold
// b.mu.
func (b *SMBBackend) resetConnLocked() {
	if b.conn != nil {
		_ = b.conn.Close()
	}
	b.conn = nil
	b.session = nil
	b.share = nil
}

// watchdog force-closes the backend connection if ctx is cancelled before the
// returned cancel func is called. It exists solely to unblock context-unaware
// *smb2.File I/O, which the go-smb2 library cannot otherwise interrupt; share
// operations are cancelled through Share.WithContext and must not use this.
//
// The connection is snapshotted at call time so a concurrent reconnect is never
// torn down by a stale watchdog. Closing the shared conn unblocks every in-flight
// op on it, not just this one, but that blast radius is inherent to a library
// that multiplexes all requests over a single connection with no per-request
// cancellation.
func (b *SMBBackend) watchdog(ctx context.Context) func() {
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// If the op already finished, done is closed too and select picked
			// this branch on a tie. Prefer done so a healthy conn is never torn
			// down out from under a completed op.
			select {
			case <-done:
				return
			default:
			}
			b.mu.Lock()
			if b.conn == conn {
				b.resetConnLocked()
			}
			b.mu.Unlock()
		case <-done:
		}
	}()

	return func() {
		close(done)
	}
}

