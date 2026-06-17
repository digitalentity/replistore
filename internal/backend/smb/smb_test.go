package smb

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSMBBackend_AutoReconnect(t *testing.T) {
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to listen")
	defer l.Close()

	addr := l.Addr().String()

	var conns []net.Conn
	var mu sync.Mutex

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()

			_ = conn.Close()
		}
	}()

	b := NewSMBBackend("test", addr, "share", "user", "pass", "", 10, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = b.Ping(ctx)

	mu.Lock()
	connsLen := len(conns)
	mu.Unlock()
	require.Equal(t, 1, connsLen, "expected 1 connection attempt")

	_ = b.Ping(ctx)

	mu.Lock()
	connsLen = len(conns)
	mu.Unlock()
	require.Equal(t, 2, connsLen, "expected 2 connection attempts (reconnect)")
}

// trackConn wraps a net.Conn and records whether Close was called.
type trackConn struct {
	net.Conn
	mu     sync.Mutex
	closed bool
}

func (c *trackConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	return c.Conn.Close()
}

func (c *trackConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closed
}

func newTrackConn(t *testing.T) *trackConn {
	t.Helper()
	c, other := net.Pipe()
	t.Cleanup(func() { _ = other.Close() })

	return &trackConn{Conn: c}
}

func TestWatchdog_ClosesConnOnCancel(t *testing.T) {
	conn := newTrackConn(t)
	b := &SMBBackend{conn: conn, session: nil, share: nil}

	ctx, cancel := context.WithCancel(context.Background())
	defer b.watchdog(ctx)()

	cancel()

	assert.Eventually(t, conn.isClosed, time.Second, 5*time.Millisecond,
		"watchdog should close the connection on context cancel")

	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Nil(t, b.conn, "watchdog should null the connection")
	assert.Nil(t, b.share, "watchdog should drop the share")
}

func TestWatchdog_NoCloseWhenCancelledFirst(t *testing.T) {
	conn := newTrackConn(t)
	b := &SMBBackend{conn: conn}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := b.watchdog(ctx)
	stop()
	cancel()

	// Give any erroneous goroutine a chance to fire.
	time.Sleep(20 * time.Millisecond)
	assert.False(t, conn.isClosed(), "watchdog must not close conn after its cancel func ran")
}

func TestWatchdog_IgnoresReplacedConn(t *testing.T) {
	oldConn := newTrackConn(t)
	newConn := newTrackConn(t)
	b := &SMBBackend{conn: oldConn}

	ctx, cancel := context.WithCancel(context.Background())
	defer b.watchdog(ctx)()

	// Simulate a concurrent reconnect swapping the connection after the
	// watchdog snapshotted the old one.
	b.mu.Lock()
	b.conn = newConn
	b.mu.Unlock()

	cancel()

	time.Sleep(20 * time.Millisecond)
	assert.False(t, oldConn.isClosed(), "stale watchdog must not close the old conn")
	assert.False(t, newConn.isClosed(), "stale watchdog must not close the replacement conn")
}

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{io.EOF, true},
		{context.Canceled, false},
		{errors.New("some random error"), false},
		{&net.OpError{Err: &os.SyscallError{Err: syscall.EPIPE}}, true},
		{&net.OpError{Err: &os.SyscallError{Err: syscall.ECONNRESET}}, true},
		{errors.New("use of closed network connection"), true},
	}

	for _, tc := range tests {
		got := isConnectionError(tc.err)
		assert.Equal(t, tc.expected, got, "isConnectionError(%v)", tc.err)
	}
}

func TestServiceCtx(t *testing.T) {
	b := &SMBBackend{}
	assert.Equal(t, context.Background(), b.serviceCtx(), "nil ctx must fall back to Background")

	ctx := t.Context()
	b.ctx = ctx
	assert.Equal(t, ctx, b.serviceCtx(), "must return the captured lifecycle ctx")
}

func TestOpenScopedCtx_SwapsToLifecycleOnRelease(t *testing.T) {
	callCtx, cancelCall := context.WithCancel(context.Background())
	defer cancelCall()
	lifeCtx, cancelLife := context.WithCancel(context.Background())
	defer cancelLife()

	c := newOpenScopedCtx(callCtx, lifeCtx)

	// Before release the wrapper tracks the caller context.
	cancelCall()
	require.ErrorIs(t, c.Err(), context.Canceled)
	select {
	case <-c.Done():
	default:
		t.Fatal("Done must fire while bound to a cancelled caller ctx")
	}

	// After release it detaches from the caller ctx and tracks the lifecycle.
	c.release()
	require.NoError(t, c.Err(), "released ctx must not observe caller cancellation")
	select {
	case <-c.Done():
		t.Fatal("Done must not fire while the lifecycle ctx is live")
	default:
	}

	// Lifecycle cancellation still propagates after release.
	cancelLife()
	require.ErrorIs(t, c.Err(), context.Canceled)
}

func TestOpenScopedCtx_DeadlineForwarded(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	callCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	c := newOpenScopedCtx(callCtx, context.Background())

	got, ok := c.Deadline()
	require.True(t, ok)
	assert.True(t, got.Equal(deadline), "must forward the caller deadline")

	c.release()
	_, ok = c.Deadline()
	assert.False(t, ok, "background lifecycle has no deadline")
}
