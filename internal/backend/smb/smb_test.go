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
