package backend

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
)

func TestSMBBackend_AutoReconnect(t *testing.T) {
	// 1. Starts a local TCP listener.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	addr := l.Addr().String()

	// Track accepted connections
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
			
			// Close the accepted connection to simulate a drop / failure
			conn.Close()
		}
	}()

	// 2. Creates an SMBBackend pointing to the listener's address.
	b := NewSMBBackend("test", addr, "share", "user", "pass", "")

	// 3. Verifies that calling an operation (e.g., Ping or Stat) attempts to connect.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call Ping. Since the connection is closed immediately, this will fail.
	// But it should attempt to connect.
	_ = b.Ping(ctx)

	// Check if the listener accepted the connection
	mu.Lock()
	if len(conns) != 1 {
		mu.Unlock()
		t.Fatalf("expected 1 connection attempt, got %d", len(conns))
	}
	mu.Unlock()

	// 4. Verifies that calling the operation again successfully attempts to reconnect (accepts new connection).
	// Call Ping again. Since b.share is nil (connect failed), it will attempt to connect again.
	_ = b.Ping(ctx)

	mu.Lock()
	if len(conns) != 2 {
		mu.Unlock()
		t.Fatalf("expected 2 connection attempts (reconnect), got %d", len(conns))
	}
	mu.Unlock()
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
		if got != tc.expected {
			t.Errorf("isConnectionError(%v) = %v, expected %v", tc.err, got, tc.expected)
		}
	}
}
