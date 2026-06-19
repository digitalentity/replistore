package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/backend/smb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectBackendVia dials a single backend at addr (typically a proxy in front
// of the real server) against the "share" share and registers cleanup.
func connectBackendVia(t *testing.T, ctx context.Context, addr string) *smb.SMBBackend {
	t.Helper()
	b := smb.NewSMBBackend("c", addr, "share", testUser, testPass, "", 10, nil)
	require.NoError(t, b.Connect(ctx))
	t.Cleanup(func() { _ = b.Close() })

	return b
}

func writeBackendFile(t *testing.T, ctx context.Context, b backend.Backend, name string, content []byte) {
	t.Helper()
	f, err := b.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	n, err := f.WriteAt(ctx, content, 0)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.NoError(t, f.Close())
}

// TestSMB_ReadAtHonorsContextCancel proves the per-call File.WithContext binding
// from the smb refactor actually cancels a blocked read. With the proxy paused,
// the READ request is stranded on the wire; cancelling the call context must
// unblock ReadAt promptly rather than hang until the network gives up.
func TestSMB_ReadAtHonorsContextCancel(t *testing.T) {
	s := startTestSMBServer(t, "share")
	proxy := startPausableProxy(t, fmt.Sprintf("127.0.0.1:%d", s.Port))

	setupCtx := t.Context()

	b := connectBackendVia(t, setupCtx, proxy.Addr())
	writeBackendFile(t, setupCtx, b, "blocked.txt", []byte("payload"))

	f, err := b.OpenFile(setupCtx, "blocked.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	// Defers run LIFO: tear the proxy down first so the handle/backend CLOSE and
	// Umount that follow are not themselves stranded by the pause this test
	// leaves in place.
	defer f.Close()
	defer proxy.Close()

	// Strand the next request, then cancel the read mid-flight.
	proxy.Pause()
	readCtx, readCancel := context.WithCancel(context.Background())

	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		buf := make([]byte, 7)
		_, err := f.ReadAt(readCtx, buf, 0)
		done <- result{err: err}
	}()

	// Give the read a beat to actually block on the stranded request.
	time.Sleep(50 * time.Millisecond)
	readCancel()

	select {
	case r := <-done:
		require.ErrorIs(t, r.err, context.Canceled, "cancelled ReadAt must return context.Canceled")
	case <-time.After(3 * time.Second):
		t.Fatal("ReadAt did not return after context cancellation: per-call cancellation is broken")
	}
}

// TestSMB_ReadAtHonorsDeadline is the deadline twin of the cancel test: a call
// context deadline must abort a stranded read.
func TestSMB_ReadAtHonorsDeadline(t *testing.T) {
	s := startTestSMBServer(t, "share")
	proxy := startPausableProxy(t, fmt.Sprintf("127.0.0.1:%d", s.Port))

	setupCtx := t.Context()

	b := connectBackendVia(t, setupCtx, proxy.Addr())
	writeBackendFile(t, setupCtx, b, "slow.txt", []byte("payload"))

	f, err := b.OpenFile(setupCtx, "slow.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	// Defers run LIFO: tear the proxy down before the handle close so the CLOSE
	// request is not stranded by the pause this test leaves in place.
	defer f.Close()
	defer proxy.Close()

	proxy.Pause()

	readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer readCancel()

	start := time.Now()
	buf := make([]byte, 7)
	_, err = f.ReadAt(readCtx, buf, 0)

	require.ErrorIs(t, err, context.DeadlineExceeded, "ReadAt must abort at the call deadline")
	assert.Less(t, time.Since(start), 2*time.Second, "ReadAt must abort near the deadline, not hang")
}

// TestSMB_CancelledOpDoesNotPoisonBackend guards the headline win of the
// refactor: a cancelled call no longer tears down the shared connection (the old
// watchdog force-closed it). A read with an already-cancelled context must fail,
// yet a later read on a fresh context must still succeed over the same backend.
func TestSMB_CancelledOpDoesNotPoisonBackend(t *testing.T) {
	s := startTestSMBServer(t, "share")

	setupCtx := t.Context()

	b := connectBackendVia(t, setupCtx, fmt.Sprintf("127.0.0.1:%d", s.Port))
	content := []byte("survives cancellation")
	writeBackendFile(t, setupCtx, b, "live.txt", content)

	f, err := b.OpenFile(setupCtx, "live.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	defer f.Close()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.ReadAt(cancelledCtx, make([]byte, len(content)), 0)
	require.ErrorIs(t, err, context.Canceled)

	// A fresh read on the same backend must still work: the connection was not
	// torn down by the cancelled call.
	f2, err := b.OpenFile(setupCtx, "live.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	defer f2.Close()

	buf := make([]byte, len(content))
	n, err := f2.ReadAt(setupCtx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, content, buf[:n])
}
