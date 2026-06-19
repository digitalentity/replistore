package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSMB_ReconnectsAfterConnectionDrop exercises the execute() retry path
// against a real dropped connection (the unit test only counts dials against a
// listener that refuses every connection). After the proxy cuts the live
// connection, a share-level operation must transparently reconnect and succeed.
//
// It also pins the documented handle contract: an open *smb2.File does not
// survive a reconnect, so a read on the pre-drop handle is expected to fail
// while a freshly opened handle reads the content back intact.
func TestSMB_ReconnectsAfterConnectionDrop(t *testing.T) {
	s := startTestSMBServer(t, "share")
	proxy := startPausableProxy(t, fmt.Sprintf("127.0.0.1:%d", s.Port))

	ctx := t.Context()
	b := connectBackendVia(t, ctx, proxy.Addr())

	content := []byte("survives a reconnect")
	writeBackendFile(t, ctx, b, "drop.txt", content)

	staleHandle, err := b.OpenFile(ctx, "drop.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	defer staleHandle.Close()

	// Drop the live connection out from under the backend.
	proxy.Cut()

	// The pre-drop handle is tied to the dead connection and must not silently
	// succeed.
	_, err = staleHandle.ReadAt(ctx, make([]byte, len(content)), 0)
	require.Error(t, err, "read on a handle from before the drop must fail")

	// A share-level operation must transparently reconnect through the proxy.
	_, err = b.Stat(ctx, "drop.txt")
	require.NoError(t, err, "backend must reconnect after the drop")

	// And a fresh handle on the reconnected backend reads the content back.
	freshHandle, err := b.OpenFile(ctx, "drop.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	defer freshHandle.Close()

	buf := make([]byte, len(content))
	n, err := freshHandle.ReadAt(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, content, buf[:n])
}

// TestSMB_ReconnectHonorsContextDeadline pins the M7 fix: a reconnect triggered
// by an operation is bounded by that operation's context deadline, not by the
// backend lifecycle context or the dial timeout. The proxy drops the live
// connection and then stalls every new handshake, so the only way the call can
// return is by honoring its own short deadline.
func TestSMB_ReconnectHonorsContextDeadline(t *testing.T) {
	s := startTestSMBServer(t, "share")
	proxy := startPausableProxy(t, addrOf(s))

	// Initial connect succeeds over the live proxy.
	b := connectBackendVia(t, t.Context(), proxy.Addr())

	// Drop the live connection so the next op must reconnect, then stall every
	// future handshake so the reconnect can only end by hitting its deadline.
	proxy.Cut()
	proxy.Pause()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := b.Stat(ctx, "anything")
	elapsed := time.Since(start)

	require.Error(t, err, "stat over a stalled reconnect must fail")
	assert.Less(t, elapsed, 3*time.Second,
		"reconnect must abort at the request deadline, not stall on the lifecycle ctx or dial timeout")
}
