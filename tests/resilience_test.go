package tests

import (
	"fmt"
	"os"
	"testing"

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
