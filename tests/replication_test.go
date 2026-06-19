package tests

import (
	"context"
	"fmt"
	"io"
	"testing"

	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_BasicReplication(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	s2 := startTestSMBServer(t, "share2")
	s3 := startTestSMBServer(t, "share3")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replFS, _, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 3)
	defer cleanup()

	rootNode, err := replFS.Root()
	require.NoError(t, err)
	dir := rootNode.(*rfuse.Dir)

	content := []byte("hello from integration tests")

	// 1. Write file
	err = writeFile(ctx, dir, "test.txt", content)
	require.NoError(t, err)

	// 2. Lookup & verify VFS layer
	node, err := dir.Lookup(ctx, "test.txt")
	require.NoError(t, err)
	file := node.(*rfuse.File)

	cachedNode, ok := replFS.Cache.Get("test.txt")
	require.True(t, ok)
	cachedNode.Mu.RLock()
	assert.Equal(t, int64(len(content)), cachedNode.Meta.Size)
	assert.ElementsMatch(t, []string{"b1", "b2", "b3"}, cachedNode.Meta.Backends)
	cachedNode.Mu.RUnlock()

	// 3. Read back from RepliFS
	data, err := readFile(ctx, file)
	require.NoError(t, err)
	assert.Equal(t, content, data)

	// 4. Verify underlying memfs filesystems of all three servers
	for i, s := range []*TestSMBServer{s1, s2, s3} {
		f, err := s.MemFS.Open("/test.txt")
		require.NoError(t, err, "file not found on server %d", i+1)
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		_ = f.Close()
		assert.Equal(t, content, b, "mismatched content on server %d", i+1)

		// Verify that sidecars exist
		sidecar, err := vfs.ReadMeta(ctx, replFS.Backends[fmt.Sprintf("b%d", i+1)], "test.txt")
		require.NoError(t, err)
		assert.False(t, sidecar.Deleted)
		assert.Equal(t, int64(1), sidecar.Gen)
	}
}
