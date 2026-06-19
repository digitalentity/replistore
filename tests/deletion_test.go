package tests

import (
	"context"
	"fmt"
	"testing"

	"bazil.org/fuse"
	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_TombstoneDeletion(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	s2 := startTestSMBServer(t, "share2")
	s3 := startTestSMBServer(t, "share3")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replFS, _, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 3)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// Write file
	err := writeFile(ctx, dir, "deleteme.txt", []byte("some content"))
	require.NoError(t, err)

	// Remove it
	err = dir.Remove(ctx, &fuse.RemoveRequest{Name: "deleteme.txt"})
	require.NoError(t, err)

	// Lookup should return EIO or not exist
	_, err = dir.Lookup(ctx, "deleteme.txt")
	require.Error(t, err)

	// Verify deletion on underlying backends and presence of tombstone sidecars
	for i, s := range []*TestSMBServer{s1, s2, s3} {
		_, err := s.MemFS.Open("/deleteme.txt")
		require.Error(t, err, "file still exists on server %d", i+1)

		sidecar, err := vfs.ReadMeta(ctx, replFS.Backends[fmt.Sprintf("b%d", i+1)], "deleteme.txt")
		require.NoError(t, err)
		assert.True(t, sidecar.Deleted)
	}
}
