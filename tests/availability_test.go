package tests

import (
	"context"
	"testing"
	"time"

	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_ReadFailover(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	s2 := startTestSMBServer(t, "share2")
	s3 := startTestSMBServer(t, "share3")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 3)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	content := []byte("read failover data")
	err := writeFile(ctx, dir, "failover.txt", content)
	require.NoError(t, err)

	// Stop s3
	s3.Stop()
	_ = replFS.Backends["b3"].Close()

	// Wait for monitor to mark b3 down
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 2*time.Second, 20*time.Millisecond)

	// Reading should still succeed by routing to b1 or b2
	node, err := dir.Lookup(ctx, "failover.txt")
	require.NoError(t, err)
	file := node.(*rfuse.File)

	data, err := readFile(ctx, file)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestIntegration_WriteQuorumEnforcement(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	s2 := startTestSMBServer(t, "share2")
	s3 := startTestSMBServer(t, "share3")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write Quorum = 2, so we can write even if 1 server is down
	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 2)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// Stop s3
	s3.Stop()
	_ = replFS.Backends["b3"].Close()

	// Wait for monitor to mark b3 down
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 2*time.Second, 20*time.Millisecond)

	// Write should succeed because write quorum of 2 is met (b1, b2)
	content := []byte("quorum test content")
	err := writeFile(ctx, dir, "quorum.txt", content)
	require.NoError(t, err)

	// Verify s1 and s2 have it, s3 does not
	f1, err := s1.MemFS.Open("/quorum.txt")
	require.NoError(t, err)
	_ = f1.Close()
	f2, err := s2.MemFS.Open("/quorum.txt")
	require.NoError(t, err)
	_ = f2.Close()
	_, err = s3.MemFS.Open("/quorum.txt")
	require.Error(t, err)

	// Now stop s2, leaving only s1 alive
	s2.Stop()
	_ = replFS.Backends["b2"].Close()
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b2")
	}, 2*time.Second, 20*time.Millisecond)

	// Write should fail because write quorum of 2 is not met
	err = writeFile(ctx, dir, "failed_quorum.txt", []byte("should fail"))
	require.Error(t, err)
}
