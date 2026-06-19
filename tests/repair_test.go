package tests

import (
	"context"
	"io"
	"testing"
	"time"

	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Repair(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	s2 := startTestSMBServer(t, "share2")
	s3 := startTestSMBServer(t, "share3")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write Quorum = 2
	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 2)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// 1. Stop s3
	s3.Stop()
	_ = replFS.Backends["b3"].Close()
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 2*time.Second, 20*time.Millisecond)

	// 2. Write file (written to s1 and s2)
	content := []byte("repair data content")
	err := writeFile(ctx, dir, "repairme.txt", content)
	require.NoError(t, err)

	// 3. Restart s3
	err = s3.Restart()
	require.NoError(t, err)

	// 4. Wait for monitor to mark b3 healthy
	assert.Eventually(t, func() bool {
		return monitor.IsHealthy("b3")
	}, 3*time.Second, 50*time.Millisecond)

	// 5. Start RepairManager with a very short interval
	repairManager := rfuse.NewRepairManager(replFS, 50*time.Millisecond, 0, 2)
	repairManager.Start(ctx)

	// 6. Wait for file to be repaired onto s3
	assert.Eventually(t, func() bool {
		f, err := s3.MemFS.Open("/repairme.txt")
		if err != nil {
			return false
		}
		defer f.Close()
		b, err := io.ReadAll(f)

		return err == nil && string(b) == string(content)
	}, 5*time.Second, 100*time.Millisecond)
}
