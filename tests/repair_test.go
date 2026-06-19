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

// TestIntegration_RepairGraceWindowDelaysRepair proves the repair grace window
// end to end: a genuinely degraded file (written while a backend was down) is
// withheld from repair until the grace elapses, then repaired once it does. The
// Never window is a fraction of the grace, so it stays robust under load — a
// slow scheduler only makes an early repair less likely, never more.
func TestIntegration_RepairGraceWindowDelaysRepair(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	s2 := startTestSMBServer(t, "share2")
	s3 := startTestSMBServer(t, "share3")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write quorum 2 so a write lands while one backend is down.
	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 2)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// Degrade: write while s3 is down so the file lands on b1 and b2 only.
	s3.Stop()
	_ = replFS.Backends["b3"].Close()
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 2*time.Second, 20*time.Millisecond)

	content := []byte("grace window content")
	require.NoError(t, writeFile(ctx, dir, "graced.txt", content))

	// Bring s3 back so it is a healthy, valid repair target throughout the test.
	require.NoError(t, s3.Restart())
	assert.Eventually(t, func() bool {
		return monitor.IsHealthy("b3")
	}, 3*time.Second, 50*time.Millisecond)

	repairedOntoS3 := func() bool {
		f, err := s3.MemFS.Open("/graced.txt")
		if err != nil {
			return false
		}
		defer f.Close()
		b, err := io.ReadAll(f)

		return err == nil && string(b) == string(content)
	}

	const grace = 2 * time.Second
	repairManager := rfuse.NewRepairManager(replFS, 30*time.Millisecond, grace, 2)
	repairManager.Start(ctx)

	// Inside the grace window the file must NOT be repaired onto s3, even though
	// the scrub runs every 30ms and s3 is a healthy target.
	require.Never(t, repairedOntoS3, 750*time.Millisecond, 50*time.Millisecond,
		"repair must be withheld until the grace window elapses")

	// Past the grace window the scrub repairs it.
	require.Eventually(t, repairedOntoS3, grace+3*time.Second, 50*time.Millisecond,
		"repair must proceed once the grace window has elapsed")
}
