package vfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClusterSecret is shared by every LockManager in these tests so signed
// lock datagrams verify across "nodes".
var testClusterSecret = []byte("vfs-test-secret-at-least-16-chars")

func TestDistributedLock_AcquireQuorum(t *testing.T) {
	// Setup 3 nodes
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 3
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	n2 := cluster.NewLockManager("node2")
	n2.Secret = testClusterSecret
	n2.ExpectedClusterSize = 3
	addr2, _ := n2.Start("127.0.0.1:0")
	defer n2.Stop()

	n3 := cluster.NewLockManager("node3")
	n3.Secret = testClusterSecret
	n3.ExpectedClusterSize = 3
	addr3, _ := n3.Start("127.0.0.1:0")
	defer n3.Stop()

	// Discovery mock for node1 (sees node2 and node3)
	disco1 := cluster.NewDiscovery("node1", "", nil)
	disco1.Peers["node2"] = cluster.Peer{ID: "node2", Address: addr2, LastSeen: time.Now()}
	disco1.Peers["node3"] = cluster.Peer{ID: "node3", Address: addr3, LastSeen: time.Now()}

	lock := vfs.NewDistributedLock("test/path", n1, disco1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := lock.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, lock.IsValid())
	assert.NotEmpty(t, lock.FencingToken)

	lock.Release()
	assert.False(t, lock.IsValid())
}

func TestDistributedLock_AcquireQuorumFailure(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 2
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	// node2 is "down": an unused UDP port with no listener. CallUDP must
	// keep retrying until its context expires, so the acquisition fails by
	// timeout rather than a fast connection error.
	addr2 := "127.0.0.1:65535"

	disco1 := cluster.NewDiscovery("node1", "", nil)
	disco1.Peers["node2"] = cluster.Peer{ID: "node2", Address: addr2, LastSeen: time.Now()}

	lock := vfs.NewDistributedLock("test/path", n1, disco1)

	// With 2 nodes total (n1, n2), quorum is 2.
	// Since node2 is down, acquisition should fail.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := lock.Acquire(ctx)
	require.Error(t, err)
	assert.False(t, lock.IsValid())
}

func TestDistributedLock_Renewal(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 1
	n1.LeaseTTL = 500 * time.Millisecond
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	disco1 := cluster.NewDiscovery("node1", "", nil)
	// Just local node for simplicity in renewal test

	lock := vfs.NewDistributedLock("renew/path", n1, disco1)

	err := lock.Acquire(context.Background())
	require.NoError(t, err)
	assert.True(t, lock.IsValid())

	// Wait more than LeaseTTL, renewal should keep it valid
	time.Sleep(800 * time.Millisecond)
	assert.True(t, lock.IsValid())

	lock.Release()
}

// TestDistributedLock_RenewalGracePeriod verifies H4: a single renewal round
// that misses quorum does NOT immediately surrender the lock. The lock stays
// valid until the lease deadline actually passes without a successful round.
func TestDistributedLock_RenewalGracePeriod(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 2
	n1.LeaseTTL = 600 * time.Millisecond
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	n2 := cluster.NewLockManager("node2")
	n2.Secret = testClusterSecret
	n2.ExpectedClusterSize = 2
	n2.LeaseTTL = 600 * time.Millisecond
	addr2, _ := n2.Start("127.0.0.1:0")

	disco1 := cluster.NewDiscovery("node1", "", nil)
	disco1.Peers["node2"] = cluster.Peer{ID: "node2", Address: addr2, LastSeen: time.Now()}

	lock := vfs.NewDistributedLock("grace/path", n1, disco1)

	err := lock.Acquire(context.Background())
	require.NoError(t, err)
	assert.True(t, lock.IsValid())

	// Kill the peer: every subsequent renewal round can reach only the local
	// node (1 of 2), missing quorum.
	n2.Stop()

	// One cadence (LeaseTTL/3 = 200ms) plus margin later, at least one renewal
	// round has failed, yet the lease deadline (~600ms out) has not passed: the
	// lock must still be held.
	time.Sleep(300 * time.Millisecond)
	assert.True(t, lock.IsValid(), "lock surrendered on a single missed renewal round")

	// Past the lease deadline with no successful round, the lock is lost.
	time.Sleep(500 * time.Millisecond)
	assert.False(t, lock.IsValid(), "lock not surrendered after lease deadline passed")

	lock.Release()
}

func TestDistributedLock_SameNodeMutualExclusion(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 1
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	disco1 := cluster.NewDiscovery("node1", "", nil)

	lock1 := vfs.NewDistributedLock("same/path", n1, disco1)
	lock2 := vfs.NewDistributedLock("same/path", n1, disco1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First acquisition succeeds.
	err := lock1.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, lock1.IsValid())

	// Second acquisition for the same path from the same node must fail
	// while the first lock is held.
	err = lock2.Acquire(ctx)
	require.Error(t, err)
	assert.False(t, lock2.IsValid())

	// The first lock is unaffected.
	assert.True(t, lock1.IsValid())

	lock1.Release()
}

func TestDistributedLock_ExpectedClusterSizeQuorum(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 5
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	n2 := cluster.NewLockManager("node2")
	n2.Secret = testClusterSecret
	n2.ExpectedClusterSize = 5
	addr2, _ := n2.Start("127.0.0.1:0")
	defer n2.Stop()

	// Discovery mock for node1 (only sees node2)
	disco1 := cluster.NewDiscovery("node1", "", nil)
	disco1.Peers["node2"] = cluster.Peer{ID: "node2", Address: addr2, LastSeen: time.Now()}

	lock := vfs.NewDistributedLock("test/expected_size", n1, disco1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Quorum for ExpectedClusterSize=5 is 3. We only have node1 and node2 (2 nodes).
	// So acquisition should fail.
	err := lock.Acquire(ctx)
	require.Error(t, err)
	assert.False(t, lock.IsValid())
}

func TestDistributedLock_AcquireRetriesAfterContention(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 1
	n1.LeaseTTL = 100 * time.Millisecond
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	disco1 := cluster.NewDiscovery("node1", "", nil)

	// Simulate a conflicting holder: a grant on the same path from another
	// (node, lockid), expiring after LeaseTTL.
	var resp cluster.LockResponse
	err := n1.RequestLock(cluster.LockRequest{
		Path:         "contended/path",
		NodeID:       "node2",
		LockID:       "other-lock",
		LamportTime:  1,
		FencingToken: "other-fence",
	}, &resp)
	require.NoError(t, err)
	assert.Equal(t, cluster.LockOK, resp.Status)

	// Let most of the conflicting lease elapse so the first Acquire attempt
	// is rejected but a backed-off retry lands after expiry.
	time.Sleep(80 * time.Millisecond)

	lock := vfs.NewDistributedLock("contended/path", n1, disco1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = lock.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, lock.IsValid())
	assert.NotEmpty(t, lock.FencingToken)

	lock.Release()
	assert.False(t, lock.IsValid())
}

func TestDistributedLock_IsValidWithBuffer(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.Secret = testClusterSecret
	n1.ExpectedClusterSize = 1
	n1.LeaseTTL = 500 * time.Millisecond
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	disco1 := cluster.NewDiscovery("node1", "", nil)

	lock := vfs.NewDistributedLock("buffer/path", n1, disco1)

	err := lock.Acquire(context.Background())
	require.NoError(t, err)
	assert.True(t, lock.IsValid())

	// If we check with a buffer of 100ms, it should be valid since lease is 500ms
	assert.True(t, lock.IsValidWithBuffer(100*time.Millisecond))

	// If we check with a buffer of 600ms, it should be invalid since lease is only 500ms
	assert.False(t, lock.IsValidWithBuffer(600*time.Millisecond))

	// Wait 100ms, now remaining is ~400ms. The first renewal tick fires at
	// LeaseTTL/3 (~167ms), so no background renewal has advanced the lease yet.
	time.Sleep(100 * time.Millisecond)
	assert.True(t, lock.IsValidWithBuffer(200*time.Millisecond))
	assert.False(t, lock.IsValidWithBuffer(450*time.Millisecond))

	lock.Release()
	assert.False(t, lock.IsValidWithBuffer(10*time.Millisecond))
}
