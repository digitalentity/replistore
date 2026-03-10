package vfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
)

func TestDistributedLock_AcquireQuorum(t *testing.T) {
	// Setup 3 nodes
	n1 := cluster.NewLockManager("node1")
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	n2 := cluster.NewLockManager("node2")
	addr2, _ := n2.Start("127.0.0.1:0")
	defer n2.Stop()

	n3 := cluster.NewLockManager("node3")
	addr3, _ := n3.Start("127.0.0.1:0")
	defer n3.Stop()

	// Discovery mock for node1 (sees node2 and node3)
	disco1 := cluster.NewDiscovery("node1", 0)
	disco1.Peers["node2"] = cluster.Peer{ID: "node2", Address: addr2, LastSeen: time.Now()}
	disco1.Peers["node3"] = cluster.Peer{ID: "node3", Address: addr3, LastSeen: time.Now()}

	lock := vfs.NewDistributedLock("test/path", n1, disco1)
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := lock.Acquire(ctx)
	assert.NoError(t, err)
	assert.True(t, lock.IsValid())
	assert.NotEmpty(t, lock.FencingToken)

	lock.Release()
	assert.False(t, lock.IsValid())
}

func TestDistributedLock_AcquireQuorumFailure(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	// node2 is "down" (no server started)
	addr2 := "127.0.0.1:65535" 

	disco1 := cluster.NewDiscovery("node1", 0)
	disco1.Peers["node2"] = cluster.Peer{ID: "node2", Address: addr2, LastSeen: time.Now()}

	lock := vfs.NewDistributedLock("test/path", n1, disco1)
	
	// With 2 nodes total (n1, n2), quorum is 2.
	// Since node2 is down, acquisition should fail.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := lock.Acquire(ctx)
	assert.Error(t, err)
	assert.False(t, lock.IsValid())
}

func TestDistributedLock_Renewal(t *testing.T) {
	n1 := cluster.NewLockManager("node1")
	n1.LeaseTTL = 500 * time.Millisecond
	_, _ = n1.Start("127.0.0.1:0")
	defer n1.Stop()

	disco1 := cluster.NewDiscovery("node1", 0)
	// Just local node for simplicity in renewal test

	lock := vfs.NewDistributedLock("renew/path", n1, disco1)
	
	err := lock.Acquire(context.Background())
	assert.NoError(t, err)
	assert.True(t, lock.IsValid())

	// Wait more than LeaseTTL, renewal should keep it valid
	time.Sleep(800 * time.Millisecond)
	assert.True(t, lock.IsValid())

	lock.Release()
}
