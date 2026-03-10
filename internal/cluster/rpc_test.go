package cluster

import (
	"net/rpc"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLockManager_RPC(t *testing.T) {
	m := NewLockManager("node1")
	addr, err := m.Start("127.0.0.1:0")
	assert.NoError(t, err)
	defer m.Stop()

	client, err := rpc.Dial("tcp", addr)
	assert.NoError(t, err)
	defer client.Close()

	path := "test/path"
	
	// 1. Request Lock
	req := LockRequest{
		Path:        path,
		NodeID:      "node2",
		LamportTime: 100,
	}
	var resp LockResponse
	err = client.Call("LockManager.RequestLock", req, &resp)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, resp.Status)
	assert.NotEmpty(t, resp.FencingToken)

	// 2. Try to request same lock from another node
	req2 := LockRequest{
		Path:        path,
		NodeID:      "node3",
		LamportTime: 101,
	}
	var resp2 LockResponse
	err = client.Call("LockManager.RequestLock", req2, &resp2)
	assert.NoError(t, err)
	assert.Equal(t, LockReject, resp2.Status)

	// 3. Renew Lock
	renewReq := LockRenewal{
		Path:         path,
		NodeID:       "node2",
		FencingToken: resp.FencingToken,
	}
	var status LockStatus
	err = client.Call("LockManager.RenewLock", renewReq, &status)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, status)

	// 4. Release Lock
	releaseReq := LockRelease{
		Path:         path,
		NodeID:       "node2",
		FencingToken: resp.FencingToken,
	}
	err = client.Call("LockManager.ReleaseLock", releaseReq, &status)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, status)

	// 5. Request again (should succeed)
	err = client.Call("LockManager.RequestLock", req2, &resp2)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, resp2.Status)
}

func TestLamportClock(t *testing.T) {
	c := &LamportClock{}
	assert.Equal(t, int64(1), c.Tick())
	assert.Equal(t, int64(2), c.Tick())
	
	assert.Equal(t, int64(11), c.Update(10))
	assert.Equal(t, int64(12), c.Tick())
	
	assert.Equal(t, int64(13), c.Update(5)) // Update with smaller time still ticks
}

func TestLockManager_LeaseExpiration(t *testing.T) {
	m := NewLockManager("node1")
	m.LeaseTTL = 100 * time.Millisecond
	
	req := LockRequest{Path: "expire", NodeID: "node2"}
	var resp LockResponse
	_ = m.RequestLock(req, &resp)
	
	assert.Equal(t, LockOK, resp.Status)
	
	// Wait for expiration
	time.Sleep(200 * time.Millisecond)
	
	// Should be able to lock from another node now
	req2 := LockRequest{Path: "expire", NodeID: "node3"}
	var resp2 LockResponse
	_ = m.RequestLock(req2, &resp2)
	assert.Equal(t, LockOK, resp2.Status)
}
