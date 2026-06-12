package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLockManager_RPC(t *testing.T) {
	secret := []byte("test-secret-at-least-16-chars")

	m := NewLockManager("node1")
	m.Secret = secret
	addr, err := m.Start("127.0.0.1:0")
	assert.NoError(t, err)
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	path := "test/path"

	// 1. Request Lock
	req := LockRequest{
		Path:        path,
		NodeID:      "node2",
		LockID:      "lock-a",
		LamportTime: 100,
	}
	var resp LockResponse
	err = CallUDP(ctx, secret, addr, TypRequestLock, req, &resp)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, resp.Status)
	assert.NotEmpty(t, resp.FencingToken)

	// 2. Try to request same lock from another node
	req2 := LockRequest{
		Path:        path,
		NodeID:      "node3",
		LockID:      "lock-b",
		LamportTime: 101,
	}
	var resp2 LockResponse
	err = CallUDP(ctx, secret, addr, TypRequestLock, req2, &resp2)
	assert.NoError(t, err)
	assert.Equal(t, LockReject, resp2.Status)

	// 3. Renew Lock
	renewReq := LockRenewal{
		Path:         path,
		NodeID:       "node2",
		LockID:       "lock-a",
		FencingToken: resp.FencingToken,
	}
	var status LockStatus
	err = CallUDP(ctx, secret, addr, TypRenewLock, renewReq, &status)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, status)

	// 4. Release Lock
	releaseReq := LockRelease{
		Path:         path,
		NodeID:       "node2",
		LockID:       "lock-a",
		FencingToken: resp.FencingToken,
	}
	err = CallUDP(ctx, secret, addr, TypReleaseLock, releaseReq, &status)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, status)

	// 5. Request again (should succeed)
	err = CallUDP(ctx, secret, addr, TypRequestLock, req2, &resp2)
	assert.NoError(t, err)
	assert.Equal(t, LockOK, resp2.Status)
}

func TestLockManager_SameNodeDifferentLockID(t *testing.T) {
	m := NewLockManager("node1")

	path := "same-node/path"

	// First acquisition from node2 with lock-a
	req := LockRequest{Path: path, NodeID: "node2", LockID: "lock-a", LamportTime: 100}
	var resp LockResponse
	_ = m.RequestLock(req, &resp)
	assert.Equal(t, LockOK, resp.Status)

	// Second acquisition from the SAME node but a different LockID must be
	// rejected while the first grant is unexpired.
	req2 := LockRequest{Path: path, NodeID: "node2", LockID: "lock-b", LamportTime: 101}
	var resp2 LockResponse
	_ = m.RequestLock(req2, &resp2)
	assert.Equal(t, LockReject, resp2.Status)

	// Idempotent retry of the same (NodeID, LockID) succeeds with a
	// consistent grant.
	var resp3 LockResponse
	_ = m.RequestLock(req, &resp3)
	assert.Equal(t, LockOK, resp3.Status)
	assert.Equal(t, resp.FencingToken, resp3.FencingToken)

	// The original holder's token is still valid for renewal.
	renewReq := LockRenewal{Path: path, NodeID: "node2", LockID: "lock-a", FencingToken: resp.FencingToken}
	var status LockStatus
	_ = m.RenewLock(renewReq, &status)
	assert.Equal(t, LockOK, status)
}

func TestLockManager_SweepExpiredGrants(t *testing.T) {
	m := NewLockManager("node1")
	m.LeaseTTL = 10 * time.Millisecond

	countGrants := func() int {
		n := 0
		m.grants.Range(func(_, _ interface{}) bool {
			n++
			return true
		})
		return n
	}

	// Grant two locks
	var resp LockResponse
	_ = m.RequestLock(LockRequest{Path: "sweep/a", NodeID: "node2", LockID: "lock-a", LamportTime: 100}, &resp)
	assert.Equal(t, LockOK, resp.Status)
	_ = m.RequestLock(LockRequest{Path: "sweep/b", NodeID: "node3", LockID: "lock-b", LamportTime: 101}, &resp)
	assert.Equal(t, LockOK, resp.Status)
	assert.Equal(t, 2, countGrants())

	// A sweep at the current time collects nothing: both grants are live.
	m.sweepExpiredGrants(time.Now())
	assert.Equal(t, 2, countGrants())

	// Advance "now" past expiry + slack (TTL + TTL); both grants are collected.
	m.sweepExpiredGrants(time.Now().Add(3 * m.LeaseTTL))
	assert.Equal(t, 0, countGrants())

	// A non-expired grant survives a sweep that collects an expired one.
	_ = m.RequestLock(LockRequest{Path: "sweep/expired", NodeID: "node2", LockID: "lock-c", LamportTime: 102}, &resp)
	assert.Equal(t, LockOK, resp.Status)
	time.Sleep(3 * m.LeaseTTL) // let it expire past the slack window
	_ = m.RequestLock(LockRequest{Path: "sweep/live", NodeID: "node2", LockID: "lock-d", LamportTime: 103}, &resp)
	assert.Equal(t, LockOK, resp.Status)

	m.sweepExpiredGrants(time.Now())
	assert.Equal(t, 1, countGrants())
	_, ok := m.grants.Load("sweep/live")
	assert.True(t, ok)
}

func TestCallUDP_DeadlineExceeded(t *testing.T) {
	// No server is listening on this port. On Linux a connected UDP socket
	// surfaces ECONNREFUSED via ICMP; CallUDP must keep retrying (the peer
	// could come up mid-retry) and fail only when the ctx expires.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	var resp LockResponse
	start := time.Now()
	err := CallUDP(ctx, []byte("test-secret-at-least-16-chars"), "127.0.0.1:1", TypRequestLock, LockRequest{Path: "p"}, &resp)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 1*time.Second, "CallUDP should return promptly after ctx expiry")
}

func TestLamportClock(t *testing.T) {
	c := &LamportClock{}
	assert.Equal(t, int64(1), c.Tick())
	assert.Equal(t, int64(2), c.Tick())

	assert.Equal(t, int64(11), c.Update(10))
	assert.Equal(t, int64(12), c.Tick())

	assert.Equal(t, int64(13), c.Update(5)) // Update with smaller time still ticks
}

func TestLockManager_RenewExpiredLease(t *testing.T) {
	m := NewLockManager("node1")
	m.LeaseTTL = 10 * time.Millisecond

	req := LockRequest{Path: "renew-expired", NodeID: "node2", LockID: "lock-a", LamportTime: 100}
	var resp LockResponse
	_ = m.RequestLock(req, &resp)
	assert.Equal(t, LockOK, resp.Status)

	// Renewal before expiry succeeds
	renewReq := LockRenewal{
		Path:         "renew-expired",
		NodeID:       "node2",
		LockID:       "lock-a",
		FencingToken: resp.FencingToken,
	}
	var status LockStatus
	_ = m.RenewLock(renewReq, &status)
	assert.Equal(t, LockOK, status)

	// Wait for the lease to expire
	time.Sleep(20 * time.Millisecond)

	// Renewal after expiry is rejected; holder must re-Acquire
	_ = m.RenewLock(renewReq, &status)
	assert.Equal(t, LockReject, status)

	// The expired grant was deleted, so another node can acquire the lock
	req2 := LockRequest{Path: "renew-expired", NodeID: "node3", LockID: "lock-b", LamportTime: 101}
	var resp2 LockResponse
	_ = m.RequestLock(req2, &resp2)
	assert.Equal(t, LockOK, resp2.Status)
}

func TestLockManager_LeaseExpiration(t *testing.T) {
	m := NewLockManager("node1")
	m.LeaseTTL = 100 * time.Millisecond

	req := LockRequest{Path: "expire", NodeID: "node2", LockID: "lock-a"}
	var resp LockResponse
	_ = m.RequestLock(req, &resp)

	assert.Equal(t, LockOK, resp.Status)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should be able to lock from another node now
	req2 := LockRequest{Path: "expire", NodeID: "node3", LockID: "lock-b"}
	var resp2 LockResponse
	_ = m.RequestLock(req2, &resp2)
	assert.Equal(t, LockOK, resp2.Status)
}
