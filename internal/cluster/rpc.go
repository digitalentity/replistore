package cluster

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type LockStatus string

const (
	LockOK     LockStatus = "OK"
	LockReject LockStatus = "REJECT"
)

type LockRequest struct {
	Path         string
	NodeID       string
	LockID       string
	LamportTime  int64
	FencingToken string
}

type LockResponse struct {
	Status       LockStatus
	FencingToken string
}

type LockRenewal struct {
	Path         string
	NodeID       string
	LockID       string
	FencingToken string
}

type LockRelease struct {
	Path         string
	NodeID       string
	LockID       string
	FencingToken string
}

// Grant represents a lock granted by this node to a requester.
type Grant struct {
	NodeID       string
	LockID       string
	FencingToken string
	ExpiresAt    time.Time
}

// LamportClock provides atomic logical time operations.
type LamportClock struct {
	time int64
}

func (c *LamportClock) Tick() int64 {
	return atomic.AddInt64(&c.time, 1)
}

func (c *LamportClock) Update(received int64) int64 {
	for {
		local := atomic.LoadInt64(&c.time)
		next := local
		if received > local {
			next = received
		}
		next++
		if atomic.CompareAndSwapInt64(&c.time, local, next) {
			return next
		}
	}
}

func (c *LamportClock) Get() int64 {
	return atomic.LoadInt64(&c.time)
}

// LockManager handles the local lock table and Lamport clock for a node.
type LockManager struct {
	NodeID              string
	Clock               *LamportClock
	LeaseTTL            time.Duration
	ExpectedClusterSize int
	Secret              []byte // shared cluster secret for HMAC-signing lock datagrams

	grants   sync.Map // path (string) -> Grant
	log      *logrus.Entry
	conn     *net.UDPConn
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewLockManager(nodeID string) *LockManager {
	return &LockManager{
		NodeID:   nodeID,
		Clock:    &LamportClock{},
		LeaseTTL: 5 * time.Second,
		log:      logrus.WithField("component", "lock-manager").WithField("node_id", nodeID),
		stopCh:   make(chan struct{}),
	}
}

func (m *LockManager) Start(address string) (string, error) {
	laddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return "", err
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return "", err
	}
	m.conn = conn
	actualAddr := conn.LocalAddr().String()

	go m.serveLoop()
	go m.janitorLoop()

	m.log.Infof("Lock UDP server listening on %s", actualAddr)
	return actualAddr, nil
}

func (m *LockManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	if m.conn != nil {
		m.conn.Close()
	}
}

// janitorLoop periodically removes long-expired grants so the grants map
// does not grow without bound on long-running nodes.
func (m *LockManager) janitorLoop() {
	ticker := time.NewTicker(m.LeaseTTL)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-ticker.C:
			m.sweepExpiredGrants(now)
		}
	}
}

// sweepExpiredGrants deletes grants that expired more than one LeaseTTL ago.
// The extra TTL of slack ensures a grant mid-renewal is never collected
// prematurely.
func (m *LockManager) sweepExpiredGrants(now time.Time) {
	slack := m.LeaseTTL
	m.grants.Range(func(key, val interface{}) bool {
		grant := val.(Grant)
		if now.After(grant.ExpiresAt.Add(slack)) {
			// CompareAndDelete so a grant re-issued for this path between the
			// Range read and the delete is never collected.
			m.grants.CompareAndDelete(key, val)
		}
		return true
	})
}

// --- RPC Methods (Exported) ---

func (m *LockManager) RequestLock(req LockRequest, resp *LockResponse) error {
	m.Clock.Update(req.LamportTime)
	now := time.Now()
	path := req.Path
	log := m.log.WithField("path", path)

	// Check if we've already granted this lock to someone else
	if val, ok := m.grants.Load(path); ok {
		grant := val.(Grant)
		if now.Before(grant.ExpiresAt) && (grant.NodeID != req.NodeID || grant.LockID != req.LockID) {
			// Lock held by another acquisition (different node or different
			// lock instance on the same node)
			log.Warnf("Rejecting lock request from node %s (lock ID %s): held by node %s (lock ID %s, expires in %v)", req.NodeID, req.LockID, grant.NodeID, grant.LockID, grant.ExpiresAt.Sub(now))
			resp.Status = LockReject
			return nil
		}
	}

	// The fencing token is an unguessable random value minted by the client
	// once per acquisition (see vfs.NewDistributedLock) and carried unchanged
	// across peers and renewals. Reject requests without one rather than
	// granting a forgeable lease.
	fencingToken := req.FencingToken
	if fencingToken == "" {
		log.Warnf("Rejecting lock request from node %s (lock ID %s): missing fencing token", req.NodeID, req.LockID)
		resp.Status = LockReject
		return nil
	}
	// Grant (or refresh, for an idempotent retry of the same acquisition) the lock.
	m.grants.Store(path, Grant{
		NodeID:       req.NodeID,
		LockID:       req.LockID,
		FencingToken: fencingToken,
		ExpiresAt:    now.Add(m.LeaseTTL),
	})

	log.Infof("Granted lock request to node %s (lock ID %s, token %s)", req.NodeID, req.LockID, fencingToken)
	resp.Status = LockOK
	resp.FencingToken = fencingToken
	return nil
}

func (m *LockManager) RenewLock(req LockRenewal, resp *LockStatus) error {
	now := time.Now()
	path := req.Path
	log := m.log.WithField("path", path)

	val, ok := m.grants.Load(path)
	if !ok {
		log.Warnf("Rejecting lock renewal from node %s (lock ID %s): no active grant found", req.NodeID, req.LockID)
		*resp = LockReject
		return nil
	}

	grant := val.(Grant)
	if grant.NodeID != req.NodeID || grant.LockID != req.LockID || grant.FencingToken != req.FencingToken {
		// Not the owner or stale token
		log.Warnf("Rejecting lock renewal from node %s (lock ID %s): owner mismatch (held by node %s, lock ID %s)", req.NodeID, req.LockID, grant.NodeID, grant.LockID)
		*resp = LockReject
		return nil
	}

	if !now.Before(grant.ExpiresAt) {
		// Lease already expired; the holder must re-Acquire so conflict
		// checks run again. Treat the expired grant as gone.
		m.grants.Delete(path)
		log.Warnf("Rejecting lock renewal from node %s (lock ID %s): lease expired at %v", req.NodeID, req.LockID, grant.ExpiresAt)
		*resp = LockReject
		return nil
	}

	// Extend lease
	grant.ExpiresAt = now.Add(m.LeaseTTL)
	m.grants.Store(path, grant)

	log.Debugf("Renewed lock for node %s (lock ID %s, expires in %v)", req.NodeID, req.LockID, m.LeaseTTL)
	*resp = LockOK
	return nil
}

func (m *LockManager) ReleaseLock(req LockRelease, resp *LockStatus) error {
	path := req.Path
	log := m.log.WithField("path", path)

	val, ok := m.grants.Load(path)
	if !ok {
		// Already released or expired
		log.Debugf("Lock release request from node %s (lock ID %s): already released or expired", req.NodeID, req.LockID)
		*resp = LockOK
		return nil
	}

	grant := val.(Grant)
	if grant.NodeID != req.NodeID || grant.LockID != req.LockID || grant.FencingToken != req.FencingToken {
		// Not the owner or different token, but we still return OK because it's effectively released
		log.Debugf("Lock release request from node %s (lock ID %s): ownership mismatch, returning OK", req.NodeID, req.LockID)
		*resp = LockOK
		return nil
	}

	m.grants.Delete(path)
	log.Infof("Released lock for node %s (lock ID %s)", req.NodeID, req.LockID)
	*resp = LockOK
	return nil
}
