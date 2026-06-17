package cluster

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"
)

type LockStatus string

const (
	LockOK          LockStatus = "OK"
	LockReject      LockStatus = "REJECT"
	defaultLeaseTTL            = 5 * time.Second
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
	time atomic.Int64
}

func (c *LamportClock) Tick() int64 {
	return c.time.Add(1)
}

func (c *LamportClock) Update(received int64) int64 {
	for {
		local := c.time.Load()
		next := max(received, local)
		next++
		if c.time.CompareAndSwap(local, next) {
			return next
		}
	}
}

func (c *LamportClock) Get() int64 {
	return c.time.Load()
}

// LockManager handles the local lock table and Lamport clock for a node.
type LockManager struct {
	NodeID              string
	Clock               *LamportClock
	LeaseTTL            time.Duration
	ExpectedClusterSize int
	Secret              []byte // shared cluster secret for HMAC-signing lock datagrams

	grants   sync.Map // path (string) -> Grant
	log      *slog.Logger
	conn     *net.UDPConn
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewLockManager(nodeID string) *LockManager {
	return &LockManager{
		NodeID:   nodeID,
		Clock:    &LamportClock{},
		LeaseTTL: defaultLeaseTTL,
		log:      slog.Default().With(slog.String("component", "lock-manager"), slog.String("node_id", nodeID)),
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

	m.log.Info("Lock UDP server listening", slog.String("address", actualAddr))

	return actualAddr, nil
}

func (m *LockManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	if m.conn != nil {
		_ = m.conn.Close()
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
	m.grants.Range(func(key, val any) bool {
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
	log := m.log.With(slog.String("path", path))

	// Check if we've already granted this lock to someone else
	if val, ok := m.grants.Load(path); ok {
		grant := val.(Grant)
		if now.Before(grant.ExpiresAt) && (grant.NodeID != req.NodeID || grant.LockID != req.LockID) {
			// Lock held by another acquisition (different node or different
			// lock instance on the same node)
			log.Warn("Rejecting lock request: held by another node",
				slog.String("req_node_id", req.NodeID),
				slog.String("req_lock_id", req.LockID),
				slog.String("holder_node_id", grant.NodeID),
				slog.String("holder_lock_id", grant.LockID),
				slog.Duration("expires_in", grant.ExpiresAt.Sub(now)),
			)
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
		log.Warn("Rejecting lock request: missing fencing token",
			slog.String("req_node_id", req.NodeID),
			slog.String("req_lock_id", req.LockID),
		)
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

	log.Info("Granted lock request",
		slog.String("req_node_id", req.NodeID),
		slog.String("req_lock_id", req.LockID),
		slog.String("fencing_token", fencingToken),
	)
	resp.Status = LockOK
	resp.FencingToken = fencingToken

	return nil
}

func (m *LockManager) RenewLock(req LockRenewal, resp *LockStatus) error {
	now := time.Now()
	path := req.Path
	log := m.log.With(slog.String("path", path))

	val, ok := m.grants.Load(path)
	if !ok {
		log.Warn("Rejecting lock renewal: no active grant found",
			slog.String("req_node_id", req.NodeID),
			slog.String("req_lock_id", req.LockID),
		)
		*resp = LockReject

		return nil
	}

	grant := val.(Grant)
	if grant.NodeID != req.NodeID || grant.LockID != req.LockID || grant.FencingToken != req.FencingToken {
		// Not the owner or stale token
		log.Warn("Rejecting lock renewal: owner mismatch",
			slog.String("req_node_id", req.NodeID),
			slog.String("req_lock_id", req.LockID),
			slog.String("holder_node_id", grant.NodeID),
			slog.String("holder_lock_id", grant.LockID),
		)
		*resp = LockReject

		return nil
	}

	if !now.Before(grant.ExpiresAt) {
		// Lease already expired; the holder must re-Acquire so conflict
		// checks run again. Treat the expired grant as gone.
		m.grants.Delete(path)
		log.Warn("Rejecting lock renewal: lease expired",
			slog.String("req_node_id", req.NodeID),
			slog.String("req_lock_id", req.LockID),
			slog.Time("expired_at", grant.ExpiresAt),
		)
		*resp = LockReject

		return nil
	}

	// Extend lease
	grant.ExpiresAt = now.Add(m.LeaseTTL)
	m.grants.Store(path, grant)

	log.Debug("Renewed lock",
		slog.String("req_node_id", req.NodeID),
		slog.String("req_lock_id", req.LockID),
		slog.Duration("expires_in", m.LeaseTTL),
	)
	*resp = LockOK

	return nil
}

func (m *LockManager) ReleaseLock(req LockRelease, resp *LockStatus) error {
	path := req.Path
	log := m.log.With(slog.String("path", path))

	val, ok := m.grants.Load(path)
	if !ok {
		// Already released or expired
		log.Debug("Lock release request: already released or expired",
			slog.String("req_node_id", req.NodeID),
			slog.String("req_lock_id", req.LockID),
		)
		*resp = LockOK

		return nil
	}

	grant := val.(Grant)
	if grant.NodeID != req.NodeID || grant.LockID != req.LockID || grant.FencingToken != req.FencingToken {
		// Not the owner or different token, but we still return OK because it's effectively released
		log.Debug("Lock release request: ownership mismatch, returning OK",
			slog.String("req_node_id", req.NodeID),
			slog.String("req_lock_id", req.LockID),
		)
		*resp = LockOK

		return nil
	}

	m.grants.Delete(path)
	log.Info("Released lock",
		slog.String("req_node_id", req.NodeID),
		slog.String("req_lock_id", req.LockID),
	)
	*resp = LockOK

	return nil
}
