package cluster

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
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
	Path        string
	NodeID      string
	LamportTime int64
}

type LockResponse struct {
	Status       LockStatus
	FencingToken string
}

type LockRenewal struct {
	Path         string
	NodeID       string
	FencingToken string
}

type LockRelease struct {
	Path         string
	NodeID       string
	FencingToken string
}

// Grant represents a lock granted by this node to a requester.
type Grant struct {
	NodeID       string
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
	NodeID   string
	Clock    *LamportClock
	LeaseTTL time.Duration

	grants sync.Map // path (string) -> Grant
	log    *logrus.Entry
	ln     net.Listener
}

func NewLockManager(nodeID string) *LockManager {
	return &LockManager{
		NodeID:   nodeID,
		Clock:    &LamportClock{},
		LeaseTTL: 5 * time.Second,
		log:      logrus.WithField("component", "lock-manager").WithField("node_id", nodeID),
	}
}

func (m *LockManager) Start(address string) (string, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return "", err
	}
	m.ln = ln
	actualAddr := ln.Addr().String()

	s := rpc.NewServer()
	// Export this LockManager instance as "LockManager" service
	if err := s.RegisterName("LockManager", m); err != nil {
		return "", err
	}

	go func() {
		for {
			conn, err := m.ln.Accept()
			if err != nil {
				return
			}
			go s.ServeConn(conn)
		}
	}()

	m.log.Infof("Lock RPC server listening on %s", actualAddr)
	return actualAddr, nil
}

func (m *LockManager) Stop() {
	if m.ln != nil {
		m.ln.Close()
	}
}

// --- RPC Methods (Exported) ---

func (m *LockManager) RequestLock(req LockRequest, resp *LockResponse) error {
	m.Clock.Update(req.LamportTime)
	now := time.Now()
	path := req.Path

	// Check if we've already granted this lock to someone else
	if val, ok := m.grants.Load(path); ok {
		grant := val.(Grant)
		if now.Before(grant.ExpiresAt) && grant.NodeID != req.NodeID {
			// Lock held by someone else
			resp.Status = LockReject
			return nil
		}
	}

	// Grant (or renew implicitly via Request) the lock
	// Fencing token is <LamportTime>-<NodeID>
	fencingToken := fmt.Sprintf("%d-%s", req.LamportTime, req.NodeID)
	m.grants.Store(path, Grant{
		NodeID:       req.NodeID,
		FencingToken: fencingToken,
		ExpiresAt:    now.Add(m.LeaseTTL),
	})

	resp.Status = LockOK
	resp.FencingToken = fencingToken
	return nil
}

func (m *LockManager) RenewLock(req LockRenewal, resp *LockStatus) error {
	now := time.Now()
	path := req.Path

	val, ok := m.grants.Load(path)
	if !ok {
		*resp = LockReject
		return nil
	}

	grant := val.(Grant)
	if grant.NodeID != req.NodeID || grant.FencingToken != req.FencingToken {
		// Not the owner or stale token
		*resp = LockReject
		return nil
	}

	// Extend lease
	grant.ExpiresAt = now.Add(m.LeaseTTL)
	m.grants.Store(path, grant)

	*resp = LockOK
	return nil
}

func (m *LockManager) ReleaseLock(req LockRelease, resp *LockStatus) error {
	path := req.Path

	val, ok := m.grants.Load(path)
	if !ok {
		// Already released or expired
		*resp = LockOK
		return nil
	}

	grant := val.(Grant)
	if grant.NodeID != req.NodeID || grant.FencingToken != req.FencingToken {
		// Not the owner or different token, but we still return OK because it's effectively released
		*resp = LockOK
		return nil
	}

	m.grants.Delete(path)
	*resp = LockOK
	return nil
}

// --- RPC Helpers ---

func DialWithContext(ctx context.Context, network, address string) (*rpc.Client, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func CallWithContext(ctx context.Context, client *rpc.Client, serviceMethod string, args interface{}, reply interface{}) error {
	done := make(chan error, 1)
	go func() {
		done <- client.Call(serviceMethod, args, reply)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}
