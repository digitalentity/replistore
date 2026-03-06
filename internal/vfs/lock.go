package vfs

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/sirupsen/logrus"
)

type DistributedLock struct {
	Path         string
	FencingToken string
	Manager      *cluster.LockManager
	Discovery    *cluster.Discovery

	isValid       bool
	mu            sync.RWMutex
	cancelRenewal context.CancelFunc
	renewalWg     sync.WaitGroup
	log           *logrus.Entry
}

func NewDistributedLock(path string, mgr *cluster.LockManager, disco *cluster.Discovery) *DistributedLock {
	return &DistributedLock{
		Path:      path,
		Manager:   mgr,
		Discovery: disco,
		log:       logrus.WithField("component", "dist-lock").WithField("path", path),
	}
}

func (l *DistributedLock) IsValid() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isValid
}

// Acquire attempts to get a quorum of peers to grant the lock.
func (l *DistributedLock) Acquire(ctx context.Context) error {
	peers := l.Discovery.GetPeers()
	n := len(peers) + 1 // +1 for local node
	quorum := (n / 2) + 1

	l.log.Debugf("Attempting to acquire lock with %d peers (quorum: %d)", n-1, quorum)

	lamportTime := l.Manager.Clock.Tick()
	req := cluster.LockRequest{
		Path:        l.Path,
		NodeID:      l.Manager.NodeID,
		LamportTime: lamportTime,
	}

	type result struct {
		peerID string
		resp   cluster.LockResponse
		err    error
	}

	results := make(chan result, n)
	
	// 1. Local Request
	go func() {
		var resp cluster.LockResponse
		err := l.Manager.RequestLock(req, &resp)
		results <- result{peerID: l.Manager.NodeID, resp: resp, err: err}
	}()

	// 2. Remote Requests
	for _, p := range peers {
		go func(p cluster.Peer) {
			// net/rpc dial doesn't take context, so we dial with a short timeout
			conn, err := net.DialTimeout("tcp", p.Address, 1*time.Second)
			if err != nil {
				results <- result{peerID: p.ID, err: err}
				return
			}
			client := rpc.NewClient(conn)
			defer client.Close()

			var resp cluster.LockResponse
			err = client.Call("LockManager.RequestLock", req, &resp)
			results <- result{peerID: p.ID, resp: resp, err: err}
		}(p)
	}

	successes := 0
	grantedPeers := make([]string, 0)
	var fencingToken string

	// Wait for quorum or timeout
	timeout := time.After(2 * time.Second)

	for i := 0; i < n; i++ {
		select {
		case res := <-results:
			if res.err == nil && res.resp.Status == cluster.LockOK {
				successes++
				grantedPeers = append(grantedPeers, res.peerID)
				if fencingToken == "" {
					fencingToken = res.resp.FencingToken
				}
			}
		case <-timeout:
			break
		case <-ctx.Done():
			return ctx.Err()
		}
		if successes >= quorum {
			break
		}
	}

	if successes >= quorum {
		l.mu.Lock()
		l.isValid = true
		l.FencingToken = fencingToken
		l.mu.Unlock()
		l.startRenewal(grantedPeers)
		return nil
	}

	// Rollback if quorum not reached
	l.log.Warnf("Failed to reach quorum (%d/%d), rolling back", successes, quorum)
	l.rollback(grantedPeers, fencingToken)
	return fmt.Errorf("failed to acquire distributed lock")
}

func (l *DistributedLock) Release() {
	l.mu.Lock()
	l.isValid = false
	l.mu.Unlock()
	if l.cancelRenewal != nil {
		l.cancelRenewal()
		l.renewalWg.Wait()
	}
}

func (l *DistributedLock) startRenewal(peers []string) {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancelRenewal = cancel
	l.renewalWg.Add(1)

	go func() {
		defer l.renewalWg.Done()
		ticker := time.NewTicker(l.Manager.LeaseTTL / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.rollback(peers, l.FencingToken)
				return
			case <-ticker.C:
				if !l.renew(peers) {
					l.log.Error("Failed to renew lease quorum, lock lost")
					l.mu.Lock()
					l.isValid = false
					l.mu.Unlock()
					return
				}
			}
		}
	}()
}

func (l *DistributedLock) renew(peers []string) bool {
	n := len(peers)
	quorum := (n / 2) + 1
	successes := 0

	req := cluster.LockRenewal{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		FencingToken: l.FencingToken,
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	
	for _, pID := range peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			var status cluster.LockStatus
			var err error

			if peerID == l.Manager.NodeID {
				err = l.Manager.RenewLock(req, &status)
			} else {
				p, ok := l.findPeer(peerID)
				if !ok {
					return
				}
				client, dialErr := rpc.Dial("tcp", p.Address)
				if dialErr != nil {
					return
				}
				defer client.Close()
				err = client.Call("LockManager.RenewLock", req, &status)
			}

			if err == nil && status == cluster.LockOK {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(pID)
	}

	wg.Wait()
	return successes >= quorum
}

func (l *DistributedLock) rollback(peers []string, token string) {
	req := cluster.LockRelease{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		FencingToken: token,
	}

	for _, pID := range peers {
		go func(peerID string) {
			var status cluster.LockStatus
			if peerID == l.Manager.NodeID {
				l.Manager.ReleaseLock(req, &status)
			} else {
				p, ok := l.findPeer(peerID)
				if !ok {
					return
				}
				client, err := rpc.Dial("tcp", p.Address)
				if err != nil {
					return
				}
				defer client.Close()
				client.Call("LockManager.ReleaseLock", req, &status)
			}
		}(pID)
	}
}

func (l *DistributedLock) findPeer(id string) (cluster.Peer, bool) {
	for _, p := range l.Discovery.GetPeers() {
		if p.ID == id {
			return p, true
		}
	}
	return cluster.Peer{}, false
}
