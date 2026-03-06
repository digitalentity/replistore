package vfs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

	var mu sync.Mutex
	successes := 0
	grantedPeers := make([]string, 0)
	var fencingToken string

	g, gCtx := errgroup.WithContext(ctx)
	// Add timeout to the whole acquisition process
	gCtx, cancel := context.WithTimeout(gCtx, 3*time.Second)
	defer cancel()

	// 1. Local Request
	g.Go(func() error {
		var resp cluster.LockResponse
		err := l.Manager.RequestLock(req, &resp)
		if err == nil && resp.Status == cluster.LockOK {
			mu.Lock()
			successes++
			grantedPeers = append(grantedPeers, l.Manager.NodeID)
			if fencingToken == "" {
				fencingToken = resp.FencingToken
			}
			mu.Unlock()
		}
		return nil
	})

	// 2. Remote Requests
	for _, p := range peers {
		p := p
		g.Go(func() error {
			client, err := cluster.DialWithContext(gCtx, "tcp", p.Address)
			if err != nil {
				return nil
			}
			defer client.Close()

			var resp cluster.LockResponse
			err = cluster.CallWithContext(gCtx, client, "LockManager.RequestLock", req, &resp)
			if err == nil && resp.Status == cluster.LockOK {
				mu.Lock()
				successes++
				grantedPeers = append(grantedPeers, p.ID)
				if fencingToken == "" {
					fencingToken = resp.FencingToken
				}
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()

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
	
	var mu sync.Mutex
	successes := 0

	req := cluster.LockRenewal{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		FencingToken: l.FencingToken,
	}

	g, gCtx := errgroup.WithContext(context.Background())
	// Renewal should have a strict timeout
	gCtx, cancel := context.WithTimeout(gCtx, l.Manager.LeaseTTL/2)
	defer cancel()
	
	for _, pID := range peers {
		pID := pID
		g.Go(func() error {
			var status cluster.LockStatus
			var err error

			if pID == l.Manager.NodeID {
				err = l.Manager.RenewLock(req, &status)
			} else {
				p, ok := l.findPeer(pID)
				if !ok {
					return nil
				}
				client, dialErr := cluster.DialWithContext(gCtx, "tcp", p.Address)
				if dialErr != nil {
					return nil
				}
				defer client.Close()
				err = cluster.CallWithContext(gCtx, client, "LockManager.RenewLock", req, &status)
			}

			if err == nil && status == cluster.LockOK {
				mu.Lock()
				successes++
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()
	return successes >= quorum
}

func (l *DistributedLock) rollback(peers []string, token string) {
	req := cluster.LockRelease{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		FencingToken: token,
	}

	g, gCtx := errgroup.WithContext(context.Background())
	gCtx, cancel := context.WithTimeout(gCtx, 2*time.Second)
	defer cancel()

	for _, pID := range peers {
		pID := pID
		g.Go(func() error {
			var status cluster.LockStatus
			if pID == l.Manager.NodeID {
				_ = l.Manager.ReleaseLock(req, &status)
			} else {
				p, ok := l.findPeer(pID)
				if !ok {
					return nil
				}
				client, err := cluster.DialWithContext(gCtx, "tcp", p.Address)
				if err != nil {
					return nil
				}
				defer client.Close()
				_ = cluster.CallWithContext(gCtx, client, "LockManager.ReleaseLock", req, &status)
			}
			return nil
		})
	}
	_ = g.Wait()
}

func (l *DistributedLock) findPeer(id string) (cluster.Peer, bool) {
	for _, p := range l.Discovery.GetPeers() {
		if p.ID == id {
			return p, true
		}
	}
	return cluster.Peer{}, false
}
