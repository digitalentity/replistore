package vfs

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type DistributedLock struct {
	Path         string
	LockID       string
	FencingToken string
	Manager      *cluster.LockManager
	Discovery    *cluster.Discovery

	isValid       bool
	expiresAt     time.Time
	mu            sync.RWMutex
	cancelRenewal context.CancelFunc
	renewalWg     sync.WaitGroup
	log           *logrus.Entry
}

func NewDistributedLock(path string, mgr *cluster.LockManager, disco *cluster.Discovery) *DistributedLock {
	return &DistributedLock{
		Path: path,
		// LockID identifies the acquisition; FencingToken is the unguessable
		// capability that authorizes renew/release. Both are minted once here
		// and carried unchanged across every peer and renewal round so the
		// holder presents the same token to all peers (see tryAcquire).
		LockID:       randomHex(),
		FencingToken: randomHex(),
		Manager:      mgr,
		Discovery:    disco,
		log:          logrus.WithField("component", "dist-lock").WithField("path", path),
	}
}

// randomHex returns 16 random bytes, hex-encoded.
func randomHex() string {
	b := make([]byte, 16)
	if _, err := cryptorand.Read(b); err != nil {
		// crypto/rand never fails on supported platforms; fall back to a
		// time-derived value rather than panicking.
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func (l *DistributedLock) IsValid() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isValid
}

func (l *DistributedLock) ExpiresAt() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.expiresAt
}

func (l *DistributedLock) IsValidWithBuffer(buffer time.Duration) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.isValid {
		return false
	}
	return time.Now().Add(buffer).Before(l.expiresAt)
}

const (
	acquireMaxAttempts = 4
	acquireBaseBackoff = 50 * time.Millisecond
	acquireMaxBackoff  = 400 * time.Millisecond

	// renewalCadenceDivisor sets the renewal interval to LeaseTTL/3, giving
	// roughly three renewal attempts per lease before the deadline passes.
	renewalCadenceDivisor = 3
)

// Acquire attempts to get a quorum of peers to grant the lock. On quorum
// failure it retries with randomized backoff, up to acquireMaxAttempts total
// attempts, while ctx remains live. Every attempt reuses the same LockID
// (peers treat repeated requests for the same (NodeID, LockID) idempotently)
// but takes a fresh Lamport tick.
func (l *DistributedLock) Acquire(ctx context.Context) error {
	backoff := acquireBaseBackoff
	var lastErr error

	for attempt := 1; attempt <= acquireMaxAttempts; attempt++ {
		lastErr = l.tryAcquire(ctx)
		if lastErr == nil {
			l.log.Infof("Successfully acquired distributed lock (fencing token: %s)", l.FencingToken)
			return nil
		}
		if attempt == acquireMaxAttempts {
			break
		}

		// Full jitter: sleep a random duration in [0, backoff).
		wait := time.Duration(rand.Int63n(int64(backoff)))
		backoff *= 2
		if backoff > acquireMaxBackoff {
			backoff = acquireMaxBackoff
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

// tryAcquire performs a single quorum acquisition attempt with its own
// per-attempt timeout.
func (l *DistributedLock) tryAcquire(ctx context.Context) error {
	peers := l.Discovery.GetPeers()
	quorum := (l.Manager.ExpectedClusterSize / 2) + 1

	l.log.Debugf("Attempting to acquire lock with %d peers (quorum: %d)", len(peers), quorum)

	lamportTime := l.Manager.Clock.Tick()
	req := cluster.LockRequest{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		LockID:       l.LockID,
		LamportTime:  lamportTime,
		FencingToken: l.FencingToken,
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
			var resp cluster.LockResponse
			err := cluster.CallUDP(gCtx, l.Manager.Secret, p.Address, cluster.TypRequestLock, req, &resp)
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
		l.expiresAt = time.Now().Add(l.Manager.LeaseTTL)
		l.mu.Unlock()
		l.startRenewal(grantedPeers)
		return nil
	}

	// Rollback if quorum not reached
	l.log.Warnf("Failed to reach quorum (%d/%d), rolling back", successes, quorum)
	l.rollback(grantedPeers, fencingToken)
	return errors.New("failed to acquire distributed lock")
}

func (l *DistributedLock) Release() {
	l.log.Info("Releasing distributed lock")
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

	l.log.Debugf("Starting background lock renewal for peers: %v", peers)
	go func() {
		defer l.renewalWg.Done()
		// Renew on a faster cadence than the lease lasts (TTL/3) so a single
		// transient failure leaves room for two more attempts before the lease
		// deadline. A missed round does NOT immediately surrender the lock: the
		// lease is only declared lost once its deadline (expiresAt, advanced on
		// every successful round) actually passes without a quorum renewal.
		ticker := time.NewTicker(l.Manager.LeaseTTL / renewalCadenceDivisor)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.rollback(peers, l.FencingToken)
				return
			case <-ticker.C:
				if l.renew(peers) {
					continue
				}
				if time.Now().After(l.ExpiresAt()) {
					l.log.Error("Lease deadline passed without a quorum renewal, lock lost")
					l.mu.Lock()
					l.isValid = false
					l.mu.Unlock()
					return
				}
				l.log.Warnf("Renewal round missed quorum, retrying before lease deadline (%v)", l.ExpiresAt())
			}
		}
	}()
}

func (l *DistributedLock) renew(peers []string) bool {
	quorum := (l.Manager.ExpectedClusterSize / 2) + 1

	var mu sync.Mutex
	successes := 0

	req := cluster.LockRenewal{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		LockID:       l.LockID,
		FencingToken: l.FencingToken,
	}

	g, gCtx := errgroup.WithContext(context.Background())
	// Bound each round to the renewal cadence so a hung round cannot eat the
	// grace window the retry loop relies on.
	gCtx, cancel := context.WithTimeout(gCtx, l.Manager.LeaseTTL/renewalCadenceDivisor)
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
				err = cluster.CallUDP(gCtx, l.Manager.Secret, p.Address, cluster.TypRenewLock, req, &status)
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
	ok := successes >= quorum
	if ok {
		l.mu.Lock()
		l.expiresAt = time.Now().Add(l.Manager.LeaseTTL)
		l.mu.Unlock()
		l.log.Debugf("Successfully renewed distributed lock lease (%d/%d)", successes, quorum)
	}
	return ok
}

func (l *DistributedLock) rollback(peers []string, token string) {
	req := cluster.LockRelease{
		Path:         l.Path,
		NodeID:       l.Manager.NodeID,
		LockID:       l.LockID,
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
				_ = cluster.CallUDP(gCtx, l.Manager.Secret, p.Address, cluster.TypReleaseLock, req, &status)
			}
			return nil
		})
	}
	_ = g.Wait()
}

func (l *DistributedLock) findPeer(id string) (cluster.Peer, bool) {
	return l.Discovery.GetPeer(id)
}
