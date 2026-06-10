package fuse

import (
	"testing"
	"time"
)

// TestPathLocksSamePathBlocks verifies that a second lock on the same path
// blocks until the first holder unlocks.
func TestPathLocksSamePathBlocks(t *testing.T) {
	var p pathLocks

	unlock1 := p.lock("a/b")

	acquired := make(chan struct{})
	go func() {
		unlock2 := p.lock("a/b")
		close(acquired)
		unlock2()
	}()

	select {
	case <-acquired:
		t.Fatal("second lock acquired while first was still held")
	case <-time.After(50 * time.Millisecond):
		// Expected: second goroutine is blocked.
	}

	unlock1()

	select {
	case <-acquired:
		// Expected: second goroutine acquired after the first unlocked.
	case <-time.After(2 * time.Second):
		t.Fatal("second lock was not acquired after first unlock")
	}
}

// TestPathLocksDifferentPathsIndependent verifies that locks on different
// paths do not block each other.
func TestPathLocksDifferentPathsIndependent(t *testing.T) {
	var p pathLocks

	unlockA := p.lock("a")
	defer unlockA()

	acquired := make(chan struct{})
	go func() {
		unlockB := p.lock("b")
		close(acquired)
		unlockB()
	}()

	select {
	case <-acquired:
		// Expected: "b" is independent of "a".
	case <-time.After(2 * time.Second):
		t.Fatal("lock on a different path blocked")
	}
}

// TestPathLocksEntryRemovedAfterLastUnlock verifies that map entries are
// reference-counted and removed once the last holder/waiter unlocks.
func TestPathLocksEntryRemovedAfterLastUnlock(t *testing.T) {
	var p pathLocks

	unlock1 := p.lock("x")

	second := make(chan func(), 1)
	go func() {
		second <- p.lock("x")
	}()

	// Wait until the second goroutine has registered as a waiter.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		e := p.locks["x"]
		refs := 0
		if e != nil {
			refs = e.refs
		}
		p.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second waiter never registered, refs=%d", refs)
		}
		time.Sleep(time.Millisecond)
	}

	unlock1()

	// Entry must survive while the second holder still references it.
	unlock2 := <-second
	p.mu.Lock()
	_, present := p.locks["x"]
	p.mu.Unlock()
	if !present {
		t.Fatal("entry removed while still held by second locker")
	}

	unlock2()

	p.mu.Lock()
	_, present = p.locks["x"]
	n := len(p.locks)
	p.mu.Unlock()
	if present {
		t.Fatal("entry not removed after last unlock")
	}
	if n != 0 {
		t.Fatalf("lock table not empty after all unlocks: %d entries", n)
	}
}
