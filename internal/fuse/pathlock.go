package fuse

import "sync"

// pathLocks is an in-process per-path lock table. It serializes mutating
// operations (create/mkdir/remove/rename/open-for-write/repair) on the same
// path within this process, complementing the distributed lock manager (which
// may be disabled in single-node deployments, and which rejects rather than
// queues a second acquisition from the same node).
//
// The table is intentionally simple: no TryLock and no context cancellation.
// FUSE operations are already bounded by the kernel, and repair copies are
// bounded by backend I/O timeouts, so blocking on the entry mutex is fine.
//
// The zero value is ready to use; the map is initialized lazily under mu so
// pathLocks can be embedded by value in a struct built with a struct literal.
type pathLocks struct {
	mu    sync.Mutex
	locks map[string]*pathLockEntry
}

type pathLockEntry struct {
	mu   sync.Mutex
	refs int
}

// lock acquires the per-path lock, blocking until it is available, and
// returns a closure that releases it. Entries are reference-counted and
// removed once the last holder/waiter unlocks, so the map size is bounded by
// the number of concurrently locked paths.
func (p *pathLocks) lock(path string) func() {
	p.mu.Lock()
	if p.locks == nil {
		p.locks = make(map[string]*pathLockEntry)
	}
	e, ok := p.locks[path]
	if !ok {
		e = &pathLockEntry{}
		p.locks[path] = e
	}
	e.refs++
	p.mu.Unlock()

	e.mu.Lock()

	return func() {
		e.mu.Unlock()
		p.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(p.locks, path)
		}
		p.mu.Unlock()
	}
}
