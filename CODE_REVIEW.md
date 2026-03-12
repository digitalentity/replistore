# Code Review

Issues organized by severity. File references use `file:line` notation.

---

## Critical

### 1. `Create` does not roll back files on write-quorum failure

**`internal/fuse/fs.go:196-199`**

When `len(successfulBackends) < d.fs.WriteQuorum`, the code calls `h.Release(ctx, nil)` which only closes the open file handles — it does not remove the partially created files from the backends that succeeded. Those files are left as orphaned garbage on the backends.

Compare with `Mkdir` (line 274-277), which explicitly calls `b.Remove()` for every backend that succeeded before returning the error. `Create` needs the same rollback loop.

---

### 2. `RandomSelector` is not goroutine-safe

**`internal/vfs/selector.go:16,38,91`**

`RandomSelector` holds a `*rand.Rand` (from `math/rand`) which is explicitly not safe for concurrent use. `SelectForRead` and `SelectForWrite` are called concurrently from multiple FUSE goroutines, so there is a data race on the internal state of `s.r`.

Fix: replace with `rand.New(rand.NewSource(...))` protected by a mutex, or use the global `rand` functions (which are goroutine-safe since Go 1.20), or use `math/rand/v2`.

---

### 3. Renewal quorum is computed against granted peers, not total cluster size

**`internal/vfs/lock.go:163-165`**

```go
func (l *DistributedLock) renew(peers []string) bool {
    n := len(peers)       // peers = originally granted nodes only
    quorum := (n / 2) + 1
```

`peers` is the list of nodes that granted the lock at acquisition time, not the total cluster size. If the cluster has 5 nodes and the lock was granted by exactly 3 (the minimum quorum), `renew` uses quorum = `(3/2)+1 = 2`. The lock can be renewed with only 2 out of the original 3 granted nodes responding, which is below the acquisition cluster quorum of 3. A partition where 2 nodes are isolated can then lead to split-brain — both sides believe they hold the lock.

Fix: `Acquire` should pass the full cluster size `n` (not just `len(grantedPeers)`) to `startRenewal`, and `renew` should compute quorum against that value.

---

## High

### 4. `CallWithContext` goroutine leak on context cancellation

**`internal/cluster/rpc.go:222-234`**

```go
func CallWithContext(ctx context.Context, client *rpc.Client, ...) error {
    done := make(chan error, 1)
    go func() {
        done <- client.Call(serviceMethod, args, reply)
    }()
    select {
    case <-ctx.Done():
        return ctx.Err()   // goroutine above is now orphaned
    case err := <-done:
        return err
    }
}
```

When the context is cancelled, the function returns immediately but the goroutine calling `client.Call()` keeps running — `net/rpc` has no cancellation. The goroutine leaks until the RPC completes or the underlying TCP connection times out. Under high lock contention with short timeouts, this can accumulate many leaked goroutines.

Fix: close `client` on context cancellation to unblock the pending `Call` (closing an `rpc.Client` causes any in-flight call to return with an error).

---

### 5. `Open` for write opens backends sequentially

**`internal/fuse/fs.go:539-547`**

```go
} else {
    for _, bName := range backends {
        b := f.fs.Backends[bName]
        sf, err := b.OpenFile(ctx, path, int(req.Flags), 0)
        if err != nil {
            _ = h.Release(ctx, nil)
            return nil, err
        }
        h.backends[bName] = sf
    }
}
```

Write opens are sequential. `Create` (line 176-193) opens backends in parallel using `errgroup`. For a replication factor of 3 with SMB backends across a network, sequential opens add unnecessary latency proportional to `RF × round-trip-time`.

Fix: use the same `errgroup` fan-out pattern used in `Create`.

---

### 6. `sweep` holds parent write-lock for entire subtree recursion

**`internal/vfs/cache.go:267-297`**

`sweep` acquires `node.Mu.Lock()` via `defer node.Mu.Unlock()` at the start, then calls `c.sweep(child, ...)` recursively for every child while still holding the parent's write lock. For a deep or wide directory tree, this means the root node's write lock is held for the entire duration of the scan. Any concurrent FUSE operation that needs to read even the root node (via `node.Mu.RLock()`) is blocked for the full scan duration.

Additionally, after the recursive `sweep(child)` returns (releasing `child.Mu` via that call's own defer), the parent immediately calls `child.Mu.Lock()` again (line 279) to do the pruning check. This two-phase lock on the child is fragile — it creates a window between the recursive unlock and the re-lock where another goroutine can modify the child.

Fix: Collect nodes to prune during the recursive scan (without locking), then apply deletions in a second pass. Or restructure to avoid holding the parent lock during child recursion.

---

### 7. `Remove` only removes from known backends

**`internal/fuse/fs.go:315-316`**

`Remove` builds the list of backends to remove from using `child.Meta.Backends`, which is the cache's view of where the file lives. If the cache is stale or a backend has a copy that was written outside of RepliStore (or after a partial write), `Remove` won't touch it. The orphan copy remains on that backend and will reappear in the cache at the next sync.

This is a fundamental consistency issue. At minimum, `Remove` should attempt removal on all healthy backends, not just the ones listed in the cache.

---

## Medium

### 8. `Node` exposes its mutex and internal fields as exported

**`internal/vfs/cache.go:25-35`**

```go
type Node struct {
    Meta          Metadata
    Children      map[string]*Node
    FullyIndexed  bool
    Mu            sync.RWMutex
}
```

All fields including the mutex are exported. The `fuse` package acquires `node.Mu` directly (e.g., `fs.go:59,69,101,133`), accesses `node.Children` directly, and mutates `node.Meta` directly. This bypasses the cache's own locking abstractions. The `Cache` struct has the same issue with its exported `Root` and `Mu` fields.

Consequence: there is no single place to enforce locking discipline. Any future change to the locking strategy requires auditing all callers.

Fix: unexport the fields and expose deliberate accessor/mutator methods on `Cache` for all cross-package operations.

---

### 9. `Create` has a TOCTOU on the name-exists check

**`internal/fuse/fs.go:133-141` and `206-209`**

The code checks for an existing child at line 134, releases the lock at line 141 to do I/O, then re-acquires the lock at line 202 to check again at line 206. This double-check pattern is necessary and correct in principle. However, between lines 196-199 (failed quorum path), the code calls `h.Release` and returns the error without checking whether the conflict check (line 206) would have fired — but at that point the write already failed so the node was never added to the cache anyway. The logic is sound but the ordering is non-obvious and the two distinct check sites are easy to misread as redundant.

---

### 10. Ignored errors without explanation

**`cmd/replistore/main.go:59,71`**

```go
nodeID, _ := os.Hostname()   // line 59

_, portStr, _ := net.SplitHostPort(actualAddr)  // line 70
port, _ := strconv.Atoi(portStr)                // line 71
```

`os.Hostname()` error is handled (there's a fallback), but the pattern `nodeID, _ :=` without a comment suggests the error is simply discarded. More critically, `strconv.Atoi(portStr)` on line 71: if `SplitHostPort` failed (line 70 also discards its error), `portStr` is empty and `port` is silently 0. `disco = cluster.NewDiscovery(nodeID, 0)` would advertise an unusable address to peers. Both lines should handle or explicitly document why the error is safe to ignore.

---

### 11. Bare `errors.New` strings prevent structured error handling

**`internal/backend/backend.go:72,137`**

```go
return nil, errors.New("not connected")
```

Callers cannot distinguish "not connected" from other errors using `errors.Is`. These should be package-level sentinel errors:

```go
var ErrNotConnected = errors.New("smb backend: not connected")
```

---

### 12. Custom `split` function reimplements `strings.Split` poorly

**`internal/vfs/cache.go:299-323`**

The custom `split` function compares bytes with `string(s[i]) == sep`, which allocates a new string for every character in the path. The outer `splitPath` wrapper then does `append([]string{}, split(...)...)` for an unnecessary copy. The entire thing can be replaced with:

```go
func splitPath(path string) []string {
    if path == "" || path == "." || path == "/" {
        return nil
    }
    parts := strings.Split(strings.Trim(path, "/"), "/")
    // filter empty strings from double slashes
    result := parts[:0]
    for _, p := range parts {
        if p != "" {
            result = append(result, p)
        }
    }
    return result
}
```

---

### 13. `getBackendList()` returns non-deterministic order

**`internal/fuse/fs.go:45-51`**

Iterating over a `map[string]backend.Backend` produces random ordering. This slice is passed to `FetchEntry` and `FetchDir` for lazy warmup, and the order affects which backend "wins" when concurrent goroutines race (e.g., which backend's stat result is treated as authoritative first). This is low-impact in practice due to the reconciliation logic, but can make behavior hard to reproduce in tests and should be made deterministic.

---

### 14. Repair manager logs wrong condition

**`cmd/replistore/main.go:135-137`**

```go
repairManager.Start(ctx)
if repairInterval > 0 {
    logrus.Infof("Background repair manager started...")
}
```

The repair manager is always started (line 134), but the log only fires when `repairInterval > 0`. If `repairInterval` is zero, the manager still starts and runs with a zero-duration ticker (which fires immediately and continuously). The condition should guard the `Start` call, not just the log.

---

## Low / Style

### 15. `main.go` does not close backends on shutdown

**`cmd/replistore/main.go:37-48,140-154`**

SMB backends are connected during startup but never explicitly closed. The shutdown handler (line 142) cancels the context, stops discovery and the lock manager, then unmounts FUSE, but there is no loop to call `b.Close()` on each backend. SMB sessions and underlying TCP connections are left to be cleaned up by OS process exit.

---

### 16. Path construction duplicated across `Create`, `Mkdir`, `Remove`, `Rename`

**`internal/fuse/fs.go:143-146, 238-241, 383-386`**

```go
path := parentPath + "/" + req.Name
if parentPath == "" {
    path = req.Name
}
```

This pattern appears identically in four methods. Extract to a helper:

```go
func joinPath(parent, name string) string {
    if parent == "" {
        return name
    }
    return parent + "/" + name
}
```

---

### 17. `LockStatus` is a `string` type used as an RPC response field

**`internal/cluster/rpc.go:15-20`**

Using a named `string` type for `LockStatus` sent over `net/rpc` is fragile — the gob encoder will encode it as a string but any future change to use `iota` constants would silently break the wire format. The current approach works, but a comment explaining the choice would help, or use explicit string constants instead of a named type.

---

### 18. `FetchDir` silently ignores per-backend errors

**`internal/vfs/cache.go:496-500`**

```go
g.Go(func() error {
    entries, err := b.ReadDir(gCtx, path)
    if err != nil {
        return nil   // error swallowed
    }
```

Backend errors during lazy `FetchDir` are discarded. If all backends fail, `FetchDir` returns nil and marks the node `FullyIndexed = true` (line 518), making the directory appear empty to the caller. The caller in `fs.go:108-111` logs the error but returns the (empty) result anyway. An empty directory is worse than an error for the user.
