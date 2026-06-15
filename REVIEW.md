# RepliStore Design & Correctness Review

Reviewed at commit `af0541f` (2026-06-10). Scope: full source tree (`internal/`), with emphasis on logic flaws, inconsistent design decisions, and missed edge cases in the replication, caching, and distributed-locking layers.

## Status summary (as of `c1ec0fd`, 2026-06-12)

This document has been cleaned up to list only open or partially-fixed findings. Completed items have been removed.

---

Severity legend:
- **CRITICAL** — can lose or corrupt data, or silently violates the system's stated consistency guarantees.
- **HIGH** — incorrect behavior visible to applications, or guarantees that hold only under lucky timing.
- **MEDIUM** — robustness, resource, or operability gaps.
- **LOW** — minor inconsistencies and dead code.

---

## 1. Critical findings

All critical findings have been resolved.

---

## 2. High-severity findings

### H2. Lock contention has no resolution protocol: Lamport time is carried but never used; failed acquisitions don't retry

> **Status: PARTIALLY FIXED** in `5003ee2` — retry/backoff added; Lamport ordering still unused

RequestLock is first-come-first-served per peer; `LamportTime` only updates the clock. Two simultaneous requesters can each collect a partial set of grants, both miss quorum, both roll back, and both immediately fail the FUSE op back to the application. the project's test plan claimed deterministic Lamport-ordered conflict resolution; the code has none.

**Proposed solution:** either (a) implement request ordering — a peer that has granted to `(T1, NodeA)` and receives `(T0, NodeB)` with `T0 < T1` (or equal time, lower node ID) should be able to yield via a wound/wait rule — or (b) keep FCFS but add randomized exponential backoff and retry inside `Acquire` before surfacing failure. Option (b) is far simpler and adequate at this scale.

### H4. Lease renewal is brittle: one failed round = lock lost; quorum denominator drifts from the granted set

> **Status: FIXED** — renewal quorum derives from `expected_cluster_size` (never the live peer list) since `881277e`; renewal now runs at `LeaseTTL/3` and a missed round no longer surrenders the lock. The lease is declared lost only once its deadline (`expiresAt`, advanced on each successful round) actually passes without a quorum renewal, and each round is bounded to the cadence so a hung round cannot consume the grace window.

`startRenewal` declares the lock lost on the *first* renewal round that misses quorum (lock.go:156) — a single transient network blip longer than `LeaseTTL/2 = 2.5s` kills an active write handle. Also, `renew` recomputes quorum from the *current* peer count (lock.go:174) while only ever contacting the original `grantedPeers`; if membership grows after acquisition, quorum can exceed the contactable set and the lock is unrenewable by construction.

**Proposed solution:** renew on a faster cadence (`TTL/3`) and only declare the lock lost when the lease deadline actually passes without a successful quorum round (track `lastRenewed`). Compute renewal quorum against the same denominator used at acquisition (`ExpectedClusterSize`), never against the live peer list.

### H7. Repair and inline healing race with active writers when the DLM is disabled (and on the same node even when enabled, per C2)

> **Status: PARTIALLY FIXED** in `2497fb1` — residual gap: in-flight writes on open handles

With `listen_addr` unset, `acquireLock` returns `(nil, nil)` and `RepairManager.repairNode` / the inline heal in `File.Open` proceed with no coordination at all. A repair copy of a file being actively written produces a torn replica that then participates in LWW (and per C4 may even win). bazil/FUSE dispatches operations concurrently, so single-node deployment does not imply serialization.

**Proposed solution:** add an in-process per-path lock table (`map[string]*sync.Mutex` with refcounting) in `RepliFS`, taken by Create/Open(write)/Remove/Rename/repair/heal regardless of DLM configuration; the DLM then layers cross-node exclusion on top. This also addresses the same-node hole in C2.

### H8. Read-only `Open` has no failover; `Read` failover ignores newly-healthy replicas

> **Status: FIXED** — open-time failover added in `cb361b7`; `Read`'s `tried` set is scoped per `Read` call (not per handle), so a backend that recovers is reopened from `Meta.Backends` on the next Read syscall — the handle is no longer permanently dead.

`File.Open` read path (fs.go:717) picks one backend and returns the open error directly — a single unhealthy-but-not-yet-marked backend fails the open even though other replicas are fine. `FileHandle.Read`'s failover loop is solid, but it can never recover once every backend in `Meta.Backends` has been `tried`, even if a backend recovered seconds later (handle is permanently dead until reopened).

**Proposed solution:** in `Open`, iterate over `Meta.Backends` (health-filtered, e.g. via `SelectForRead` repeatedly with exclusion) until one opens. Optionally reset `tried` on a timer or treat `tried` as a per-attempt set rather than per-handle.

---

## 3. Medium-severity findings

### M2. Negative lookups are never cached

Every `Lookup` miss in a not-fully-indexed directory triggers a parallel `Stat` fan-out to all backends (fs.go:88). Path-probing workloads (compilers, shells walking `$PATH`) hammer every NAS. **Fix:** cache negative entries with a short TTL (e.g., 1–5 s), invalidated by local creates.

### M6. Cluster RPC and mDNS membership are completely unauthenticated

> **Status: PARTIALLY FIXED** — lock channel HMAC-authenticated in `dcf5a7b` (UDP + JWS/HS256 with mandatory `cluster_secret`); discovery membership gated by SMB credentials since `2dac188`; fencing tokens are now unguessable 128-bit `crypto/rand` values minted client-side once per acquisition and carried unchanged across peers and renewals (peers reject a request that carries no token). Remaining: replayed renewals can still extend a dead holder lease (documented, capture-required).

Any host on the LAN can register as a peer, grant/deny locks, call `RequestLock` with another node's `NodeID` to hijack a lease, or `ReleaseLock` it (token is guessable: `<lamport>-<nodeID>`). For a system coordinating writes to shared storage this is a meaningful integrity risk even on a "trusted" LAN. **Fix:** pre-shared cluster secret — HMAC over RPC payloads at minimum, or mutual TLS on the RPC listener; make fencing tokens random (crypto/rand) rather than derived.

### M7. Health checks and reconnects ignore their context deadlines

`Ping` gets a 2 s context (monitor.go:52) but `execute → ensureConnected → connectLocked` uses a hardcoded 5 s `net.DialTimeout` and ctx-unaware SMB dial/mount, so a down backend stalls each check ~3× its budget. `smbFile` I/O only checks `ctx.Err()` before issuing the call — FUSE interrupts and timeouts can't cancel in-flight SMB ops. **Fix:** thread `ctx` into `connectLocked` (use `net.Dialer.DialContext`); accept that go-smb2 calls aren't cancellable and wrap them with a watchdog that closes the connection on deadline (the reconnect path already recovers from closed connections).

### M11. Rename-over-existing-target is not atomic and likely fails

> **Status: FIXED** — `Dir.Rename` now clears an existing target before the source fan-out (`clearRenameTarget`): it enforces the POSIX type rules (a directory may only be replaced by an empty directory; otherwise `EISDIR`/`ENOTDIR`/`ENOTEMPTY`), durably tombstones the target, then removes its replicas across all backends so they are not leaked. Renaming a path onto itself is a no-op success. The replace is not a single atomic SMB operation, so a crash between the target delete and the source rename can leave the target missing — documented, acceptable.

POSIX `rename` must atomically replace an existing target. SMB2 rename fails if the target exists (go-smb2 does not set the replace flag), so `mv a b` with `b` present errors; the cache's `Rename` would also silently overwrite the target node, leaking its replicas (never deleted on backends). **Fix:** detect existing target in `Dir.Rename`; implement replace as locked delete-then-rename (with tombstone, per C6), and document the non-atomicity window.

---

## 4. Low-severity / consistency nits

- **L1.** `Acquire`'s lock-acquisition timeout (3 s, lock.go:69) is hardcoded and shorter than SMB stalls it may sit behind; make TTL, renewal cadence, and acquire timeout configurable together (they're coupled).
- **L3.** ~~`splitPath`/`split` (cache.go:299) reimplement `strings.Split`; replace, and use `path.Join` instead of ad-hoc concatenation (three different sites build child paths slightly differently).~~ **FIXED** — hand-rolled `split` dropped for `strings.Split`; child-path sites unified on a `joinPath` helper (the `path` package can't be imported cleanly: `path` is used as a variable name throughout the file).
- **L5.** ~~Shutdown path (main.go:143) unmounts and `os.Exit(0)` without closing SMB backends or releasing held locks; rely-on-TTL is fine for locks, but backends deserve a `Close()` sweep, and `cancel()` should precede `fuse.Unmount` to stop background scans racing the unmount.~~ **FIXED** — `Close() error` added to the `Backend` interface (SMB logs off/unmounts; local and mock are no-ops); shutdown now closes the FUSE conn and sweeps all backends explicitly after `cancel()` → `Unmount` (the explicit calls are required because `os.Exit` skips deferred cleanup). Locks still rely on TTL, by design.
- **L6.** `Dir.Attr`/`File.Attr` never set `Valid`, `Uid`, `Gid`, or `Nlink`; the kernel default attr-cache hides cross-node updates for an unspecified duration — set `a.Valid` explicitly to a value consistent with the staleness budget.
- **L7.** `markAllIndexed` runs even when `Warmup` had per-backend scan errors; with one backend unscanned, directories are still marked authoritative (mini-C8). Gate it on per-backend success, or per-directory success tracking.
