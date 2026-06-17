# RepliStore Roadmap

This document tracks proposed improvements that are not yet implemented, plus known gaps from the last code review. For the design rationale behind decisions that have already shipped (sidecar version metadata, backend-based discovery, the UDP lock transport), see the "Design Decisions" section of [docs/architecture.md](docs/architecture.md).

## 1. High Availability & Resilience

### 1.1. Hedged Reads
Read failover is reactive today: the open-time and read-time loops try replicas in sequence after an error (`HealthMonitor` results already keep known-dead backends out of the initial pick). The remaining improvement is parallel "hedged" reads: if a backend is slow (exceeds a latency threshold), issue a concurrent read to another replica and take the first successful response.

## 2. Metadata & Performance

### 2.1. Smart Backend Selection
`RandomSelector` does not consider backend state beyond binary health.
- **Space-Aware:** Query backend free space and prioritize shares with more capacity for new file creations.
- **Latency-Aware:** Track the response time of each backend and prioritize the fastest one for read operations.

## 3. Data Integrity & Correctness

### 3.1. Read-Path Checksum Verification
Half of end-to-end checksumming is done: repair records `sha256` content sums in the per-file sidecars while copying, and flags same-generation replicas with divergent sums. What remains is the read path: verify the stored checksum on read and, on a mismatch, transparently fail over to another replica and log a corruption event. (Writers blank the sum on every generation bump — random-access FUSE writes make continuous hashing infeasible — so verification is only possible for replicas whose sum has been filled in by repair.)

### 3.2. Local Data Tiering (Read-Through Cache)
Every `Read` involves a network round-trip to an SMB share. A local disk-based cache for frequently accessed small files or file blocks would significantly improve performance for "hot" data while keeping the SMB shares as the authoritative source.

## 4. FUSE Protocol & Compatibility

### 4.1. Full `Setattr` (Chmod/Chown/Utimes)
`Setattr` currently handles size changes only (truncate, fanned out to all replicas with quorum accounting and a generation bump). Mode, ownership, and timestamp changes are still not forwarded to the backends.

### 4.2. Symlink and Readlink
Symbolic links are not supported. Implement `Symlink` and `Readlink` to allow creating and following links across the unified filesystem.

## 5. Connectivity & Reliability

### 5.1. `O_APPEND` Support
`O_APPEND` opens are currently rejected with `ENOTSUP`, because passing the flag through would let each backend append at its own EOF and guarantee replica divergence. Proper support means tracking the append offset in the `FileHandle` (single source of truth) and issuing positioned writes to all replicas.

## 6. Multi-Client & Distributed Coordination

### 6.1. Change-Based Cache Invalidation (SMB Notify)
Metadata staleness between instances; full scans are expensive and slow. Utilize the SMB `CHANGE_NOTIFY` feature to subscribe to directory changes on the backends, allowing instances to perform surgical, near-real-time cache updates instead of relying solely on periodic full scans.

### 6.2. Shared Metadata Store
Independent in-memory caches lead to divergent views of the filesystem between sync cycles. Support an optional shared metadata backend (e.g., etcd or Redis) so all RepliStore instances see an identical, atomic view of the unified namespace in real time.

## 7. Reliability & Data Recovery

### 7.1. Repair: Read Once, Write Many
The repair copy loop reads the source file once *per target* and writes targets sequentially. Optimize by reading each chunk from the source once and writing it to all target backends in parallel. (The other half of the original proposal — creating the destination parent directory with `MkdirAll` before copying — is done: file/dir creation and rename implicitly create parent directories on target backends.)

### 7.2. Backend Selection during `Create`
`Dir.Create` uses all healthy backends as candidates for `SelectForWrite` without preferring backends that already contain the parent directory. Since parents are now created implicitly with `MkdirAll`, this is an optimization (avoiding extra directory creation), not a correctness issue.


## 8. Operational & Observability

### 8.1. Observability, Logging, and Error Handling Upgrade
Implement the comprehensive observability upgrade. See details in [docs/observability.md](docs/observability.md). This includes:
- **Migration to standard `log/slog`** with `samber/slog-multi` for handler composition and structured fields.
- **Context-bound Correlation/Request IDs** across FUSE, VFS, and SMB backend operations.
- **Improved error wrapping** and POSIX translation mapping at the FUSE layer (`syscall.Errno`).
- **Request logging middleware** using `samber/slog-http` on the REST API server.

### 8.2. Metrics Export (Prometheus)
Export metrics for operation latency (read/write/metadata), cache hit/miss ratios, backend health and latency, and replication health (number of degraded files, replica divergence events — repair already keeps an internal divergence counter intended to feed this).

### 8.3. Secure Secret Management
Integrate with external secret providers (e.g., HashiCorp Vault) or system keyrings instead of relying on environment variables or plain-text configuration for SMB passwords and the `cluster_secret`.

### 8.4. REST/HTTP Control & Observability API
Implement an HTTP server exposing the REST API for system state monitoring (node health, backend latency, cluster peers, cache statistics, active lock leases) and direct data operations (raw file download/upload with automatic directory provisioning and static token authorization). See [docs/api.md](docs/api.md) for endpoints specification.

## Known Gaps (from code review)

One-liners for the items still open in [REVIEW.md](REVIEW.md); see the finding bodies there for details.

- **C8 residual:** no read-quorum/staleness semantics for lazy fetches — a single responding backend is treated as authoritative.
- **M2:** negative lookups are never cached; path-probing workloads fan a `Stat` out to every backend per miss.
- **M7:** backend reconnects ignore context deadlines; a down backend stalls health checks beyond their budget.
- **H4 residual:** a single renewal round missing quorum declares the lock lost immediately, instead of retrying until the lease deadline passes.
- **H7/H8 residuals:** repair doesn't serialize against in-flight writes on already-open handles; a read handle's `tried` set never resets.
- **L-nits (L1/L3/L5/L6/L7):** hardcoded lock timeouts, ad-hoc path helpers, shutdown without backend `Close()` sweep, unset FUSE attr `Valid`/`Uid`/`Gid`, `markAllIndexed` ignoring per-backend scan errors.

The test suite is mock-based throughout; a real-cluster smoke test of the sidecar/tombstone machinery is advised before production use.

## Completed

Major items delivered during the 2026-06 remediation, newest first:

- Metadata rearchitecture: sidecars and deletion tombstones unified into one document per path (a tombstone is a document with `Deleted` set), keyed by the SHA-256 of the path, sharded two levels (`.replistore/meta/<h0>/<h1>/<hash>.json`), with the data path stored inside the document. Replaces the path-mirrored `meta/<path>.json` + separate `tombstones/<path>.json` trees. Directory-metadata rehashing/re-keying on rename and directory tombstones are fully implemented (C6-dirs).
- (Remediation) — 7.3: replica pruning for over-replicated files implemented in RepairManager.
- Refactored local backend package: isolated local backend into its own package (`internal/backend/local`).
- (Remediation) — M12: emptiness verification for directory removes checks child existence in unified view, returning ENOTEMPTY.
- (Remediation) — C6-dirs: directory deletes and renames write tombstones and sidecars; repair enforces directory tombstones.
- (Remediation) — M3: walkStart timestamp captured in syncAll and sweep skipped for nodes updated during/after walk.
- (Remediation) — M4: active open handles tracked on vfs.Node and checked to prevent cache node pruning during sync.
- `8266ec7` — `File.Fsync` routed through open write handles; background rename orphan cleanup; `findPeer` optimization.
- `78c3dfd` — implicit parent-directory creation (`MkdirAll`) on target backends during file/dir creation and rename.
- `9ebbbe9` — inline healing for degraded files on write open.
- `1dc5624` — post-write lease re-check before acknowledging writes (pragmatic fencing; see docs/multi-client.md for the honest limitation).
- `87355f5` — content checksums recorded on repair copies; divergent same-generation replicas flagged.
- `4fff8cf` — durable deletes via tombstones (`.replistore/tombstones/`), with zombie cleanup and GC at scrub start.
- `b1800e1` / `41f3fa5` — per-file generation sidecars (`.replistore/meta/`); reconciliation by generation instead of raw mtime.
- `1c0c463` — the reserved `.replistore` tree hidden from the unified namespace (mutation attempts return `EACCES`).
- `dcf5a7b` — lock transport moved to authenticated UDP datagrams (JWS compact / HS256, mandatory `cluster_secret`).
- `2dac188` — mDNS discovery replaced by a backend-based peer registry (`.replistore/peers/`); `advertise_addr` mandatory.
- `cb361b7` / `6898ec3` — read-open failover across replicas; truncate via `Setattr`; `O_APPEND` rejected.
- `c8e1be3` — stale partial replicas deleted after dropping a failed write backend.
- `12b76c5` — source mtime preserved (`Chtimes`) on repair/heal copies.
- `2497fb1` — in-process per-path lock table serializing same-path mutations beneath the DLM.
- Earlier DLM hardening: mandatory `expected_cluster_size`, per-acquisition `LockID`s, expired-lease renewal rejection, grant GC, lexicographic rename lock ordering, jittered acquire retries (see REVIEW.md C1/C2/H1/H2/H5/M5).
