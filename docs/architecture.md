# RepliStore Architecture

RepliStore is a distributed, FUSE-based replicated storage system. It aggregates multiple SMB2/SMB3 network shares into a single unified mount point, providing file-level replication and high-performance metadata access.

## High-Level Overview

RepliStore consists of four primary layers:
1.  **Frontend (FUSE):** Translates OS syscalls into VFS operations.
2.  **Virtual File System (VFS):** Manages the unified namespace, replication logic, and metadata cache.
3.  **Cluster (P2P):** Handles node discovery and distributed locking across multiple RepliStore instances.
4.  **Backend (SMB):** Handles raw I/O and connectivity to the storage providers.

```mermaid
graph TD
    User([User Application]) --> FUSE[FUSE Interface /dev/fuse]
    FUSE --> Frontend[Frontend Layer /internal/fuse]
    Frontend --> VFS[VFS Layer /internal/vfs]
    VFS --> Cache[(Metadata Cache)]
    VFS --> Backend[Backend Layer /internal/backend]
    VFS --> Cluster[Cluster Layer /internal/cluster]
    VFS --> Repair[Repair Manager /internal/fuse/repair.go]
    Cluster <--> Peer[Other RepliStore Nodes]
    Repair --> Backend
    Backend --> SMB1[SMB Share A]
    Backend --> SMB2[SMB Share B]
    Backend --> SMB3[SMB Share C]
```

## Key Components

### Frontend Layer
Responsible for handling FUSE requests and converting them into VFS operations. It uses `bazil.org/fuse` as the FUSE library.

### VFS Layer
The core of the system. It maintains an in-memory tree structure (Metadata Cache) of the unified filesystem. It also implements the replication logic (selecting backends for writes) and read failover.

### Cluster Layer (DLM)
The Distributed Lock Manager (DLM) ensures that only one node in the cluster can perform conflicting operations (like writing to the same file) at any given time.
- **Discovery:** Nodes find each other through the shared SMB backends themselves: each node heartbeats a peer entry at `.replistore/peers/<nodeID>.json` on every backend and polls that directory for membership. No multicast or shared L2 network is required.
- **Transport:** Lock messages are UDP datagrams in JWS compact serialization (HS256), signed with the mandatory shared `cluster_secret`; unauthenticated datagrams are dropped silently.
- **Consensus:** Implements a masterless quorum-based mutual exclusion algorithm; the quorum is `expected_cluster_size/2 + 1`, derived from configuration rather than the live peer list.
- **Robustness:** Per-acquisition lock IDs, TTL-based leases with background renewal (expired leases cannot be renewed), and jittered retry with backoff on acquisition failure provide automatic lock recovery after node failures.

### Backend Layer
Manages connections to remote SMB shares. It uses `github.com/hirochachacha/go-smb2` for SMB2/3 communication. It also includes a health monitor that periodically pings backends to check their availability.

### Repair Manager
A background worker that periodically "scrubs" the filesystem: first it enforces deletion tombstones (removing zombie replicas and garbage-collecting converged tombstones), then it scans the Metadata Cache for degraded files (those with fewer than `replication_factor` replicas) and restores them by copying data from healthy replicas to available backends, preserving the source mtime and replicating the version sidecar (with a freshly computed `sha256` content sum) onto the new copy.

### Reserved Tree (`.replistore`)
Each backend carries a reserved `.replistore` directory holding RepliStore's own metadata: the peer registry (`peers/`), per-file version metadata documents — sidecars and deletion tombstones alike — under `meta/`, and the global repair lock. This tree is invisible through the FUSE mount, and any attempt to create, open, or otherwise mutate paths inside it via the mount returns `EACCES`.

## Design Philosophy

- **Authoritative Source:** Remote SMB shares are the ultimate source of truth.
- **In-Memory Metadata:** For high performance, directory listings and lookups are served from an in-memory cache populated during startup.
- **Statelessness:** No local database is required; the system reconstructs its state from the backends.
- **Quorum-Based Write Consistency:** Writes and creates are fanned out to all mapped backends and succeed if a configurable `write_quorum` acknowledges the operation. This provides a balance between reliability and availability.
- **I/O Resilience:** All backend and cluster lock operations support `context.Context` for timeouts and cancellation, preventing kernel-level hangs in the FUSE filesystem.
- **Standardized Concurrency:** Parallel operations (writes, repairs, background sync) are managed using `golang.org/x/sync/errgroup` to ensure robust error collection and resource management.
- **Read Resilience:** Reads can fail over to alternative replicas if the primary choice is unavailable.

## Design Decisions

The rationale behind three structural choices made during the 2026-06 hardening pass. The operational view of these mechanisms lives in [multi-client.md](multi-client.md) and the component docs; this section records *why* the designs look the way they do.

### Sidecar Tree for Version Metadata (not Alternate Data Streams)

RepliStore maintains its own per-file version metadata — a generation counter bumped under the path lock once per write session, plus an optional content checksum — because backend mtimes are stamped by each SMB server's own clock and cannot be trusted as version vectors. Two storage options were considered: SMB Alternate Data Streams (ADS) attached to each data file, and a sidecar tree of plain files.

**Decision: sidecar tree.** ADS requires `vfs_streams_xattr` on Samba and is silently unsupported on some NAS firmware, while plain files work on every SMB server this tool targets; deployment safety outweighs the extra round-trip and the (hidden) on-share clutter. A replica with no metadata document reports generation 0 and is treated as a legacy (pre-versioning) file.

**Key scheme: hash, not path mirror.** A path's metadata lives at `.replistore/meta/<h0>/<h1>/<sha256hex>.json`, where the key is the SHA-256 of the data path (hex), sharded two levels deep (256×256 fanout) to keep directory listings small. The data path is one-way under the hash, so it is recorded inside the document — which is exactly the primary-key shape a key/value metadata store would later want. Hashing (rather than mirroring the data tree as `meta/<path>.json`) decouples metadata layout from the data tree, makes directory metadata clean, and never needs deep parent `mkdir` chains. Sidecars are point-looked-up by an existing data path, so their tree is never enumerated and needs no sharding for walks — the sharding is purely to bound per-directory entry counts.

**Sidecars and tombstones share one document.** A tombstone is a metadata document with its `Deleted` flag set; it occupies the same key as the path's sidecar. A delete or rename writes the tombstone (overwriting the live sidecar); a later write at the path clears the flag. This is simpler than two parallel trees and is correct because a path is either live or deleted, never both. The cost is that enumerating deletions (which sync does each pass, since a deleted path is gone from the data tree and cannot be found by walking data) requires walking the whole `meta/` tree and filtering on `Deleted` — O(total documents) rather than O(live tombstones). That trade was taken deliberately: at meaningful deletion density the two costs converge, and the single-tree model removes a class of consistency bugs. Two invariants fall out of the shared key: delete/rename must **not** remove the document after writing its tombstone (it would erase the deletion record), and tombstone garbage-collection must read-then-remove only when `Deleted` is still set (never clobbering a live sidecar that has reclaimed the key).

### Backend-Based Node Discovery (replaces mDNS)

mDNS discovery confined clusters to one L2 broadcast domain, let any LAN host register as a peer, picked an arbitrary interface address on multi-homed hosts, and relied on zeroconf re-announcements for liveness (peers flapped after 2 minutes of multicast silence).

**Design:** the SMB backends are already the cluster rendezvous, so they serve as the membership registry.
- Each node maintains an entry `.replistore/peers/<nodeID>.json` (`{id, address, seq}`) on **every** backend. One file per node — no write contention, no atomic-append requirement.
- `address` comes from the mandatory `advertise_addr` config (required when `listen_addr` is set); this fixes the multi-homed problem by construction.
- **Heartbeat:** every interval (10s) the node rewrites its entry with a fresh `seq` (writer's `UnixNano`; only ever compared against the *same node's* previous value, never across nodes — no cross-node clock comparison).
- **Poll:** every interval (10s) the node lists `.replistore/peers/` on all backends and takes the union, deduplicated by node ID with the highest `seq` winning. A peer expires when its `seq` has not changed for the expiry window (35s), measured on the **reader's own clock** from the moment the reader last observed a change. If no backend can be listed at all, membership is left untouched rather than dropping every peer over a transient outage.
- **Lifecycle:** entries are deleted from all backends on graceful shutdown; a janitor purges entries that have been observed unchanged for a long multiple of the heartbeat (10 minutes — crash leftovers).
- **Safety:** discovery only feeds the peer list for lock fan-out; lock quorum derives from `expected_cluster_size`, so a stale address book degrades availability, never consistency.
- Membership implicitly requires SMB credentials (an improvement over open multicast). Discovery latency is the poll interval (~10s), acceptable for quasi-static membership. Removes the zeroconf dependency.

### Datagram Lock Transport: UDP + JWS/HS256 (replaces net/rpc over TCP)

Lock RPC previously used `net/rpc` over a fresh TCP connection per request: a handshake per lock operation, renewal dials to every granted peer every 2.5s per held lock, TIME_WAIT/ephemeral-port churn, kernel-paced failure detection, and a completely unauthenticated channel.

**Design:** the lock protocol is lease-based, idempotent per `(NodeID, LockID)`, and quorum-tolerant of non-response — the textbook profile for datagram transport. TCP's reliability layer only duplicated (more slowly) what the protocol already provides.
- **Wire format:** each datagram is a JWS compact serialization (JWT-style), HS256: `base64url(header) "." base64url(claims) "." base64url(HMAC-SHA256(secret, header "." claims))`. Signing the *encoded segments* (per JWS) rather than JSON structures avoids canonicalization pitfalls entirely. Claims: `{"v": 1, "typ": "request_lock" | "renew_lock" | "release_lock" (responses suffixed "_resp"), "rid": "<16-hex crypto/rand request ID>", "body": { ...lock message... }}`. The verifier compares the header segment byte-for-byte against the pinned constant `{"alg":"HS256","typ":"JWT"}` — the algorithm field is never parsed from the wire, which forecloses `alg`-confusion/`none` attacks. There are no `exp`/`iat` claims: lease TTLs handle expiry, and timestamp claims would reintroduce cross-node clock comparison, which this codebase deliberately avoids. Implemented with the standard library only (one fixed algorithm needs no JWT dependency).
- **Authentication:** HMAC-SHA256 with the mandatory `cluster_secret` (min 16 chars, required when `listen_addr` is set). Datagrams with bad signatures are dropped silently.
- **Client:** per-call connected UDP socket (the kernel rejects datagrams from other sources); `crypto/rand` request IDs; retransmit with exponential backoff (200/400/800 ms, ...) under the caller's context deadline; the first MAC-valid response with a matching request ID wins. Handlers are idempotent, so retransmitted requests are harmless.
- **Server:** a single UDP socket, stateless: read → verify MAC → dispatch to the unchanged `RequestLock`/`RenewLock`/`ReleaseLock` handlers → reply to the source address echoing the request ID.
- **Replay considerations:** replayed requests are idempotent no-ops (LockID/fencing-token matching), except a replayed `Renew`, which can extend a dead holder's lease — a minor, capture-required DoS, accepted and documented (the alternative is timestamp windows, i.e. cross-node clock comparison).
- Lock semantics (quorum from `expected_cluster_size`, lease TTLs, LockID idempotency) are untouched: this was a transport swap only. It removed `net/rpc` usage and the per-request connection churn.
