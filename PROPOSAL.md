# RepliStore: Improvement Proposal

This document outlines a roadmap for evolving RepliStore from a functional prototype into a production-ready distributed storage system. The proposed improvements focus on resilience, performance, and data integrity.

## 1. High Availability & Resilience

### 1.1. Active Read Failover (Enhanced)
**Current Issue:** Read failover is reactive (triggered by an error).

**Proposal:**
- Use the `HealthMonitor` results to proactively avoid selecting known unhealthy backends for initial read attempts.
- Implement parallel "hedged" reads: if a backend is slow (exceeds a latency threshold), issue a concurrent read to another replica and take the first successful response.

## 2. Metadata & Performance


### 2.1. Lazy / Progressive Warmup
**Current Issue:** The system waits for a full metadata scan of all backends before mounting the FUSE filesystem, which can take minutes for large datasets.

**Proposal:**
- Mount the filesystem immediately.
- Serve `Lookup` and `ReadDir` requests by falling back to a synchronous backend call if the metadata for that specific path is not yet cached.
- Continue the full scan in the background to gradually populate the cache.

### 2.2. Smart Backend Selection
**Current Issue:** `RandomSelector` does not consider backend state beyond binary health.

**Proposal:**
- **Space-Aware:** Query backend free space and prioritize shares with more capacity for new file creations.
- **Latency-Aware:** Track the response time of each backend and prioritize the fastest one for read operations.

## 3. Data Integrity & Correctness

### 3.1. End-to-End Checksumming
**Current Issue:** RepliStore relies on the underlying SMB protocol for integrity. It cannot detect bit rot or silent corruption if the backend returns "successful" but corrupted data.

**Proposal:**
- Calculate and store a checksum (e.g., BLAKE3 or SHA-256) in the metadata cache when a file is written.
- Verify the checksum on read. If a mismatch is detected, transparently fail over to another replica and log a corruption event.

### 3.2. Local Data Tiering (Read-Through Cache)
**Current Issue:** Every `Read` involves a network round-trip to an SMB share.

**Proposal:**
- Implement a local disk-based cache for frequently accessed small files or file blocks.
- This would significantly improve performance for "hot" data while keeping the SMB shares as the authoritative source.

## 4. FUSE Protocol & Compatibility

### 4.1. `Setattr` (Chmod/Chown/Utimes)
**Current Issue:** Standard filesystem operations like changing permissions or timestamps are not currently supported.
**Proposal:** Implement `Setattr` on `Dir` and `File` nodes to forward attribute changes to all replicas.

### 4.2. Symlink and Readlink
**Current Issue:** Symbolic links are currently not supported.
**Proposal:** Implement `Symlink` and `Readlink` support to allow creating and following links across the unified filesystem.

## 5. Connectivity & Reliability

### 5.1. HealthMonitor Enhancements
**Current Issue:** `Ping()` calls to backends are synchronous and performed serially. A single slow or hanging backend can block the health check for all other backends.
**Proposal:** 
- Add context support with a strict timeout to the `Backend.Ping()` interface.
- Parallelize health checks so that multiple backends can be monitored concurrently.

### 5.2. `O_APPEND` Support
**Current Issue:** Log-style writes using `O_APPEND` are not handled, as `WriteAt` is the primary interface used.
**Proposal:** Extend the `FileHandle` to handle the `Append` flag by querying the current file size from the cache before performing parallel writes.

## 6. Multi-Client & Distributed Coordination

### 6.1. Change-Based Cache Invalidation (SMB Notify)
**Current Issue:** Metadata staleness between instances; full scans are expensive and slow.
**Proposal:** Utilize the SMB `CHANGE_NOTIFY` feature to subscribe to directory changes on the backends. This allows instances to perform surgical, near-real-time cache updates instead of relying solely on periodic full scans.

### 6.3. Shared Metadata Store
**Current Issue:** Independent in-memory caches lead to divergent views of the filesystem.
**Proposal:** Support an optional shared metadata backend (e.g., etcd or Redis). This ensures all RepliStore instances see an identical, atomic view of the unified namespace in real-time.

### 6.5. Conflict Resolution with Versioning (Vector Clocks)
**Current Issue:** "Last-writer-wins" (based on `mtime`) is insufficient for resolving complex distributed conflicts.
**Proposal:** Store versioning metadata (e.g., vector clocks or generation IDs) alongside files using SMB Alternative Data Streams (ADS). This enables deterministic detection and resolution of divergent replicas.

## 7. Reliability & Data Recovery

### 7.1. Quorum-Based `Remove`
**Current Issue:** The `Remove` operation currently fails if any backend returns an error, which can prevent file deletion if one backend is temporarily unavailable.
**Proposal:** Implement quorum-based `Remove`. The operation should succeed if at least `write_quorum` backends acknowledge the deletion. Remaining replicas can be cleaned up later by the `RepairManager`.

### 7.2. Repair Manager Optimizations
**Current Issue:** `RepairManager` copies data sequentially to each target backend and does not explicitly ensure the target parent path exists.
**Proposal:**
- **Read Once, Write Many:** Optimize the repair process by reading a chunk of data from the source once and writing it to all target backends in parallel.
- **Safe Directory Creation:** Ensure the destination parent directory exists on the target backend (using `MkdirAll`) before starting the copy.

### 7.3. Metadata Accuracy
**Current Issue:** File modification times (`ModTime`) are not updated in the VFS cache during `Write` operations, leading to stale metadata.
**Proposal:** Update the `ModTime` in the cache node whenever a successful write (meeting quorum) occurs.

## 8. Operational & Observability

### 8.1. Metrics Export (Prometheus)
**Proposal:**
- Export metrics for:
    - Operation latency (Read/Write/Metadata).
    - Cache hit/miss ratios.
    - Backend health and latency.
    - Replication health (number of degraded files).

### 8.2. Secure Secret Management
**Proposal:**
- Integrate with external secret providers (e.g., HashiCorp Vault) or system keyrings instead of relying on environment variables or plain text configuration.

---

## 9. Recently Completed

### 9.1. Fsync Support
- **Implemented:** `Flush` and `Fsync` support in the FUSE layer.
- **Functionality:** Synchronizes data to all open backend handles. Successfully syncs if `write_quorum` is met. Automatically removes replicas that fail the sync operation.
- **Verification:** Added `TestFile_Fsync` to verify fan-out and quorum behavior.

### 9.2. Quorum-Based Consistency
- **Implemented:** Support for `write_quorum` in configuration and filesystem operations.
- **Functionality:** File creation and data writes succeed if a quorum of replicas acknowledge the operation. Failed backends are automatically removed from the file's metadata to ensure consistency with the surviving replicas.
- **Verification:** Added `TestFile_Write_Quorum` to verify behavior during partial backend failures.

### 9.3. Background Repair (Anti-Entropy)
- **Implemented:** Background `RepairManager` that identifies degraded files and restores replicas.
- **Functionality:** Periodically scans the metadata cache for files with fewer than `replication_factor` backends and automatically copies data from healthy replicas to missing ones. Supports concurrency control.
- **Verification:** Added `TestRepairManager_RepairNode` and `TestCache_FindDegraded`.

### 9.4. Background Metadata Synchronization
- **Implemented:** A continuous background scan using the `cache_refresh_interval`.
- **Functionality:** Reconciles the cache by detecting new files, modifications, and deletions.
- **Verification:** Unit tests added for node pruning and reconciliation logic.

### 9.5. Atomic Multi-Backend Rename
- **Implemented:** `Rename` support in the FUSE layer and VFS cache.
- **Functionality:** Moves files and directories across the unified namespace. Ensures target parent directories exist on backends. Successfully renames if `write_quorum` is reached. Atomically updates the metadata cache and all child paths for directory moves.
- **Verification:** Added `internal/fuse/rename_test.go` with cross-directory and quorum failure scenarios.

### 9.6. P2P Distributed Lock Manager (DLM)
- **Implemented:** Fully decentralized DLM using mDNS for discovery and a masterless quorum-based protocol.
- **Functionality:** 
    - **Zero-Conf Discovery:** Nodes find each other via Multicast DNS (`_replistore._tcp`).
    - **Quorum-Based Locking:** Acquires path-level locks by gathering votes from a majority of active peers.
    - **Lamport Logical Clocks:** Ensures deterministic ordering of simultaneous requests.
    - **Lease-Based Fencing:** Locks are granted as TTL leases; background renewal and validation loops prevent stale nodes from corrupting data.
    - **Full Coordination:** All metadata operations (`Create`, `Mkdir`, `Rename`, `Remove`) and the **Background Repair Manager** now acquire distributed locks, eliminating "undelete" races and concurrent modification conflicts in multi-client clusters.
    - **Implicit Leader Election:** The Repair Manager uses a **Global Repair Lock** (`.replistore/repair.lock`) to ensure that only one node in the cluster performs background repairs at any given time, preventing redundant NAS traffic.
- **Verification:** Verified cluster formation, lock coordination, and single-node repair execution in multi-instance environments.
