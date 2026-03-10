# RepliStore: Improvement Proposal

This document outlines a roadmap for evolving RepliStore from a functional prototype into a production-ready distributed storage system. The proposed improvements focus on resilience, performance, and data integrity.

## 1. High Availability & Resilience

### 1.1. Active Read Failover (Enhanced)
**Current Issue:** Read failover is reactive (triggered by an error).

**Proposal:**
- Use the `HealthMonitor` results to proactively avoid selecting known unhealthy backends for initial read attempts.
- Implement parallel "hedged" reads: if a backend is slow (exceeds a latency threshold), issue a concurrent read to another replica and take the first successful response.

## 2. Metadata & Performance

### 2.1. Smart Backend Selection
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

### 5.1. `O_APPEND` Support
**Current Issue:** Log-style writes using `O_APPEND` are not handled, as `WriteAt` is the primary interface used.
**Proposal:** Extend the `FileHandle` to handle the `Append` flag by querying the current file size from the cache before performing parallel writes.

## 6. Multi-Client & Distributed Coordination

### 6.1. Change-Based Cache Invalidation (SMB Notify)
**Current Issue:** Metadata staleness between instances; full scans are expensive and slow.
**Proposal:** Utilize the SMB `CHANGE_NOTIFY` feature to subscribe to directory changes on the backends. This allows instances to perform surgical, near-real-time cache updates instead of relying solely on periodic full scans.

### 6.2. Shared Metadata Store
**Current Issue:** Independent in-memory caches lead to divergent views of the filesystem.
**Proposal:** Support an optional shared metadata backend (e.g., etcd or Redis). This ensures all RepliStore instances see an identical, atomic view of the unified namespace in real-time.

### 6.3. Conflict Resolution with Versioning (Vector Clocks)
**Current Issue:** "Last-writer-wins" (based on `mtime`) is insufficient for resolving complex distributed conflicts.
**Proposal:** Store versioning metadata (e.g., vector clocks or generation IDs) alongside files using SMB Alternative Data Streams (ADS). This enables deterministic detection and resolution of divergent replicas.

## 7. Reliability & Data Recovery

### 7.1. Repair Manager Optimizations
**Current Issue:** `RepairManager` copies data sequentially to each target backend and does not explicitly ensure the target parent path exists.
**Proposal:**
- **Read Once, Write Many:** Optimize the repair process by reading a chunk of data from the source once and writing it to all target backends in parallel.
- **Safe Directory Creation:** Ensure the destination parent directory exists on the target backend (using `MkdirAll`) before starting the copy.

### 7.2. Backend Selection during `Create`
**Current Issue:** `Dir.Create` uses all backends as potential candidates for `SelectForWrite`, but it doesn't explicitly filter out backends where the parent directory might not exist (e.g., if a previous `Mkdir` failed on some backends).
**Proposal:** Improve target selection to ensure that files are only created on backends that already contain the parent directory structure.

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

### 9.2. Quorum-Based Consistency
- **Implemented:** Support for `write_quorum` in configuration and filesystem operations.

### 9.3. Background Repair (Anti-Entropy)
- **Implemented:** Background `RepairManager` that identifies degraded files and restores replicas.

### 9.4. Background Metadata Synchronization
- **Implemented:** A continuous background scan using the `cache_refresh_interval`.

### 9.5. Atomic Multi-Backend Rename
- **Implemented:** `Rename` support in the FUSE layer and VFS cache.

### 9.6. P2P Distributed Lock Manager (DLM)
- **Implemented:** Fully decentralized DLM using mDNS for discovery and a masterless quorum-based protocol.

### 9.7. Lazy / Progressive Warmup
- **Implemented:** Asynchronous metadata scanning and on-demand fetching.

### 9.8. HealthMonitor Enhancements
- **Implemented:** Parallel and context-aware backend health monitoring.

### 9.9. Quorum-Based `Mkdir` and `Remove`
- **Implemented:** Quorum enforcement for directory creation and removal.
- **Functionality:** `Mkdir` and `Remove` now succeed only if at least `write_quorum` backends acknowledge the operation. `Mkdir` performs automatic rollback on backends if quorum is not reached.
