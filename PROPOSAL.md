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

### 3.2. Atomic Multi-Backend Rename
**Current Issue:** `Rename` is currently difficult to implement across multiple independent backends without leaving the system in a partial state if one backend rename fails.

**Proposal:**
- Implement a two-phase commit (2PC) or a "tombstone" approach for renames to ensure the unified view remains consistent across all replicas.

### 3.3. Local Data Tiering (Read-Through Cache)
**Current Issue:** Every `Read` involves a network round-trip to an SMB share.

**Proposal:**
- Implement a local disk-based cache for frequently accessed small files or file blocks.
- This would significantly improve performance for "hot" data while keeping the SMB shares as the authoritative source.

## 4. Operational & Observability

### 4.1. Metrics Export (Prometheus)
**Proposal:**
- Export metrics for:
    - Operation latency (Read/Write/Metadata).
    - Cache hit/miss ratios.
    - Backend health and latency.
    - Replication health (number of degraded files).

### 4.2. Secure Secret Management
**Proposal:**
- Integrate with external secret providers (e.g., HashiCorp Vault) or system keyrings instead of relying on environment variables or plain text configuration.

---

## 5. Recently Completed

### 5.1. Quorum-Based Consistency
- **Implemented:** Support for `write_quorum` in configuration and filesystem operations.
- **Functionality:** File creation and data writes succeed if a quorum of replicas acknowledge the operation. Failed backends are automatically removed from the file's metadata to ensure consistency with the surviving replicas.
- **Verification:** Added `TestFile_Write_Quorum` to verify behavior during partial backend failures.

### 5.2. Background Repair (Anti-Entropy)
- **Implemented:** Background `RepairManager` that identifies degraded files and restores replicas.
- **Functionality:** Periodically scans the metadata cache for files with fewer than `replication_factor` backends and automatically copies data from healthy replicas to missing ones. Supports concurrency control.
- **Verification:** Added `TestRepairManager_RepairNode` and `TestCache_FindDegraded`.

### 5.3. Background Metadata Synchronization
- **Implemented:** A continuous background scan using the `cache_refresh_interval`.
- **Functionality:** Reconciles the cache by detecting new files, modifications, and deletions.
- **Verification:** Unit tests added for node pruning and reconciliation logic.
