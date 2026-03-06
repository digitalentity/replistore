# Plan: Background Repair (Anti-Entropy)

This plan outlines the implementation of a background "scrub" worker that identifies and repairs degraded files (files with fewer replicas than the configured `replication_factor`).

## 1. Objective
Automatically restore the full replication factor for files that were partially written due to backend failures or were discovered to be out of sync during background metadata scans.

## 2. Architectural Changes

### 2.1. Metadata Enhancement
- Add an `IsDegraded` helper method to `vfs.Node` or `vfs.Metadata` that compares the current backend count with the global `ReplicationFactor`.

### 2.2. Repair Worker (`internal/vfs/repair.go`)
- Implement a `RepairWorker` that:
    - Periodically scans the metadata cache for degraded nodes.
    - Uses a concurrency-limited pool to perform repairs.
    - Selects a "source" backend (healthy and containing the file) and one or more "target" backends (healthy and missing the file).
    - Performs the data copy operation using `io.Copy` between backend handles.

### 2.3. Configuration
- Add `repair_interval` (default: 1h) to `config.Config`.
- Add `repair_concurrency` (default: 2) to limit the impact on network bandwidth.

## 3. Implementation Steps

### Phase 1: Candidate Identification
1.  Implement `Cache.FindDegraded(rf int) []*Node` to perform a depth-first search of the cache and return nodes needing repair.
2.  Add logging to report the number of degraded files found during each scrub cycle.

### Phase 2: Data Movement
1.  Implement `RepliFS.repairNode(node *vfs.Node)`:
    - Lock the node.
    - Identify source and targets.
    - Open source file for reading.
    - Open target files for writing (O_CREATE|O_TRUNC).
    - Stream data from source to targets.
    - Update node metadata upon successful copy.

### Phase 3: Orchestration
1.  Create a `RepairManager` that runs the scrub/repair loop in the background.
2.  Integrate the manager into `cmd/replistore/main.go`.

## 4. Safety & Edge Cases
- **Concurrent Writes:** If a file is being written to while a repair is attempted, the repair should ideally back off or coordinate with the `FileHandle`.
- **File Deletion:** If a file is deleted during repair, the repair operation must handle the "file not found" error on the source gracefully.
- **Partial Repair:** If a repair to one target fails but others succeed, the metadata should be updated to reflect the new successful replicas.
- **Large Files:** Use a buffer-limited copy to avoid excessive memory usage.

## 5. Verification Plan
- **Unit Tests:**
    - Test the `FindDegraded` logic.
    - Mock backends to simulate a repair where data is copied from B1 to B2.
- **Integration Test:**
    - Start RepliStore with 3 backends and RF=3.
    - Kill one backend, write a file (success due to Quorum=2).
    - Bring the backend back up.
    - Verify that the background repair eventually places the file on the third backend.
