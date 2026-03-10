# [COMPLETED] Implementation Plan: Missing Test Coverage

This plan outlines the strategy for closing gaps in the RepliStore test suite, focusing on new features and edge cases in the distributed VFS and Cluster layers.

## 1. Goal
Achieve high confidence in the correctness of the Distributed Lock Manager (DLM), the Lazy Warmup mechanism, and quorum-based metadata operations through automated unit and integration tests.

## 2. Priority 1: Lazy Warmup & On-Demand Fetching
The recent refactoring to a "Serve-while-Scanning" model needs empirical validation of its fallback logic.

### 2.1. VFS On-Demand Tests (`internal/vfs/cache_test.go`)
- **TestFetchEntry_Success:** Verify that `FetchEntry` correctly merges metadata from multiple mock backends and populates the cache.
- **TestFetchEntry_NotFound:** Verify that `FetchEntry` returns `os.ErrNotExist` when all backends return errors/missing.
- **TestFetchDir_Partial:** Verify that `FetchDir` only fetches entries for the specific path and correctly sets the `FullyIndexed` flag upon completion.

### 2.2. FUSE Integration Tests (`internal/fuse/fs_test.go`)
- **TestLookup_LazyTrigger:** Mock a directory as `FullyIndexed: false` and verify that a FUSE `Lookup` call triggers a backend `Stat` via `FetchEntry`.
- **TestReadDir_LazyTrigger:** Verify that `ReadDirAll` triggers `FetchDir` when the directory is not fully indexed.

## 3. Priority 2: Distributed Lock Manager (DLM)
The DLM is a critical component for data integrity in multi-client clusters but currently lacks comprehensive failure-mode testing.

### 3.1. Lock Acquisition & Quorum (`internal/cluster/rpc_test.go`)
- **TestRequestLock_QuorumSuccess:** Simulate a cluster of 3 nodes and verify that a lock is granted when 2 nodes respond with `LockOK`.
- **TestRequestLock_QuorumFailure:** Verify that acquisition fails and performs a rollback when a quorum of nodes cannot be reached (e.g., network partition).
- **TestLockConflict_LamportOrdering:** Verify that two simultaneous lock requests for the same path are resolved deterministically using Lamport logical clocks.

### 3.2. Lease Renewal & Fencing (`internal/vfs/lock_test.go`)
- **TestLockRenewal_Success:** Verify that the background renewal goroutine successfully extends the lease on both local and remote nodes.
- **TestLockRenewal_Expiration:** Simulate a renewal failure and verify that `lock.IsValid()` becomes `false` after the TTL expires.

## 4. Priority 3: Metadata Consistency & Quorum
Verify that all metadata-modifying operations respect the `write_quorum` setting.

### 4.1. Quorum Enforcement (`internal/fuse/fs_test.go`)
- **TestMkdir_Quorum:** Verify that `Mkdir` fails if fewer than `write_quorum` backends succeed. (Note: This will also serve as validation for the proposed fix in PROPOSAL.md).
- **TestRemove_Quorum:** Verify that `Remove` respects the quorum and handles partial backend availability.

## 5. Implementation Steps

### Phase 1: Mock Enhancements
1. Update `internal/test/mocks.go` to include more flexible `Stat` and `ReadDir` behaviors (e.g., returning different values for different nodes to test merging).

### Phase 2: VFS & FUSE Unit Tests
1. Implement the VFS-level `Fetch` tests.
2. Implement the FUSE integration tests for lazy lookup.

### Phase 3: Cluster & DLM Tests
1. Create a new `internal/cluster/rpc_test.go` to test the RPC methods using local listeners.
2. Implement the distributed lock quorum and renewal tests.

## 6. Verification
- All tests must pass with `go test -v ./...`.
- Use `go test -race ./...` to ensure no new race conditions were introduced by the lazy warmup or background renewal logic.
