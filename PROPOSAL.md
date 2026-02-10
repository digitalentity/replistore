# RepliStore: Analysis & Improvement Proposal

## 1. Executive Summary

RepliStore presents a solid proof-of-concept for a FUSE-based aggregated storage system. It successfully demonstrates the core mechanics of FUSE-to-SMB translation, in-memory metadata caching, and basic replication. However, the current implementation lacks the robustness required for a production environment, specifically regarding **data consistency**, **partial failure handling**, and **concurrency control**.

This document outlines the identified critical issues and proposes a roadmap for hardening the system.

## 2. Critical Findings

### 2.1. Data Consistency & Partial Failures
*   **Issue:** Operations like `Write`, `Mkdir`, and `Remove` fan out to multiple backends without a transaction mechanism.
*   **Scenario:** If `Write` succeeds on Backend A but fails on Backend B:
    *   The operation returns an error (masking the partial success).
    *   The backends are now out of sync (divergent replicas).
    *   The Metadata Cache updates size based on the request, which might not reflect reality if all writes failed.
*   **Code Reference:** `internal/fuse/fs.go` -> `FileHandle.Write` waits for all go-routines but returns `lastErr`.

### 2.2. Concurrency & Deadlocks
*   **Issue:** The locking strategy is coarse-grained and potentially dangerous.
*   **Deadlock Risk:** In `Dir.Create`, the code holds the directory lock `d.node.Mu` while performing network I/O (`b.OpenFile`). If the backend is slow or hangs, the entire directory becomes unresponsive to other threads.
*   **Race Conditions:** In `FileHandle.Write`, `h.file.node.Meta.Size` is updated. While the node is locked for this update, `Attr` or `Read` operations might read stale or transient states depending on the exact locking sequence, although the current `RLock` usage minimizes this, the lack of coordination between `Write` completion and cache update is a weak point.

### 2.3. Resiliency
*   **Issue:** `File.Open` (ReadOnly) selects exactly **one** backend (the first in the list).
*   **Impact:** If that specific backend fails (e.g., connection drop) during the `Read` operation, the user request fails immediately, even if other healthy replicas exist. There is no **read failover**.
*   **Health Checking:** There is no active or passive health checking. The system keeps trying to use dead backends.

### 2.4. Backend Selection Logic
*   **Issue:** Backend selection for `Create` relies on Go's random map iteration order (`for name := range d.fs.Backends`).
*   **Impact:** This provides a basic random distribution but ignores backend capacity, latency, or health status.

### 2.5. Security
*   **Issue:** Credentials (passwords) are stored in plain text in `config.yaml` and held in memory.

## 3. Improvement Proposal

### 3.1. Phase 1: Robustness & Consistency (High Priority)

#### 3.1.1. Implement Read Failover
*   **Change:** Modify `FileHandle.Read` to retry on alternative backends if the primary selected backend returns an error.
*   **Benefit:** Immediate improvement in read availability.

#### 3.1.2. Structured Concurrency & Error Aggregation
*   **Change:** Replace ad-hoc `sync.WaitGroup` usage with `golang.org/x/sync/errgroup` or a custom "quorum" manager.
*   **Logic:**
    *   **Write:** Define a "Write Consistency Policy" (e.g., "All-or-Error" or "Quorum-Success"). If a write fails on one replica but succeeds on others, the file should be marked as "Degraded" in the metadata to prevent serving corrupt data or to trigger a repair.
    *   **Mkdir:** Should succeed if at least one backend succeeds, but queue a background "repair" task for the failed ones.

#### 3.1.3. Unlock During I/O
*   **Change:** In `Dir.Create` and `File.Open`, release the VFS locks before making blocking network calls to backends.
*   **Pattern:**
    1.  Lock VFS.
    2.  Check state / Reserve name.
    3.  Unlock VFS.
    4.  Perform Backend I/O.
    5.  Lock VFS.
    6.  Commit state (or rollback if I/O failed).

### 3.2. Phase 2: Architecture & Features (Medium Priority)

#### 3.2.1. Abstract Backend Selection
*   **New Interface:** `BackendSelector`
    *   `SelectForRead(file Metadata) Backend`
    *   `SelectForWrite(count int) []Backend`
*   **Implementations:** `RandomSelector`, `RoundRobinSelector`, `FreeSpaceSelector`.

#### 3.2.2. Background Health Check & Recovery
*   **Component:** `HealthMonitor` running in a goroutine.
*   **Function:** Periodically `Ping` backends. Update a `status` map.
*   **Integration:** The `BackendSelector` skips unhealthy backends.

#### 3.2.3. Secure Configuration
*   **Change:** Update `config` package to support environment variable expansion (e.g., `password: ${NAS_PASSWORD}`).

### 3.3. Phase 3: Code Quality (Low Priority)

*   **Fix:** `strings.ReplaceAll(path, "/", "")` is brittle. Use `path/filepath` or a dedicated path sanitizer for SMB.
*   **Fix:** `.gitignore` blocks the `cmd/replistore` directory. Rename the binary output or fix the ignore rule to `/replistore`.

## 4. Draft Implementation Plan (Next Steps)

1.  **Fix .gitignore:** Ensure `main.go` is tracked.
2.  **Refactor `FileHandle.Read`:** Add a loop to try multiple backends.
3.  **Refactor `Dir.Create`:** Move network calls outside the critical section.
4.  **Add `Write` Quorum:** Ensure writes return success only if min-replicas acknowledge.

This proposal aims to evolve RepliStore from a prototype to a resilient distributed storage client.
