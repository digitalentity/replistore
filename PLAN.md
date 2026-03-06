# Implementation Plan: Persistent Lock-File DLM

This plan outlines a new Distributed Lock Manager (DLM) using persistent marker files and atomic creation (`O_EXCL`) to work around limitations in the current SMB library.

## 1. Objectives
- Implement distributed locking without external dependencies.
- Ensure atomicity using SMB `FILE_CREATE` (via `os.O_CREATE | os.O_EXCL`).
- Provide safety through quorum-based lock ownership.

## 2. Technical Requirements

### 2.1. Atomic Locking
- **Lock File:** `.replistore/locks/<sha256(path)>.lock`
- **Acquisition:** Use `OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)`.
- **Exclusivity:** If the file exists, `O_EXCL` ensures the SMB server returns an error, preventing multiple instances from claiming the same lock file.

### 2.2. Lock Ownership & Heartbeats
- **Owner ID:** Each RepliStore instance generates a unique UUID on startup.
- **Lock Content:** The lock file contains the `OwnerID` and a `Timestamp`.
- **Heartbeat:** The instance holding the lock must periodically update the `Timestamp` in the lock file (e.g., every 10 seconds).

### 2.3. Quorum Rule
- **Lock Quorum ($LQ$):** A strict majority of all configured backends ($LQ = \lfloor N/2 \rfloor + 1$).
- **Success:** An instance owns the lock if it successfully creates (or already owns and has updated) the lock file on a quorum of backends.

### 2.4. Recovery (Stale Lock Cleanup)
- **Stale Threshold:** A lock is considered "stale" if its `Timestamp` is older than 30 seconds.
- **Cleanup Logic:** If an instance encounters an existing lock file, it reads the content. If the lock is stale, it attempts to `Remove()` it and then re-acquire it using `O_EXCL`.

## 3. Implementation Steps

1.  **VFS Lock Manager:**
    - Implement `internal/vfs/lock.go` with `Acquire`, `Heartbeat`, and `Release` logic.
    - Handle the "Stale Lock" detection by reading file contents from backends.
2.  **FUSE Integration:**
    - Wrap mutation methods with lock acquisition.
    - Ensure the lock is released or heartbeated during long operations.
3.  **Repair Manager:**
    - Add a background task to clean up globally stale locks.

## 4. Risks & Mitigations
- **Clock Skew:** Rely on relative time intervals and conservative stale thresholds (30s) to mitigate minor drifts between instance clocks and NAS timestamps.
- **I/O Overhead:** Each lock check requires a `Stat` or `Read`. Mitigation: Use heartbeats only for long-held locks or frequent writes to the same path.
