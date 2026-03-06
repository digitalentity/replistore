# Implementation Plan: Fsync Support

This plan outlines the steps to implement `Fsync` support in RepliStore, ensuring that data written to the virtual filesystem is safely persisted to the underlying SMB storage backends.

## 1. Objectives
- Implement `fs.HandleFsyncer` and `fs.HandleFlusher` in the FUSE layer.
- Extend the `Backend` and `File` interfaces to support synchronization.
- Ensure that an `fsync` call from the OS only returns success if the data is successfully synchronized on a quorum of backends.

## 2. Technical Requirements

### 2.1. Backend Layer Updates (`internal/backend/backend.go`)
- **Add `Sync()` to `File` interface:** This method should trigger the underlying SMB `Flush` operation.
- **Implement `Sync()` in `SMBBackend`:** Wrap the `smb2.RemoteFile.Sync()` or `Flush()` method.

### 2.2. FUSE Layer Updates (`internal/fuse/fs.go`)
- **Update `FileHandle`:**
    - Implement `Flush(ctx, req)`: This is called by the kernel on every `close()`. It should trigger a sync to all backends.
    - Implement `Fsync(ctx, req)`: This is called when an application explicitly calls `fsync()` or `fdatasync()`.
- **Quorum Logic:**
    - Use parallel execution (via `errgroup` or `sync.WaitGroup`) to call `Sync()` on all open backend handles.
    - Check if the number of successful syncs meets the `WriteQuorum`.
    - If quorum is not met, return an error (e.g., `EIO`).

## 3. Implementation Steps

1.  **Interface Modification:**
    - Update `backend.File` interface to include `Sync() error`.
    - Update `SMBBackend`'s internal file wrapper to implement `Sync()`.

2.  **FUSE Handle Implementation:**
    - Add `Flush` method to `FileHandle` in `internal/fuse/fs.go`.
    - Add `Fsync` method to `FileHandle` in `internal/fuse/fs.go`.
    - Implement the fan-out and quorum check logic in a reusable private helper method `syncBackends(ctx)`.

3.  **Verification & Testing:**
    - **Unit Tests:** Update `internal/test/mocks.go` to include the `Sync()` method.
    - **Integration Test:** Add a test case in `internal/fuse/fs_test.go` that:
        - Mocks a file handle with 2 backends.
        - Calls `Fsync`.
        - Verifies that `Sync()` was called on both mock backend files.
        - Verifies success if both succeed.
        - Verifies failure/quorum behavior if one or more fail.

## 4. Considerations
- **Performance:** `Fsync` is a heavy operation. Parallelizing the calls to SMB backends is essential to minimize latency.
- **Error Handling:** If a `Sync()` fails on one backend but the quorum is still met, should we remove the "bad" backend from the replica list (similar to `Write`)? **Decision:** Yes, for consistency with the `Write` flow.
