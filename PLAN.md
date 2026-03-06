# Implementation Plan: Atomic Multi-Backend Rename

This plan outlines the steps to implement `Rename` support in RepliStore, allowing files and directories to be moved or renamed across the unified namespace.

## 1. Objectives
- Implement the `Rename` syscall in the FUSE frontend.
- Extend the `Backend` interface to support file/directory renaming.
- Update the `vfs.Cache` to handle atomic node moves between parent directories.
- Maintain consistency across multiple backends using a quorum-based approach.

## 2. Technical Requirements

### 2.1. Backend Layer Updates (`internal/backend/backend.go`)
- **Add `Rename(oldPath, newPath string) error` to `Backend` interface.**
- **Implement `Rename` in `SMBBackend`:** Wrap `smb2.Share.Rename(old, new)`.

### 2.2. VFS Layer Updates (`internal/vfs/cache.go`)
- **Implement `Cache.Rename(oldPath, newPath string) error`:**
    - This method must atomically move a `Node` from its current parent's `Children` map to the new parent's map.
    - It must update the `Path` and `Name` fields of the moved node and all its descendants (for directory renames).
    - Use fine-grained locking or a global cache lock to ensure atomicity.

### 2.3. FUSE Layer Updates (`internal/fuse/fs.go`)
- **Implement `fs.NodeRenamer` on the `Dir` type.**
- **Rename Logic:**
    1. Identify the source node and its current backends.
    2. Identify the target parent path.
    3. **Ensure Target Path:** For each backend involved in the rename, ensure all parent directories for the destination path exist (equivalent to `mkdir -p`).
    4. Perform parallel `Rename` calls on all backends where the node exists.
    5. **Quorum Check:**
        - If successful renames >= `WriteQuorum`:
            - Update the `vfs.Cache`.
            - For any backends that failed the rename: remove them from the node's `Backends` list (marking the file as degraded at the new path).
        - If successful renames < `WriteQuorum`:
            - Attempt a "Best Effort Rollback" (rename back the successful ones).
            - Return an error (e.g., `EIO`) to the OS.

## 3. Implementation Steps

1.  **Backend Support:**
    - Update `backend.Backend` and `backend.SMBBackend`.
    - Update `test.MockBackend` for testing.

2.  **VFS Move Logic:**
    - Implement `Cache.Rename` with proper recursive path updates for directory moves.

3.  **FUSE Directory implementation:**
    - Implement `Rename(ctx, req, newDirNode)` on `Dir`.
    - Handle both file-to-file and dir-to-dir renames.
    - Implement the parallel fan-out and quorum logic.

4.  **Verification & Testing:**
    - **Unit Tests:** Verify `Cache.Rename` handles tree updates correctly.
    - **Integration Tests:** 
        - Rename a file within the same directory.
        - Move a file between different directories.
        - Rename a directory containing multiple files.
        - Verify quorum behavior (successful move with one failing backend).

## 4. Multi-Client & Safety Considerations
- **Local Atomicity:** The VFS cache will be consistent for the local instance.
- **Distributed Risk:** Without the proposed `Distributed Lock Manager (DLM)`, a rename operation is susceptible to races if another instance is simultaneously writing to or renaming the same file.
- **Repair Manager:** The `RepairManager` must be careful not to "undelete" a file from its old path while a rename is in progress. (A global lock would solve this; for now, we rely on the fact that `Rename` is relatively fast).

## 5. Limitations
- **Cross-Backend Rename:** Since RepliStore aggregates independent shares, a "Rename" only works if the backends for the source and destination are compatible. (In the current design, this is always true as we maintain the same backend names across the unified tree).
