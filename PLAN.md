# Implementation Plan: Lazy / Progressive Warmup

This plan outlines the transition from a "Scan-then-Serve" model to a "Serve-while-Scanning" model. This will allow RepliStore to mount instantly, even with millions of files on the backends.

## 1. Goal
RepliStore currently performs a full recursive metadata scan before the FUSE mount is active. We will refactor this to mount immediately and fetch missing metadata on-demand (lazily) while the full scan continues in the background.

## 2. Architecture Changes

### 2.1. Cache Tracking
We need to know if a directory in the `vfs.Cache` is "complete" (fully scanned) or "partial" (only contains on-demand entries).
- **VFS Update:** Add a `FullyIndexed` boolean to directory nodes in the `vfs.Cache`.
- **Logic:** If a `Lookup` fails in a directory where `FullyIndexed == false`, RepliStore must check the SMB backends before returning `ENOENT`.

### 2.2. The "On-Demand" Bridge
Implement a helper in the VFS or FUSE layer to resolve missing entries:
1. **Parallel Stat:** Issue a `Stat` call to all healthy backends for the missing path.
2. **Quorum Merge:** If a quorum of backends return the same metadata, inject the entry into the `vfs.Cache`.
3. **Transparent Return:** Serve the result to the FUSE request as if it were always in the cache.

### 2.3. Background Scan Refactoring
- The `Warmup` process will be moved to a background goroutine started after the mount.
- As the background walker completes a directory, it marks it as `FullyIndexed = true`.

## 3. Implementation Steps

### Phase 1: VFS Cache Refactoring
1. Modify `vfs.Node` to include the `FullyIndexed` flag.
2. Update `vfs.Cache.Upsert` to ensure background scan results don't overwrite fresher on-demand metadata.
3. Implement `vfs.Cache.FetchEntry(path)` which performs parallel backend `Stat` calls and populates the cache.

### Phase 2: FUSE Integration (The "Lazy" Logic)
1. Update `Dir.Lookup` in `internal/fuse/fs.go`:
   - If child missing AND `parent.FullyIndexed == false`: call `FetchEntry`.
2. Update `Dir.ReadDirAll`:
   - If `node.FullyIndexed == false`: perform a synchronous parallel `ReadDir` from backends, merge into cache, and mark as `FullyIndexed`.

### Phase 3: Startup Refactoring
1. Modify `cmd/replistore/main.go` to remove the blocking `cache.Warmup` call.
2. Start the background scan immediately after the FUSE server begins serving.

## 4. Resilience & Edge Cases
- **Negative Caching:** Ensure that if a file truly doesn't exist (all backends return 404), we don't repeatedly hit the backends for it during the same scan cycle.
- **Race Conditions:** Use mutexes to coordinate between the background walker and simultaneous on-demand fetches for the same directory.
- **User Feedback:** Optionally log a message when the "Initial background scan is complete."
