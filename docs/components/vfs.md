# VFS and Metadata Cache Component

The VFS layer is the brain of RepliStore. It manages the unified namespace and tracks which files are stored on which backends.

## Metadata Cache

The `Cache` structure is an in-memory tree of `Node` objects. Each node contains:
- `Metadata`: Name, path, size, mode, mtime, and a list of `Backends`.
- `Children`: A map of child names to `Node` pointers for directories.
- `Mu`: A read-write mutex for thread-safe access to the node's properties and children.

### Startup (Warmup Phase)
During startup, RepliStore attempts to load the metadata cache from local disk (if `state_dir` is configured and a saved cache exists).
- **If disk cache exists:** It loads instantly, enabling immediate mount and survival during backend outages.
- **If disk cache does not exist:** `Cache.Warmup` performs a parallel recursive scan (`Walk`) of all configured backends in the background. It populates the cache with the unified view of all files while the mount is already serving requests; directories not yet fully indexed are fetched on demand.
- Replicas are reconciled (see below) into a single entry.
- The reserved `.replistore` tree is excluded from the scan and never appears in the namespace.

### Cache Disk Persistence
To survive crash scenarios and node restarts, the cache is saved to disk:
- **Periodically:** Every 30 seconds via a background save loop.
- **Graceful Shutdown:** Instantly when the process intercepts termination signals (SIGINT, SIGTERM).
- **Atomic Writing:** Writes state to a temporary file (`cache.json.tmp`) and renames it atomically to prevent corruption.

### Soft-Timeout / Stale Re-validation
If a cached metadata entry is accessed and its age exceeds `CacheTTL` (configured via `cache_refresh_interval`):
- FUSE lookup and directory listing operations trigger a lazy background fetch to re-validate it.
- **If backend is online:** The cache is updated with the fresh state.
- **If backend confirms file deletion (ErrNotExist):** The entry is permanently evicted.
- **If backend is offline (ErrUnavailable/transient error):** The entry is **not** evicted; the filesystem falls back to serving the stale cached data instead of failing with I/O errors.

### Replica Reconciliation (Generation-Aware)
RepliStore does not trust raw backend mtimes as version vectors — each SMB server stamps mtime with its own clock. Instead, every write session bumps a per-file **generation counter** stored in a metadata document on each backend holding a replica (a replica with no document reports generation 0, i.e. a legacy file). The document is keyed by the SHA-256 of the data path, sharded two levels deep — `.replistore/meta/<h0>/<h1>/<sha256hex>.json` — and records the data path inside itself. Merge rules:
1. **Higher generation wins outright:** its metadata and backend set replace the existing ones, regardless of mtime or size.
2. **Equal generation, equal size:** the same version observed on different backends — backend lists are *unioned*. This is what kills post-write repair churn: replicated writes leave equal-generation, equal-size replicas with divergent server-stamped mtimes, which must merge, not split.
3. **Equal generation, different size:** fall back to `(mtime, size)` last-writer-wins (a known residual; see REVIEW.md C4).

Directories are always treated as union types: backend lists merge and mtime/size are ignored for conflict resolution.

During a lazy single-path fetch, the document is read for each backend holding the replica. The sync pass enumerates deletions by walking the whole `meta/` tree (see Tombstones), so it reads every metadata document; conflict resolution reads no extra documents beyond that.

### Tombstones (Durable Deletes)
A sidecar and a tombstone are the **same document**: a tombstone is a metadata document with its `Deleted` flag set, carrying the deletion generation. Deletes and renames write it to the path's key (`.replistore/meta/<h0>/<h1>/<sha256hex>.json`), overwriting any prior live sidecar; a subsequent live write clears the flag. Reconciliation suppresses any replica at or below the tombstone's generation, so a backend that missed the delete cannot resurrect the file. Because deletions share the one metadata tree, sync enumerates them by walking `meta/` and filtering on the `Deleted` flag (recovering the data path from inside each document). The repair manager enforces tombstones at scrub start (deleting zombie replicas) and garbage-collects a tombstone — without disturbing a live sidecar that shares the key — once the path is verified absent on every responding backend. Directory tombstones and descendant re-keying on directory rename are fully implemented.

### Consistency
- **External Changes:** RepliStore performs periodic background synchronization (controlled by `cache_refresh_interval`). During each sync, it re-scans the backends to discover new files, modifications, and deletions, reconciling replicas with the generation-aware rules above. This ensures the in-memory cache eventually reconciles with the state of the SMB shares.
### Internal Changes
Operations performed through RepliStore (Create, Write, Mkdir, Remove, Rename) immediately update the metadata cache, ensuring strict consistency for its own operations.

## Distributed Locking (DLM)

When clustering is enabled, the VFS layer coordinates all metadata-modifying operations through the **Distributed Lock Manager**.

- **Lock Acquisition:** Before any write or directory modification occurs, the VFS layer acquires a `DistributedLock` for the target path from a quorum of active peers.
- **Lease Renewal:** A background loop periodically renews the lock lease (TTL) as long as the file is open for writing.
- **Fencing:** Every `Write` and `Sync` call verifies that the lock's `IsValid()` flag is still true. If the lease expires due to network lag or peer failures, subsequent writes are aborted to prevent data corruption.

## Namespace Operations

### `Rename`
The `Cache.Rename` method provides atomic tree-level moves within the in-memory metadata.
- **Distributed Coordination:** Both the source and destination paths are locked across the cluster before the rename proceeds on the backends.
- **Atomicity:** It uses a global cache-level lock (`Cache.Mu`) to ensure that a move is consistent for all concurrent readers and writers.
- **Recursive Update:** For directory moves, it recursively updates the `Path` property of all descendant nodes to reflect their new location.
- **Parent Management:** It automatically creates intermediate directory nodes at the destination if they are missing from the cache.
## Backend Selection

The `BackendSelector` interface defines how backends are chosen for read and write operations.

### `RandomSelector`
The default implementation which:
- **For Reads:** Randomly selects one healthy backend that contains the requested file.
- **For Writes:** Randomly selects $RF$ healthy backends for new file creation.

### `FirstSelector`
An alternative implementation that:
- **For Reads:** Always picks the first healthy backend available in the metadata list.
- **For Writes:** Picks the first $count$ healthy backends available in the configuration list.

### `SpaceAwareSelector`
An advanced implementation that:
- **For Reads:** Performs speed-based tie-breaking. It identifies the maximum speed rating among healthy backends containing the replica and randomly selects one from this fastest subset. Active read operations persist on their opened backend.
- **For Writes:** Balances storage utilization while ensuring backup replication. If write affinity tags are configured (e.g. cold storage targets), it guarantees at least one replica is placed on the healthy cold backend with the most free space. The remaining replicas are distributed to the other healthy backends with the most free space.

It uses a `HealthMonitor` to avoid selecting backends that are currently unreachable.

```mermaid
graph LR
    subgraph VFS Cache
        Root[/] --> Dir1[Folder A]
        Root --> Dir2[Folder B]
        Dir1 --> File1[file1.txt]
        File1 -.- Meta1[Size: 1MB<br/>Backends: NAS-1, NAS-2]
    end
```
