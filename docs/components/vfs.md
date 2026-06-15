# VFS and Metadata Cache Component

The VFS layer is the brain of RepliStore. It manages the unified namespace and tracks which files are stored on which backends.

## Metadata Cache

The `Cache` structure is an in-memory tree of `Node` objects. Each node contains:
- `Metadata`: Name, path, size, mode, mtime, and a list of `Backends`.
- `Children`: A map of child names to `Node` pointers for directories.
- `Mu`: A read-write mutex for thread-safe access to the node's properties and children.

### Startup (Warmup Phase)
During startup, `Cache.Warmup` performs a parallel recursive scan (`Walk`) of all configured backends in the background. It populates the cache with the unified view of all files while the mount is already serving requests; directories not yet fully indexed are fetched on demand.
- If a file exists on multiple backends, its replicas are reconciled (see below) into a single entry.
- The `Backends` list in the metadata stores all locations for that file.
- The reserved `.replistore` tree (peer registry, sidecars, tombstones) is excluded from the scan and never appears in the namespace.

### Replica Reconciliation (Generation-Aware)
RepliStore does not trust raw backend mtimes as version vectors — each SMB server stamps mtime with its own clock. Instead, every write session bumps a per-file **generation counter** stored in a sidecar at `.replistore/meta/<path>.json` on each backend holding a replica (a replica with no sidecar reports generation 0, i.e. a legacy file). Merge rules:
1. **Higher generation wins outright:** its metadata and backend set replace the existing ones, regardless of mtime or size.
2. **Equal generation, equal size:** the same version observed on different backends — backend lists are *unioned*. This is what kills post-write repair churn: replicated writes leave equal-generation, equal-size replicas with divergent server-stamped mtimes, which must merge, not split.
3. **Equal generation, different size:** fall back to `(mtime, size)` last-writer-wins (a known residual; see REVIEW.md C4).

Directories are always treated as union types: backend lists merge and mtime/size are ignored for conflict resolution.

Sidecars are read lazily, only for genuine conflicts (different sizes) or tombstoned paths, so the steady-state sync pass costs zero sidecar reads.

### Tombstones (Durable Deletes)
Deletes and renames record a tombstone at `.replistore/tombstones/<path>.json` carrying the deletion generation. Reconciliation suppresses any replica at or below the tombstone's generation, so a backend that missed the delete cannot resurrect the file. The repair manager enforces tombstones at scrub start (deleting zombie replicas) and garbage-collects a tombstone once the path is verified absent on every responding backend. Directory tombstones are not yet implemented (REVIEW.md C6-dirs).

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
