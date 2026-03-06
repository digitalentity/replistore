# VFS and Metadata Cache Component

The VFS layer is the brain of RepliStore. It manages the unified namespace and tracks which files are stored on which backends.

## Metadata Cache

The `Cache` structure is an in-memory tree of `Node` objects. Each node contains:
- `Metadata`: Name, path, size, mode, mtime, and a list of `Backends`.
- `Children`: A map of child names to `Node` pointers for directories.
- `Mu`: A read-write mutex for thread-safe access to the node's properties and children.

### Startup (Warmup Phase)
During startup, `Cache.Warmup` performs a parallel recursive scan (`Walk`) of all configured backends. It populates the cache with the unified view of all files.
- If a file exists on multiple backends, its metadata is merged (largest size, latest mtime).
- The `Backends` list in the metadata stores all locations for that file.

### Consistency
- **External Changes:** RepliStore performs periodic background synchronization (controlled by `cache_refresh_interval`). During each sync, it re-scans the backends to discover new files, modifications, and deletions. This ensures the in-memory cache eventually reconciles with the state of the SMB shares.
- **Internal Changes:** Operations performed through RepliStore (Create, Write, Mkdir, Remove, Rename) immediately update the metadata cache, ensuring strict consistency for its own operations.

## Namespace Operations

### `Rename`
The `Cache.Rename` method provides atomic tree-level moves within the in-memory metadata.
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
