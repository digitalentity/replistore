# RepliStore Design Document

## 1. Overview
RepliStore is a distributed, FUSE-based filesystem written in Go. It aggregates multiple SMB2/SMB3 network shares into a single unified mount point. It provides file-level replication for redundancy.

To ensure high read performance, the system maintains a comprehensive **Metadata Cache**. The SMB shares remain the authoritative source of truth, but their state is mirrored locally to serve metadata requests (listing, lookup, stat) instantly.

## 2. Key Objectives
*   **Protocol:** SMB2/SMB3 for backend storage.
*   **Interface:** FUSE (Filesystem in Userspace) for the frontend mount.
*   **Replication:** Configurable Replication Factor (RF) per system.
*   **Performance:** Read-heavy workload optimization via aggressive caching.
*   **Consistency:** Metadata is eventually consistent regarding external changes (discovered on scan), but strictly consistent for operations performed through RepliStore.

## 3. Architecture Layers

### 3.1. FUSE Layer (`frontend`)
*   Uses `bazil.org/fuse`.
*   Translates kernel syscalls (Open, Read, Write, Mkdir) into internal VFS operations.
*   Serves directory listings directly from the **Metadata Cache**.

### 3.2. Metadata Cache & VFS Layer (`vfs`)
This is the state manager.
*   **Structure:** An in-memory (or local temporary DB) tree representing the filesystem.
*   **Startup (Warmup Phase):**
    *   On startup, the system connects to all configured backends.
    *   It performs a full recursive scan (`Walk`) of every share.
    *   It builds a unified file tree. If `file.txt` exists on Share A and Share B, the cache records it as one file backed by `{ShareA, ShareB}`.
    *   The filesystem is not fully ready until the initial scan completes (or serves partial results with a warning).
*   **Background Sync:** Periodic re-scans or handling "Watcher" events (if SMB notifies support allows) to detect external changes.

### 3.3. Backend Layer (`backend`)
*   Uses `github.com/hirochachin/go-smb2`.
*   Manages persistent TCP connections.
*   Provides raw IO access to the shares.

## 4. Workflows

### 4.1. Directory Listing (`Readdir`) & Lookup
1.  **Hit Cache:** The VFS queries the local Metadata Cache.
2.  **Zero Latency:** Returns the file structure immediately without network round-trips.
3.  **Stale Data Risk:** If a file was added directly to the SMB share bypassing RepliStore, it won't appear until the next re-scan.

### 4.2. File Creation (`Create`)
1.  User creates `file.txt`.
2.  VFS selects $RF$ number of backends.
3.  VFS issues create commands to the selected backends.
4.  **Update Cache:** Upon success, the new file and its location mapping are immediately added to the Metadata Cache.

### 4.3. File Writing (`Write`)
1.  Incoming write buffer is fanned out to all mapped backends.
2.  **Metadata Update:** If the file size or modification time changes, the Metadata Cache is updated immediately.

### 4.4. File Reading (`Read`)
1.  VFS looks up the file in the Cache to find which backends hold it.
2.  VFS selects **one** healthy backend.
3.  Data is streamed from that backend.

## 5. Configuration (`config.yaml`)

```yaml
mount_point: "/mnt/replistore"
replication_factor: 2

# Cache Settings
cache_refresh_interval: "5m" # For detecting external changes

backends:
  - name: "nas-01"
    address: "192.168.1.10:445"
    share: "data"
    user: "admin"
    password: "secure_password"

  - name: "nas-02"
    address: "192.168.1.11:445"
    share: "backup"
    user: "admin"
    password: "secure_password"
```

## 6. Lifecycle & Warmup
1.  **Init:** Load Config.
2.  **Connect:** Dial all SMB backends.
3.  **Warmup:**
    *   Log: "Starting metadata scan..."
    *   Parallel Walk of all shares.
    *   Populate Cache.
    *   Log: "Scan complete. X files indexed."
4.  **Mount:** Open FUSE connection.
5.  **Serve:** Begin handling OS requests.