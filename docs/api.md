# Control and Observability API

RepliStore provides a REST/HTTP API to inspect and manage the internal state of the system without requiring direct interaction with the filesystem or the cluster UDP lock transport.

---

## Authentication

HTTP endpoints require authentication using static bearer tokens configured on the server:

- **`/api/*` endpoints** use `api_token` configured in `config.yaml`.
- **`/streamz` endpoints** use `metrics_token` configured in `config.yaml`.

Clients must provide the corresponding token via the HTTP standard header:

```http
Authorization: Bearer <TOKEN>
```

---

## 1. System Health & Diagnostics

These endpoints provide visibility into the state of the local node and its backends.

### `GET /api/health`
Returns the operational status of the local instance.
- **Response:**
  ```json
  {
    "status": "healthy",
    "version": "1.1.0",
    "uptime_seconds": 86400
  }
  ```

### `GET /api/backends`
Exposes the status, latency, and free space of each configured SMB backend.
- **Response:**
  ```json
  [
    {
      "name": "nas-01",
      "address": "192.168.1.10:445",
      "healthy": true,
      "latency_ms": 12,
      "free_space_bytes": 4398046511104,
      "total_space_bytes": 8796093022208
    },
    {
      "name": "nas-02",
      "address": "192.168.1.11:445",
      "healthy": false,
      "last_error": "connection timeout after 5s",
      "latency_ms": -1,
      "free_space_bytes": 0,
      "total_space_bytes": 0
    }
  ]
  ```

---

## 2. Cluster & Lock Management

Endpoints to inspect peer discovery and active distributed lock table leases.

### `GET /api/cluster/peers`
Lists all discovered peers in the RepliStore cluster, their heartbeats, and sequence numbers.
- **Response:**
  ```json
  {
    "expected_cluster_size": 2,
    "current_cluster_size": 2,
    "peers": [
      {
        "node_id": "node-alpha",
        "advertise_addr": "192.168.1.50:5050",
        "last_seen_seconds_ago": 4,
        "seq": 1782390123908
      }
    ]
  }
  ```

### `GET /api/cluster/locks`
Lists active locks currently held by the cluster, showing the path and the owning node ID.
- **Response:**
  ```json
  [
    {
      "path": "/documents/report.docx",
      "lock_id": "c1f7a012...",
      "owner_node_id": "node-alpha",
      "lease_expires_in_seconds": 12.4
    }
  ]
  ```

---

## 3. Metadata Cache Management

Allows monitoring and manual invalidation/triggering of the in-memory metadata tree.

### `GET /api/cache/stats`
Retrieves cache usage statistics, hit/miss ratios, and lazy-indexing progress.
- **Response:**
  ```json
  {
    "total_cached_nodes": 14205,
    "directories_fully_indexed": 340,
    "cache_hits": 105942,
    "cache_misses": 432
  }
  ```

### `POST /api/cache/refresh`
Triggers a manual refresh of the cache for a specific path, or a full background rescan if no path is provided.
- **Request:**
  ```json
  {
    "path": "/documents"
  }
  ```
- **Response:**
  ```json
  {
    "status": "refresh_queued",
    "path": "/documents"
  }
  ```

---

## 4. Background Repair & Data Integrity

Inspects the background scrubbing worker and repair operations.

### `GET /api/repair/status`
Returns stats on degraded files, active healing copy tasks, and replica divergence count.
- **Response:**
  ```json
  {
    "scrub_active": true,
    "last_scrub_duration_seconds": 142,
    "degraded_files_count": 3,
    "divergent_files_count": 0,
    "active_repairs": [
      {
        "path": "/videos/archive.mp4",
        "source_backend": "nas-01",
        "target_backend": "nas-02",
        "progress_percent": 45.2
      }
    ]
  }
  ```

---

## 5. File Management (Node-Free Access)

Provides direct file upload, download, and deletion capability via the API. This enables clients to interface with the cluster storage without running a FUSE mount. Operations utilize VFS replication and quorum write flows underneath.

### `GET /api/meta/*path`
Retrieves metadata for the file or directory at the specified path.
- **Headers:**
  - `Accept: application/json`
- **Response for File:**
  ```json
  {
    "name": "report.docx",
    "path": "/documents/report.docx",
    "is_dir": false,
    "size_bytes": 1048576,
    "modified_time": "2026-06-16T10:45:20Z",
    "generation": 4
  }
  ```
- **Response for Directory:**
  ```json
  {
    "name": "documents",
    "path": "/documents",
    "is_dir": true,
    "children": [
      {
        "name": "report.docx",
        "is_dir": false,
        "size_bytes": 1048576,
        "modified_time": "2026-06-16T10:45:20Z",
        "generation": 4
      }
    ]
  }
  ```
- **Error Response:**
  - `404 Not Found` if path does not exist.

### `GET /api/data/*path`
Downloads the raw file data at the specified path.
- **If the path is a file:**
  - **Headers:**
    - `Accept: application/octet-stream`
  - **Response:**
    - `200 OK` with raw data stream
- **If the path is a directory:**
  - **Response:**
    - `400 Bad Request` (cannot read raw bytes of a directory)
- **Error Response:**
  - `404 Not Found` if path does not exist.

### `PUT /api/data/*path`
Uploads or overwrites the file at the specified path. Intermediate directories on the way are automatically created.
- **Headers:**
  - `Content-Type: application/octet-stream`
- **Response:**
  ```json
  {
    "status": "uploaded",
    "path": "/documents/report.docx",
    "written_replicas": 2,
    "generation": 5
  }
  ```
- **Error Response:**
  - `400 Bad Request` if path is an existing directory.

### `DELETE /api/data/*path`
Deletes the file or directory by writing a tombstone.
- **Response:**
  ```json
  {
    "status": "deleted",
    "path": "/documents/report.docx"
  }
  ```

---

## 6. Configuration & Control

### `POST /api/shutdown`
Triggers a clean, graceful shutdown of the node (releases locks, updates peer registry, unmounts FUSE, and closes SMB connections).
- **Response:**
  ```json
  {
    "status": "shutting_down"
  }
  ```
