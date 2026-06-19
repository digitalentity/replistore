# Observability, Logging, and Error Handling Design

This document outlines the architectural plan to modernize error handling, logging, and observability/debuggability of RepliStore.

---

## 1. Contextual Structured Logging with `log/slog`

### Current State
- Logging uses `sirupsen/logrus`.
- Multi-client writes, lock renewals, cache syncs, and background repairs execute concurrently. Tracing a single transaction's lifecycle across FUSE, VFS, and SMB backend layers is extremely difficult.
- Log level and log format (e.g., JSON vs. Text) are hardcoded or defaults.

### Proposed Design
1. **Migration to Go Standard Library `log/slog`**:
   - Replace `sirupsen/logrus` with the built-in `log/slog` package.
   - Use `samber/slog-multi` to compose handlers:
     - Allows dynamic routing of logs to multiple destinations (e.g. `os.Stdout` for human/k8s logs, and a local circular buffer/file for the HTTP diagnostic API).
     - Allows middleware pipelines (e.g. injecting common attributes, filtering levels dynamically).
2. **Correlation/Request IDs via Context**:
   - Mint a unique `correlation_id` at FUSE entrypoints (`Attr`, `Lookup`, `Read`, `Write`, `Create`, etc.) and HTTP API handlers.
   - Generate correlation IDs as time-ordered Snowflake IDs using `github.com/bwmarrin/snowflake` with base36 encoding (`strconv.FormatInt(id.Int64(), 36)`).
   - Generate numeric 10-bit worker IDs by hashing the node ID/hostname.
   - Create a context logger wrapper that extracts the correlation ID from `context.Context` and appends it to log attributes automatically.
   - Propagate this context down through the VFS cache and Backend selector layers.
3. **Log Configuration**:
   - Add fields to `config.yaml`:
     ```yaml
     logging:
       level: "debug"   # debug, info, warn, error
       format: "json"   # json, text
     ```
4. **Standardized Fields**:
   - Enforce structured fields across all packages:
     - `path` - target file/directory relative path
     - `backend` - name of the backend (e.g., `nas-01`)
     - `node_id` - clustering node identifier
     - `lock_id` - distributed lock transaction ID
     - `duration_ms` - elapsed time for slow queries or background syncs

---

## 2. Robust Error Handling & POSIX Translation

### Current State
- Errors are returned as generic standard library errors (`errors.New`, `fmt.Errorf`) without wrapping (`%w`), making type assertion difficult.
- FUSE mounts must return standard POSIX error numbers (`syscall.Errno`). Currently, some errors are logged but returned as generic `EIO` (`syscall.EIO`) to the client, hiding network, lock conflict, or timeout root causes.

### Proposed Design
1. **Error Wrapping**:
   - Refactor critical backend and cluster operations to use `%w` for error wrapping.
   - Define domain-specific sentinel errors and types in `internal/errors/errors.go` (e.g., `ErrBackendDown`, `ErrQuorumFailed`, `ErrLockConflict`).
2. **FUSE Error Mapper**:
   - Implement an error translator in `internal/fuse/errors.go` to map project internal errors to appropriate POSIX errors:
     - `ErrLockConflict` / `ErrLockTimeout` $\rightarrow$ `syscall.ENOLCK` (No record locks available) or `syscall.EAGAIN`
     - `ErrBackendDown` $\rightarrow$ `syscall.EHOSTUNREACH` or `syscall.ENOTCONN`
     - `ErrQuorumFailed` $\rightarrow$ `syscall.EIO` (with detailed internal log)
     - `context.DeadlineExceeded` $\rightarrow$ `syscall.ETIMEDOUT`
3. **SMB Timeout Watchdogs**:
   - Since `go-smb2` calls do not natively support cancellation, implement the watchdog pattern: wrap calls in a select block with the context; if the context expires, close the underlying SMB TCP socket to force-fail the call quickly rather than hanging indefinitely.

---

## 3. Observability API & Prometheus Metrics

### Current State
- There is no HTTP runtime diagnostic interface.
- Monitoring backend status, peer registry, lock states, cache performance, and repair activity requires reading logs.

### Proposed Design
1. **REST/HTTP Control API**:
   - Implement an HTTP/REST server in `internal/api/server.go` exposing the endpoints defined in `docs/api.md`:
     - `GET /api/health` - Local service availability.
     - `GET /api/backends` - SMB/local backend status, latency, space constraints.
     - `GET /api/cluster/peers` - Discovered cluster peers and heartbeats.
     - `GET /api/cluster/locks` - Active distributed lock leases.
     - `GET /api/cache/stats` - Cached node statistics and indexing status.
     - `GET /api/repair/status` - Degraded/diverged files count and active operations.
   - Protect control endpoints with a static bearer token configured in `config.yaml` (`api_token`).
   - Use `samber/slog-http` middleware on the HTTP server to log requests structurally.
2. **Prometheus Metrics (`/streamz` endpoints)**:
   - Protect metrics endpoints with a separate static bearer token configured in `config.yaml` (`metrics_token`).
   - Expose system metrics for scrape collection:
     - **FS Latency**: `replistore_fs_operation_duration_seconds` (histogram by operation: read, write, lookup, etc.)
     - **Cache Hits**: `replistore_cache_hits_total`, `replistore_cache_misses_total`
     - **Backend Health**: `replistore_backend_healthy`, `replistore_backend_latency_seconds`
     - **Cluster state**: `replistore_cluster_peers_count`, `replistore_active_locks_count`
     - **Repair Health**: `replistore_degraded_files_count`, `replistore_divergent_files_count`, `replistore_repairs_total`
