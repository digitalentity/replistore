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
2. **Prometheus Metrics (`/streamz` endpoint)**:
   - Built on `prometheus/client_golang`. Served in the standard text exposition
     format at `GET /streamz`, protected by the `metrics_token` bearer token from
     `config.yaml`. Live gauges are read at scrape time, so they always reflect
     the current instant.
   - Exported series:
     - **Instance**: `replistore_up`, `replistore_build_info{version}`, `replistore_uptime_seconds`.
     - **FS latency** (histogram, label `op`): `replistore_fsop_duration_seconds{op}`
       → `_bucket`, `_sum`, `_count`.
     - **FS QPS / error rate** (counter, labels `op`, `error`): `replistore_fsop_ops_total`.
       Kernel-facing throughput: `sum(rate(replistore_fsop_ops_total[1m])) by (op)`;
       error rate: filter `error!="ok"`. `error` ∈ {ok, timeout, canceled, not_found,
       permission, exists, eof, error}.
     - **FS byte throughput** (counter, label `op`=read|write): `replistore_fsop_bytes_total`.
       Kernel-facing bytes (logical, per request): `rate(replistore_fsop_bytes_total[1m])`.
     - **Cache**: `replistore_cache_nodes`, `replistore_cache_directories_indexed`,
       `replistore_cache_hits_total`, `replistore_cache_misses_total`.
     - **Backend health** (labels `backend`, `type`=local|smb): `replistore_backend_up`
       (always present: 1 healthy / 0 down), `replistore_backend_free_bytes`,
       `replistore_backend_total_bytes` (space emitted only while healthy).
     - **Backend ping latency** (histogram, labels `backend`, `type`, `result`=success|error):
       `replistore_backend_ping_duration_seconds` → `_bucket`, `_sum`, `_count`.
       Recorded on every health probe; filter `result="success"` for clean latency.
     - **Per-backend operation latency** (histogram, labels `backend`, `type`,
       `op`, `result`=success|error): `replistore_backend_op_duration_seconds` →
       `_bucket`, `_sum`, `_count`. Times each individual backend round-trip, so a
       slow replica is visible on its own rather than folded into the FUSE
       aggregate. `op` ∈ {read, write, sync, open, readdir, stat, walk, mkdir,
       mkdir_all, remove, rename, truncate, chtimes}. Recorded by a transparent
       wrapper applied at backend construction. (Note: `read`/`write`/etc. here are
       backend ops; `replistore_fsop_duration_seconds{op}` is the FUSE boundary.)
     - **Per-backend QPS / error rate** (counter, labels `backend`, `type`, `op`,
       `error`): `replistore_backend_ops_total`. QPS:
       `sum(rate(replistore_backend_ops_total[1m])) by (backend, op)`; per-error-kind
       rate: by `error` (same category set as FS QPS). The latency histogram keeps
       only a coarse `result=success|error`; granular error kinds live on this
       counter, where extra labels cost no histogram buckets.
     - **Per-backend byte throughput** (counter, labels `backend`, `type`, `op`=read|write):
       `replistore_backend_bytes_total`. Physical per-backend I/O: a fan-out write
       counts once per replica, so this exceeds the logical FUSE byte count by the
       replication factor. Per-backend bandwidth:
       `rate(replistore_backend_bytes_total[1m])`.
     - **Cluster**: `replistore_cluster_size_current`, `replistore_cluster_size_expected`,
       `replistore_cluster_active_locks`, `replistore_replication_factor`,
       `replistore_cluster_raw_{total,used,free}_bytes`,
       `replistore_cluster_amortized_{total,used,free}_bytes`.
     - **Repair** (always present, zeroed when no repair manager): `replistore_repair_scrub_active`,
       `replistore_repair_last_scrub_duration_seconds`, `replistore_repair_degraded_files`,
       `replistore_repair_divergent_files`, `replistore_repair_active_repairs`.
     - **Runtime**: standard `go_*` and `process_*` collectors.
