# RepliStore Agent Instructions

This document provides foundational mandates, technical context, and development guidelines for AI agents working on RepliStore.

These instructions take precedence over general defaults.

## Commands & Validation Workflow

After any code modification, always perform the following validation steps:

1. **Build Check:** Run the compiler to ensure it builds successfully:
   ```bash
   go build ./cmd/replistore/...
   ```
2. **Test Suite:** Run the tests to verify correctness:
   ```bash
   # Run all tests
   go test ./...
   go test -v ./...
   go test -race ./...

   # Run specific tests (examples)
   go test ./internal/fuse/ -run TestLookup
   go test ./internal/vfs/ -run TestDistributedLock
   ```
   *Note: RepliStore relies heavily on mock-based testing in `internal/test/` to verify distributed logic without actual NAS hardware.*
3. **Documentation:** If a feature adds a configuration field or changes a flow, update the corresponding file in `docs/`.

## Engineering Standards

- **Minimalism:** Prefer the Go standard library (`net/rpc`, `net/http`, etc.). External dependencies must be justified and reviewed for "bloat."
- **I/O Resilience:** All backend and cluster RPC operations must support `context.Context` with appropriate timeouts.
- **Logging:** Use `sirupsen/logrus`. Component-specific loggers should include `component` and `path` or `node_id` fields.
- **Concurrency:** Use `golang.org/x/sync/errgroup` for parallel fan-out operations (e.g., writing to multiple replicas).

## Architecture

RepliStore is a FUSE-based replicated filesystem that aggregates multiple SMB2/3 shares into a single mount point. It is structured as a 4-layer stack:

```
FUSE Layer        (internal/fuse/)    — translates OS syscalls to VFS operations
VFS Layer         (internal/vfs/)     — metadata cache, replication logic, backend selection
Cluster Layer     (internal/cluster/) — mDNS peer discovery, RPC-based distributed locking
Backend Layer     (internal/backend/) — SMB2/3 connections, health monitoring
```

### Key Design Points

- **Source of truth is the remote SMB shares.** The in-memory metadata cache (`internal/vfs/cache.go`) is a performance layer only — it reconstructs from backends on startup via lazy/progressive warmup.
- **Lazy warmup:** The filesystem mounts immediately; directory metadata is populated on-demand. `FullyIndexed` on a cache node tracks whether a directory's children have been fetched.
- **Write quorum:** Writes fan out to `replication_factor` backends in parallel via `errgroup`. A write succeeds only if `write_quorum` backends confirm; otherwise all successful writes are rolled back.
- **Distributed locking:** Multi-client safety uses masterless quorum over mDNS-discovered peers (`internal/cluster/`). Lamport logical clocks order lock requests. Quorum is also enforced for `Mkdir` and `Remove`.
- **Background repair:** `RepairManager` (`internal/fuse/repair.go`) periodically finds degraded files (present on fewer backends than `replication_factor`) and re-replicates them.

### Component Interactions

`main.go` wires everything together:
1. Load config → initialize SMB backends → start cluster (if `listen_addr` configured) → start health monitor → initialize cache → start background warmup and sync → mount FUSE → start repair manager.

The FUSE `Dir` and `File` nodes hold a reference to the VFS cache and call through to backend operations. The `selector.go` in VFS chooses which backends to read from (healthy, round-robin) and which to write to (all replicas up to RF).

### Testing

Tests use mock backends from `internal/test/mocks.go` — no real NAS hardware needed. `MockBackend` is configurable to simulate failures, delays, and partial availability to exercise quorum and failover paths.
