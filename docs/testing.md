# Testing Guide

RepliStore includes a comprehensive test suite that covers the VFS logic, FUSE interactions, and replication mechanisms without requiring a live SMB server.

## Running Tests

To run all tests:
```bash
go test ./...
```

To run with coverage:
```bash
go test -cover ./...
```

To check for data races (important for the lazy warmup, background sync, lock renewal, and discovery goroutines):
```bash
go test -race ./...
```

## Mock-Based Testing
RepliStore uses `github.com/stretchr/testify/mock` to simulate backend behavior. This allows testing complex scenarios like:
- **Read Failover:** Verifying that the system correctly switches to another replica if the primary one fails during a read.
- **Parallel Writes:** Ensuring that data is sent to multiple backends simultaneously.
- **Metadata Merging:** Checking that the cache correctly merges file information from different shares.

## Key Test Locations

### `internal/fuse/fs_test.go`
Tests the FUSE frontend operations (Lookup, Create, Mkdir, Read, Write, Remove). It mocks the backends and verifies that the correct VFS and backend calls are made.

### `internal/fuse/repair_test.go`
Tests the background repair manager, including identifying degraded files, selecting source/target replicas, and performing the data copy.

### `internal/vfs/cache_test.go` and `internal/vfs/cache_internal_test.go`
Tests the metadata cache: upserting, retrieving, and generation-aware reconciliation — higher generation wins, equal generation + equal size unions backend lists, conflict-driven sidecar reads, and tombstone suppression (a tombstoned path is evicted from the cache and reported absent by lazy fetch).

### `internal/vfs/sidecar_test.go`
Tests the version-metadata sidecars and deletion tombstones: path mapping, read/write round-trips, missing-sidecar (generation 0) handling, and the forced `Deleted` flag on tombstones.

### `internal/vfs/lock_test.go`
Tests the distributed lock client: quorum acquisition and rollback, same-node mutual exclusion via per-acquisition `LockID`, lease renewal, expiry, and acquire retries with backoff under contention.

### `internal/cluster/rpc_test.go` and `internal/cluster/transport_test.go`
Tests the lock manager and UDP transport: lock grant/renew/release semantics, expired-lease renewal rejection, grant garbage collection, and the JWS datagram wire format (sign/verify round-trip, tampered-signature and pinned-header rejection, request-ID matching, and context-deadline behavior over loopback).

### `internal/cluster/discovery_test.go`
Tests backend-based discovery: heartbeat writes to all backends, cross-backend peer merging by highest sequence number, expiry, graceful-leave removal, and that a poll reaching zero backends does not flap membership.

### `internal/config/config_test.go`
Tests configuration loading, environment variable expansion, and the clustering validation rules (`expected_cluster_size`, `advertise_addr`, and `cluster_secret` required and validated when `listen_addr` is set).

## Failover Test Case Example
The `TestFile_Read_Failover` in `internal/fuse/fs_test.go` demonstrates how the system handles a read error:
1. It mocks a read failure on the first backend.
2. It verifies that the handle is closed.
3. It verifies that a new handle is opened on a second backend and the read is retried successfully.
