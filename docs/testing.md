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

## Mock-Based Testing
RepliStore uses `github.com/stretchr/testify/mock` to simulate backend behavior. This allows testing complex scenarios like:
- **Read Failover:** Verifying that the system correctly switches to another replica if the primary one fails during a read.
- **Parallel Writes:** Ensuring that data is sent to multiple backends simultaneously.
- **Metadata Merging:** Checking that the cache correctly merges file information from different shares.

## Key Test Locations

### `internal/fuse/fs_test.go`
Tests the FUSE frontend operations (Lookup, Create, Mkdir, Read, Write, Remove). It mocks the backends and verifies that the correct VFS and backend calls are made.

### `internal/vfs/cache_test.go`
Tests the metadata cache logic, including upserting, retrieving, and merging metadata from multiple backends.

### `internal/config/config_test.go`
Tests configuration loading and environment variable expansion.

## Failover Test Case Example
The `TestFile_Read_Failover` in `internal/fuse/fs_test.go` demonstrates how the system handles a read error:
1. It mocks a read failure on the first backend.
2. It verifies that the handle is closed.
3. It verifies that a new handle is opened on a second backend and the read is retried successfully.
