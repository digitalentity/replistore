# RepliStore

RepliStore is a distributed, FUSE-based replicated storage system written in Go. It aggregates multiple SMB2/SMB3 network shares into a single unified mount point, providing file-level replication and high-performance metadata access.

## Features

- **SMB2/3 Native Connectivity:** Directly manages connections to remote shares without requiring OS-level mounting.
- **FUSE Interface:** Provides a standard filesystem interface to the operating system.
- **File-Level Replication:** Configurable replication factor (RF) ensuring data redundancy across multiple backends.
- **Quorum-Based Write Consistency:** Configurable write quorum (WQ) to ensure that file writes and creations are acknowledged by a minimum number of backends.
- **Background Repair:** Automatic background worker that detects and restores degraded files (files with missing replicas).
- **Metadata Pre-Caching:** Aggressive startup scanning and in-memory caching for near-instant directory listings and file lookups.
- **Stateless Design:** No local database required; the remote SMB shares remain the ultimate source of truth.

## Documentation

For detailed information about RepliStore's design, architecture, and operation, please refer to the [**RepliStore Documentation**](docs/index.md).

- [Usage Guide: Use Cases & Tradeoffs](docs/usage.md)
- [Architecture Overview](docs/architecture.md)
- [Component Details](docs/components/)
- [Operational Flows](docs/flows/)
- [Configuration Guide](docs/configuration.md)
- [Testing Guide](docs/testing.md)

## Architecture

RepliStore consists of three primary layers:
1. **Frontend (FUSE):** Translates OS syscalls into VFS operations.
2. **Virtual File System (VFS):** Manages the unified namespace, replication logic, and metadata cache.
3. **Backend (SMB):** Handles raw I/O and connectivity to the storage providers.

## Getting Started

### Prerequisites

- Go 1.25 or later.
- FUSE development headers installed on your system (e.g., `libfuse-dev` on Ubuntu).

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/digitalentity/replistore.git
   cd replistore
   ```

2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Build the binary:
   ```bash
   go build -o replistore ./cmd/replistore
   ```

### Configuration

Create a `config.yaml` file (see example below):

```yaml
mount_point: "/mnt/replistore"
replication_factor: 2
cache_refresh_interval: "5m"

backends:
  - name: "nas-01"
    address: "192.168.1.10:445"
    share: "data"
    user: "admin"
    password: "your_password"
    domain: ""

  - name: "nas-02"
    address: "192.168.1.11:445"
    share: "backup"
    user: "admin"
    password: "your_password"
```

### Running

Run the service with root privileges (required for FUSE mounting in many environments):

```bash
sudo ./replistore -config config.yaml
```

The system will start by "warming up" the metadata cache by scanning all connected backends. Once the scan is complete, the filesystem will be available at the specified `mount_point`.

## Testing

Run the comprehensive test suite:

```bash
go test ./...
```

The tests use a mock-based approach to verify VFS logic, FUSE interactions, and replication fan-out without requiring actual SMB connections.

## License

[MIT License](LICENSE)
