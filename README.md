# RepliStore

RepliStore is a distributed, FUSE-based replicated storage system written in Go. It aggregates multiple SMB2/SMB3 network shares into a single unified mount point, providing file-level replication and high-performance metadata access.

## Features

- **SMB2/3 Native Connectivity:** Directly manages connections to remote shares without requiring OS-level mounting.
- **FUSE Interface:** Provides a standard filesystem interface to the operating system.
- **Peer-to-Peer (P2P) Distributed Locking:** Masterless quorum locking over HMAC-authenticated UDP datagrams, with node discovery through the shared backends (no multicast or shared L2 required).
- **File-Level Replication:** Configurable replication factor (RF) ensuring data redundancy across multiple backends.
- **Quorum-Based Write Consistency:** Configurable write quorum (WQ) to ensure that file writes and creations are acknowledged by a minimum number of backends.
- **Background Repair:** Automatic background worker that detects and restores degraded files (files with missing replicas).
- **Metadata Pre-Caching:** Aggressive startup scanning and in-memory caching for near-instant directory listings and file lookups.
- **Stateless Design:** No local database required; the remote SMB shares remain the ultimate source of truth.

## Documentation

For detailed information about RepliStore's design, architecture, and operation, please refer to the [**RepliStore Documentation**](docs/index.md).

- [Usage Guide: Use Cases & Tradeoffs](docs/usage.md)
- [Multi-Client Deployment & Limitations](docs/multi-client.md)
- [Architecture Overview](docs/architecture.md)
- [Component Details](docs/components/)
- [Operational Flows](docs/flows/)
- [Configuration Guide](docs/configuration.md)
- [Control & Observability API](docs/api.md)
- [Testing Guide](docs/testing.md)
- [Roadmap & Known Gaps](ROADMAP.md)
- [Code Review Findings](REVIEW.md)

## Architecture

RepliStore consists of four primary layers:
1. **Frontend (FUSE):** Translates OS syscalls into VFS operations.
2. **Virtual File System (VFS):** Manages the unified namespace, replication logic, and metadata cache.
3. **Cluster (P2P):** Handles node discovery and distributed locking across multiple instances.
4. **Backend (SMB):** Handles raw I/O and connectivity to the storage providers.

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
write_quorum: 1          # backends that must ack a write (defaults to replication_factor)
cache_refresh_interval: "5m"

# Optional: Enable P2P Cluster. When listen_addr is set, the other three
# cluster fields are required.
listen_addr: ":5050"                  # UDP port for the lock server
advertise_addr: "192.168.1.50:5050"   # host:port peers use to reach this node
cluster_secret: "change-me-16chars+"  # shared HMAC secret, same on all nodes (min 16 chars)
expected_cluster_size: 2              # total nodes in the cluster

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
