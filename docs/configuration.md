# Configuration

RepliStore uses a YAML-based configuration file. Environment variables are expanded automatically.

## Configuration File Structure (`config.yaml`)

```yaml
# The local path where RepliStore will be mounted.
mount_point: "/mnt/replistore"

# Number of copies for each file.
replication_factor: 2

# Number of backends that must acknowledge a write for it to be considered successful.
# Defaults to replication_factor if not specified.
write_quorum: 1

# How often to re-scan the backends to detect external changes.
cache_refresh_interval: "5m"

# How often to check for degraded files and repair them.
# Defaults to 1h.
repair_interval: "1h"

# Maximum number of concurrent repair operations.
# Defaults to 2.
repair_concurrency: 2

# P2P Cluster configuration (optional)
# Enables distributed locking across multiple RepliStore instances.
# When listen_addr is set, the other three fields become mandatory.
listen_addr: ":5050"                  # UDP port for the lock server
advertise_addr: "192.168.1.50:5050"   # host:port peers use to reach this node
cluster_secret: "change-me-16chars+"  # shared HMAC secret, same on all nodes (min 16 chars)
expected_cluster_size: 2              # total nodes in the cluster (>= 2)

# List of SMB backend shares.
backends:
  - name: "nas-01"
    address: "192.168.1.10:445"
    share: "data"
    user: "admin"
    password: "${NAS_PASSWORD}" # Env variable expansion
    domain: "WORKGROUP"

  - name: "nas-02"
    address: "192.168.1.11:445"
    share: "backup"
    user: "admin"
    password: "secure_password"
```

## Field Descriptions

### `mount_point` (string)
The absolute path on your local system where the RepliStore virtual filesystem will be available.

### `replication_factor` (int)
The number of backends a new file should be written to. If the number of available backends is less than this value, RepliStore will use all available backends.

### `write_quorum` (int)
The number of backends that must acknowledge a successful write or create operation.
- **Default:** If omitted — or set outside the valid range — `write_quorum` falls back to `replication_factor`.
- **Constraint:** Must be greater than 0 and less than or equal to `replication_factor`.
- **Use Case:** A value lower than `replication_factor` (e.g., $WQ=2, RF=3$) allows writes to succeed even if some backends are temporarily down or slow.

### `cache_refresh_interval` (duration string)
The interval between periodic scans of the backends. For example: `10s`, `5m`, `1h`.

### `repair_interval` (duration string)
How often the background repair worker scans for degraded files (files with fewer than `replication_factor` replicas) and attempts to restore them.

### `repair_concurrency` (int)
Maximum number of files being repaired simultaneously.

### `listen_addr` (string, optional)
Enables the P2P Distributed Lock Manager (DLM) by specifying the address where the internal UDP lock server will listen (e.g., `:5050` or `127.0.0.1:5050`). If omitted, P2P features are disabled and the remaining cluster fields are ignored.

**Validation:** when `listen_addr` is set, `advertise_addr`, `cluster_secret`, and `expected_cluster_size` all become mandatory; the process refuses to start otherwise.

### `advertise_addr` (string, required when `listen_addr` is set)
The `host:port` address other nodes use to reach this node's lock server. It is published to peers through the backend-based peer registry (`.replistore/peers/<nodeID>.json` on every backend). Setting it explicitly avoids any interface-selection guesswork on multi-homed hosts; it must parse as a valid `host:port` with a non-empty host.

### `cluster_secret` (string, required when `listen_addr` is set)
A shared secret, identical on all nodes, used to HMAC-SHA256-sign every lock datagram (JWS compact serialization). Datagrams with bad signatures are dropped silently. Must be at least 16 characters long.

### `expected_cluster_size` (int, required when `listen_addr` is set)
The total number of nodes in the cluster. Lock quorum is derived from this value (`expected_cluster_size/2 + 1`), never from the live peer list, so a stale peer registry can only hurt availability, not consistency. Must be at least 2 when clustering is enabled; without `listen_addr` the value is unused.

### `backends` (list)
A list of backend configurations. Each backend must have a unique `name`.
- `name`: Unique identifier for the backend.
- `address`: IP address or hostname and port (e.g., `192.168.1.10:445`).
- `share`: The name of the SMB share to mount.
- `user`, `password`: Credentials for the SMB share.
- `domain`: (Optional) The SMB/NTLM domain.

## Environment Variables
RepliStore supports `os.ExpandEnv` on the configuration file. You can use `${VAR}` or `$VAR` syntax to inject sensitive information like passwords from your environment.
