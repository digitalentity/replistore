# Configuration

RepliStore uses a YAML-based configuration file. Environment variables are expanded automatically.

## Configuration File Structure (`config.yaml`)

```yaml
# The local path where RepliStore will be mounted.
mount_point: "/mnt/replistore"

# Number of copies for each file.
replication:
  # The number of backend replicas to write for each file.
  factor: 2
  # Number of backends that must acknowledge a write for it to be considered successful.
  # Defaults to replication.factor if not specified.
  write_quorum: 1

# Minimum remaining lease headroom required before starting a write.
write_lease_buffer: "2s"

# Metadata cache settings
cache:
  # How often to re-scan the backends to detect external changes.
  refresh_interval: "5m"
  # The local directory where state files (like the serialized cache) are stored.
  state_dir: "/var/lib/replistore"

# Background repair settings
repair:
  # How often to check for degraded files and repair them.
  # Defaults to 1h.
  interval: "1h"
  # How long a file must stay under/over-replicated before repair acts, so a
  # brief backend reboot does not trigger churn. Should be >= repair_interval
  # (a shorter grace is raised to the interval). Defaults to 4h; 0 acts at once.
  grace: "4h"
  # Maximum number of concurrent repair operations.
  # Defaults to 2.
  concurrency: 2

# P2P Cluster configuration (optional)
# Enables distributed locking across multiple RepliStore instances.
# When listen_addr is set, the other fields become mandatory.
# cluster:
#   listen_addr: ":5050"                  # UDP port for the lock server
#   advertise_addr: "192.168.1.50:5050"   # host:port peers use to reach this node
#   secret: "change-me-16chars+"          # shared HMAC secret, same on all nodes (min 16 chars)
#   expected_cluster_size: 2              # total nodes in the cluster (>= 2)

# HTTP Control & Observability API Configuration (optional)
# api:
#   addr: ":8080"
#   api_token: "api-secret-token"
#   metrics_token: "metrics-secret-token"

# Selector configuration (optional)
selector:
  type: "smart"                       # "random", "first", or "smart" (default "random")
  write_affinity: ["cold-storage"]    # tags indicating cold-storage/backup targets for write affinity

# List of backends. Supports multiple backend types (e.g., "smb", "local").
backends:
  - name: "nas-01"
    type: "smb" # Defaults to "smb" if omitted
    address: "192.168.1.10:445"
    share: "data"
    user: "admin"
    password: "${NAS_PASSWORD}" # Env variable expansion
    domain: "WORKGROUP"
    speed: 10                         # Read speed rating (default 10)
    tags: ["hot"]                     # Custom tags (default empty list)

  - name: "local-01"
    type: "local"
    path: "/mnt/local_share"
    speed: 1
    tags: ["cold-storage"]
```

## Field Descriptions

### `mount_point` (string)
The absolute path on your local system where the RepliStore virtual filesystem will be available.

### `replication` (object)
Replication and write quorum settings.
- `factor` (int): The number of backends a new file should be written to. If the number of available backends is less than this value, RepliStore will use all available backends. Default is `2`.
- `write_quorum` (int): The number of backends that must acknowledge a successful write or create operation.
  - **Default:** If omitted — or set outside the valid range — `write_quorum` falls back to `factor`.
  - **Constraint:** Must be greater than 0 and less than or equal to `factor`.
  - **Use Case:** A value lower than `factor` (e.g., $WQ=2, RF=3$) allows writes to succeed even if some backends are temporarily down or slow.

### `cache` (object)
Metadata cache settings.
- `refresh_interval` (duration string): The interval between periodic scans of the backends. For example: `10s`, `5m`, `1h`.
- `state_dir` (string): The local directory path where cache-specific state files are stored (such as the serialized metadata cache file). Default is `/var/lib/replistore`.

### `repair` (object)
Background repair settings.
- `interval` (duration string): How often the background repair worker scans for degraded files (files with fewer than `replication_factor` replicas) and attempts to restore them.
- `grace` (duration string): How long a file must remain under- or over-replicated before the repair worker acts on it. A file that recovers within this window is never repaired or pruned, preventing replication churn. Defaults to `4h`; set to `0` to act on the next scan.
- `concurrency` (int): Maximum number of files being repaired simultaneously (default `2`).

### `cluster` (object, optional)
P2P clustering settings for distributed locking. All fields inside `cluster` become mandatory if `listen_addr` is set.
- `listen_addr` (string): The address where the internal UDP lock server will listen (e.g., `:5050` or `127.0.0.1:5050`).
- `advertise_addr` (string): The `host:port` address other nodes use to reach this node's lock server.
- `secret` (string): A shared HMAC-SHA256 secret, identical on all nodes, used to sign lock datagrams. Must be at least 16 characters.
- `expected_cluster_size` (int): The total number of nodes in the cluster (at least 2).

### `api` (object, optional)
HTTP Control and Observability API configuration.
- `addr` (string): The listen address for the HTTP server (e.g., `:8080`).
- `api_token` (string): Bearer token for accessing control API endpoints.
- `metrics_token` (string): Bearer token for accessing metrics/streamz endpoints.

### `write_lease_buffer` (duration string)
The minimum remaining lease duration required to start a write. If the write handle's lease has less than this time remaining before expiry, new write operations are rejected early to prevent out-of-lease backend writes. Must be less than the cluster lease TTL (5s).
- **Default:** `2s`.

### `selector` (object)
Determines how backends are selected for reads and writes.
- `type` (string): Selector algorithm to use. Options are:
  - `random` (default): Selects randomly among healthy backends.
  - `first`: Deterministically selects the first healthy backend in the list.
  - `smart`: Performs speed-based read tie-breaking and space-based write load balancing.
- `write_affinity` (list of strings): Tags indicating which backends are preferred as backup targets (e.g., cold-storage shares). Under the `smart` selector, at least one write replica is placed on a healthy backend possessing one of these tags, selecting the one with the most free space.

### `backends` (list)
A list of backend configurations. Each backend must have a unique `name`.
- `name`: Unique identifier for the backend.
- `type`: The type of the backend. Currently supported values:
  - `smb` (Default): SMB/CIFS share.
  - `local`: Local filesystem path.
- **For `smb` type backends:**
  - `address`: IP address or hostname and port (e.g., `192.168.1.10:445`).
  - `share`: The name of the SMB share to mount.
  - `user`, `password`: Credentials for the SMB share.
  - `domain`: (Optional) The SMB/NTLM domain.
- **For `local` type backends:**
  - `path`: The absolute path to a local directory to act as the backend storage.
- `speed` (int): A performance rating for read selection (default `10`). Slower backends (e.g. cold storage/HDDs) should be given lower values. Under `smart` reads, RepliStore will tie-break and only read from the fastest available replica.
- `tags` (list of strings): A list of tags associated with this backend, used for write affinity matching (default `[]`).
- `options`: (Optional) Map of custom string/interface options for future backend extensions.

## Environment Variables
RepliStore supports `os.ExpandEnv` on the configuration file. You can use `${VAR}` or `$VAR` syntax to inject sensitive information like passwords from your environment.
