# Configuration

RepliStore uses a YAML-based configuration file. Environment variables are expanded automatically.

## Configuration File Structure (`config.yaml`)

```yaml
# The local path where RepliStore will be mounted.
mount_point: "/mnt/replistore"

# Number of copies for each file.
replication_factor: 2

# How often to re-scan the backends to detect external changes.
cache_refresh_interval: "5m"

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

### `cache_refresh_interval` (duration string)
The interval between periodic scans of the backends. For example: `10s`, `5m`, `1h`.

### `backends` (list)
A list of backend configurations. Each backend must have a unique `name`.
- `name`: Unique identifier for the backend.
- `address`: IP address or hostname and port (e.g., `192.168.1.10:445`).
- `share`: The name of the SMB share to mount.
- `user`, `password`: Credentials for the SMB share.
- `domain`: (Optional) The SMB/NTLM domain.

## Environment Variables
RepliStore supports `os.ExpandEnv` on the configuration file. You can use `${VAR}` or `$VAR` syntax to inject sensitive information like passwords from your environment.
