# Proposal: Native NFSv4 Backend

This proposal details the architectural design for introducing a native NFSv4 backend driver in RepliStore, allowing direct connection to NFS exports without host OS mounting.

---

## 1. Motivation

Currently, RepliStore connects to network shares via SMB ([smb/backend.go](../../internal/backend/smb/backend.go)) or requires the host OS to mount target systems locally ([local/local.go](../../internal/backend/local/local.go)).

Adding support for NFSv4 exports via OS-level mounting requires root privileges and pollutes host mount points. We need a way to speak NFSv4 directly from user-space.

A native Go-based NFSv4 client backend driver will:
* Remove host OS mount dependencies.
* Allow cross-platform Unix/Windows interoperability within the same RepliStore instance.
* Map directly to NFSv4 RPCs, which naturally support random-access `WriteAt` and `ReadAt`.

---

## 2. Configuration Options

A new backend type `nfs` will be introduced. Below is the proposed configuration block in [config.yaml](../../config.yaml):

```yaml
backends:
  - name: "nfs-storage-01"
    type: "nfs"
    address: "192.168.1.15:2049"       # Host and port of the NFSv4 server
    export: "/srv/exports/replistore"  # NFS export path
    auth:
      type: "sys"                      # "sys" (AUTH_SYS) or "krb5" (Kerberos)
      uid: 1001                        # UID asserted on NFS calls (for AUTH_SYS)
      gid: 1001                        # GID asserted on NFS calls (for AUTH_SYS)
    speed: 30                          # Read speed rating (higher rating for fast LAN storage)
    tags: ["hot"]
```

---

## 3. Library Selection

We will use a pure-Go NFS client library (such as `github.com/vmware/go-nfs-client/nfs` or `github.com/chirino/go-nfs/nfs`) to handle NFSv4 protocol serialization and RPC exchanges over TCP:
* Supports compound procedures (reducing latency by chaining LOOKUP, OPEN, and READ).
* Supports stateful OPEN/CLOSE operations.
* Supports client-side authentication mechanisms.

---

## 4. Architecture and File Operations Mapping

The `nfs` backend driver conforms to the [Backend](../../internal/backend/backend.go#L28) interface. Unlike cloud object storage, NFSv4 maps one-to-one with POSIX filesystem operations:

### 4.1. File Operations

* **OpenFile:**
  * Uses the library's NFS client to send an `OPEN` RPC call.
  * Returns a stateful file handle wrapper satisfying the [File](../../internal/backend/backend.go#L21) interface.
* **ReadAt & WriteAt:**
  * Maps directly to NFS `READ` and `WRITE` RPCs. Since NFSv4 supports byte offsets natively, we perform true random-access writes without staging files.
* **Stat & Statfs:**
  * Maps to NFS `GETATTR` to fetch file mode, size, and mod times, and `FSINFO` / `PATHCONF` for free space.

### 4.2. Directory Operations

* **ReadDir & Walk:**
  * Maps to NFS `READDIR` RPCs. Converts NFS attributes to [FileInfo](../../internal/backend/backend.go#L13) objects.
* **Mkdir & MkdirAll:**
  * Maps to NFS `CREATE` with type `NF4DIR`.
* **Remove / Rename:**
  * Maps to NFS `REMOVE` and `RENAME`.

---

## 5. Security & Authentication

NFSv4 utilizes security flavors negotiated during connection establishment:

1. **AUTH_SYS (Standard Unix Security):**
   * The client asserts the configured `uid` and `gid` in the RPC header.
   * Simple, standard for internal trusted local area networks (LANs).
2. **RPCSEC_GSS (Kerberos):**
   * Negotiates authentication using a Kerberos credential cache or keytab file.
   * Provides transport encryption (privacy) and integrity protection where needed.

---

## 6. Comparison: SMB vs. NFSv4 in RepliStore

| Aspect | SMB Backend | NFSv4 Backend |
| :--- | :--- | :--- |
| **Transport** | TCP (Port 445) | TCP (Port 2049) |
| **Authentication** | Username, Password, Domain | UID/GID (AUTH_SYS) or Kerberos Keytab |
| **Random I/O** | Native via `go-smb2` | Native via Go NFS client |
| **Performance** | High LAN throughput | Excellent compound RPC latency |
| **Typical Target** | Windows Server, Azure Files, Samba | Linux NFS Exports, Enterprise NAS |

---

## 7. Implementation Plan

1. **Phase 1:** Integrate selected Go NFS client library dependency and wire up `Connect`/`Close` routine in `internal/backend/nfs/`.
2. **Phase 2:** Implement metadata operations: `Stat`, `ReadDir`, `Mkdir`, `Remove`, `Rename`.
3. **Phase 3:** Implement random-access I/O operations (`ReadAt` and `WriteAt` in NFS file handle).
4. **Phase 4:** Add integration tests mocking an NFSv4 exporter using a user-space NFS server in test fixtures.
