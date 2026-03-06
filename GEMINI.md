# RepliStore Agent Instructions

This document provides foundational mandates and technical context for AI agents working on RepliStore. These instructions take precedence over general defaults.

## 1. Architectural Mandates

RepliStore is a distributed system following a strict 4-layer architecture. Maintain this separation of concerns:
1.  **Frontend (FUSE):** `internal/fuse/fs.go` - OS syscall translation.
2.  **Virtual File System (VFS):** `internal/vfs/` - Namespace management and replication logic.
3.  **Cluster (P2P):** `internal/cluster/` - mDNS discovery and quorum-based distributed locking.
4.  **Backend (SMB):** `internal/backend/` - Raw I/O to remote shares.

## 2. Distributed Consistency & Locking

### 2.1. The DLM Principle
RepliStore uses a custom Peer-to-Peer Distributed Lock Manager (DLM).
- **Discovery:** Always use Multicast DNS (mDNS) via `grandcat/zeroconf`. Do not introduce static IP dependencies.
- **Mutual Exclusion:** Use the masterless quorum algorithm ($Q = \lfloor N/2 \rfloor + 1$).
- **Deterministic Ordering:** All lock requests must include a Lamport timestamp for tie-breaking.
- **Lease-Based Fencing:** Locks are leases with TTLs. 
    - **Crucial:** Every `Write` or `Sync` operation in `internal/fuse/fs.go` **must** verify `h.lock.IsValid()` before issuing backend commands.

### 2.2. Operation Safety
- **Metadata Changes:** `Create`, `Mkdir`, `Remove`, and `Rename` must acquire a distributed lock before modification.
- **Background Repairs:** The `RepairManager` must acquire:
    1. The **Global Repair Lock** (`.replistore/repair.lock`) at the start of a scrub cycle.
    2. A **Path-Level Lock** for every individual file being repaired.

## 3. Engineering Standards

- **Minimalism:** Prefer the Go standard library (`net/rpc`, `net/http`, etc.). External dependencies must be justified and reviewed for "bloat."
- **I/O Resilience:** All backend and cluster RPC operations must support `context.Context` with appropriate timeouts.
- **Logging:** Use `sirupsen/logrus`. Component-specific loggers should include `component` and `path` or `node_id` fields.
- **Concurrency:** Use `golang.org/x/sync/errgroup` for parallel fan-out operations (e.g., writing to multiple replicas).

## 4. Validation Workflow

After any code modification:
1.  **Build Check:** Run `go build ./cmd/replistore/...` to ensure compilation.
2.  **Test Suite:** Run `go test ./...`. RepliStore relies heavily on mock-based testing in `internal/test/` to verify distributed logic without actual NAS hardware.
3.  **Documentation:** If a feature adds a configuration field or changes a flow, update the corresponding file in `docs/`.
