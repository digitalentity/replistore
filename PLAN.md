# Implementation Plan: P2P Distributed Lock Manager (mDNS Discovery)

This plan outlines a decentralized Distributed Lock Manager (DLM) for RepliStore. Instead of relying on a shared filesystem for discovery or locking, RepliStore instances will use Multicast DNS (mDNS) to find each other and coordinate locks via a quorum-based P2P protocol.

## 1. Architecture: The P2P Approach

### 1.1. Discovery via mDNS (Zeroconf/Bonjour)
To achieve zero-configuration discovery without external dependencies, we use mDNS.
- **Service Registration:** On startup, each node registers a service (e.g., `_replistore._tcp`) with its Unique Node ID (UUID) and the address of its internal RPC server.
- **Continuous Discovery:** Nodes browse for `_replistore._tcp` services to maintain an up-to-date map of active peers in the local network.
- **Liveness:** mDNS naturally handles service expiration. If a node shuts down or its network fails, its service record will eventually time out and be removed from peers' views.
- **Library Recommendation:** Use `grandcat/zeroconf` for its clean API and robust support for concurrent browsing and registration.

### 1.2. Communication Protocol
Instances communicate directly via standard Go `net/rpc` or gRPC.
- **RPC Methods:**
  - `RequestLock(Path, NodeID, LamportTime)` -> `(Status, FencingToken)`
  - `RenewLock(Path, NodeID, FencingToken)` -> `Status`
  - `ReleaseLock(Path, NodeID, FencingToken)` -> `Status`

### 1.3. The Locking Algorithm (Quorum with Leases & Lamport Clocks)
To ensure robustness against split-brain and simultaneous requests, we use a masterless quorum algorithm.

1. **Logical Clocks (Lamport Timestamps):** Every request includes a Lamport timestamp to provide global deterministic ordering. In case of simultaneous requests for the same path, the lower timestamp (older request) wins. Ties are broken by Node UUID.
2. **Dynamic Quorum Calculation:** Let $N$ be the number of active peers discovered via mDNS. Quorum $Q = \lfloor N/2 \rfloor + 1$. The membership view is snapshotted at the start of a lock request.
3. **Lease-Based Grants:** Locks are granted as **Leases** with a short TTL (e.g., 5-10s). The holder must periodically send `RenewLock` RPCs to the quorum.
4. **Fencing Tokens:** Every successful acquisition returns a `FencingToken`. The VFS layer must include/check this token before any backend I/O to ensure it still holds a valid lease.
5. **Acquisition Flow:** 
   - Broadcast `RequestLock` to all $N$ peers.
   - If $\ge Q$ peers return `OK` within a strict timeout, the lock is acquired.
   - Otherwise, broadcast `ReleaseLock` to anyone who granted it and retry after a random backoff.

## 2. Implementation Steps

### Phase 1: mDNS Discovery Integration
1. Create `internal/cluster/discovery.go`.
2. Implement service registration and a background browser using `grandcat/zeroconf`.
3. Expose a `GetPeers()` method that returns a stable list of active RPC addresses.

### Phase 2: RPC & Lock Logic
1. Create `internal/cluster/rpc.go`.
2. Implement the `LockManager` with an internal state machine handling grants and TTLs.
3. Implement the Lamport clock logic to synchronize logical time across the cluster.

### Phase 3: Distributed Lock Manager (DLM)
1. Create `internal/vfs/lock.go`.
2. Implement the `DistributedLock` struct that orchestrates the quorum request, renewal loop, and fencing.
3. Integrate into `RepliFS` (FUSE layer) to protect `Create`, `Write`, `Rename`, and `Remove` operations.

## 3. Resilience & Edge Cases
- **Node Crash:** If a node crashes, its lease expires on the remaining peers. The mDNS browser will also eventually drop the node from the active list.
- **Network Partition:** A minority partition cannot reach $Q$ and will fail to acquire or renew locks, preventing data corruption.
- **Clock Skew:** Lamport clocks ensure logical ordering independent of system wall-clock synchronization. Leases use monotonic elapsed time (`time.Since`).
- **Simultaneous Requests:** deterministic tie-breaking via Lamport time + UUID prevents livelocks.
