# Implementation Plan: Peer-to-Peer (P2P) Distributed Lock Manager

This plan outlines a truly decentralized Distributed Lock Manager (DLM) for RepliStore. Instead of relying on the SMB filesystem for atomic locking (which varies by NAS implementation) or requiring external infrastructure like Redis, RepliStore instances will communicate directly with each other to coordinate locks.

## 1. Architecture: The P2P Approach

### 1.1. Discovery via SMB (The "Rendezvous")
To form a P2P cluster without manual IP configuration or external DNS, we use the SMB share itself purely for peer discovery.
- **Mechanism:** On startup, each RepliStore instance generates a unique UUID and starts an RPC server on a random (or configured) available port.
- **Registration:** It writes a small JSON file to `.replistore/peers/<UUID>.json` containing its IP address, RPC port, and a timestamp.
- **Heartbeat:** Instances periodically update their timestamp in this file.
- **Peer List:** Instances periodically read the `.replistore/peers/` directory to discover other active nodes in the cluster. Stale peers (no heartbeat in >30s) are ignored.

### 1.2. Communication Protocol
- Instances communicate directly via standard Go `net/rpc` or gRPC.
- **RPC Methods:**
  - `RequestLock(Path, RequesterID)`
  - `ReleaseLock(Path, RequesterID)`

### 1.3. The Locking Algorithm (Quorum-Based Mutual Exclusion)
We will implement a simplified, masterless quorum algorithm (similar to Maekawa's algorithm or Redlock, but applied to P2P nodes).

1. **Calculate Quorum:** Let $N$ be the number of currently active peers discovered. The quorum is $Q = \lfloor N/2 \rfloor + 1$.
2. **Request Phase:** The requesting instance sends a `RequestLock` RPC to all known peers (including itself).
3. **Peer Logic:**
   - If a peer is not holding the lock for `Path` and hasn't granted it to anyone else recently, it replies `OK`.
   - If it holds the lock or granted it to another pending request, it replies `REJECT`.
4. **Acquisition:** If the requester receives `OK` from $Q$ peers within a timeout (e.g., 500ms), it owns the lock.
5. **Rollback:** If it receives fewer than $Q$ `OK`s, it sends `ReleaseLock` to the peers that granted it and returns `ErrLocked`.

## 2. Existing Libraries vs. Custom Implementation

### Option A: Existing Libraries
There are several Go libraries for P2P locking, but they often come with tradeoffs:
- **`hashicorp/memberlist` / `serf`:** Excellent for peer discovery and gossip, but do not provide distributed locking out-of-the-box. We would still have to write the lock logic over their custom messages.
- **`lni/dragonboat` (Embedded Raft):** Highly robust, but Raft is a consensus algorithm that requires a stable membership list and elects a strong leader. It is relatively heavy for a simple path-locking mechanism and handles dynamic membership (nodes coming and going randomly) poorly compared to pure P2P.
- **Pure P2P Mutual Exclusion (e.g., Ricart-Agrawala implementations):** Most open-source Go implementations of these academic algorithms are unmaintained or lack robust failure recovery.

### Option B: Custom Implementation (Recommended)
Given RepliStore's unique environment (we already have a shared storage medium for discovery), a custom implementation is the most efficient path.
- **Why?** We bypass the hardest part of P2P (discovery and network partitions) by using the SMB share as the definitive "membership registry." If an instance loses connection to the SMB share, it implicitly drops out of the cluster.
- **Complexity:** Writing a simple RPC server and a quorum-gathering loop in Go is straightforward (using `errgroup` and channels) and avoids importing heavy frameworks.

## 3. Implementation Steps (Custom Approach)

### Phase 1: Peer Discovery
1. Create `internal/cluster/discovery.go`.
2. Implement logic to write/heartbeat the instance's RPC address to `.replistore/peers/`.
3. Implement a watcher that maintains a list of active `Peer` addresses.

### Phase 2: RPC Server & Lock State
1. Create `internal/cluster/rpc.go`.
2. Maintain a local `sync.Map` of locks currently granted by this node.
3. Implement `RequestLock` (grant if available) and `ReleaseLock` (clear from map).

### Phase 3: Lock Manager & Integration
1. Create `internal/vfs/lock.go` that implements the quorum gathering logic (send RPCs, count `OK`s).
2. Integrate `vfs.LockManager` into the FUSE layer (`Create`, `Write`, `Rename`, etc.) just as previously planned.

## 4. Resilience & Edge Cases
- **Node Crash:** If Node A crashes while holding a lock, the other nodes will eventually detect its absence via the SMB peer registry (missing heartbeats). The local nodes will clear any locks granted to Node A, allowing new quorums to form.
- **Split Brain:** The strict majority requirement ($> N/2$) ensures that two different instances cannot acquire the lock simultaneously, even if the network briefly partitions.
