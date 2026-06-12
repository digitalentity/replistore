# Multi-Client Deployment & Distributed Locking

RepliStore supports multi-client clusters through a Peer-to-Peer (P2P) Distributed Lock Manager (DLM). This allows multiple RepliStore instances to share the same SMB backends while maintaining data integrity.

## Cluster Architecture

When configured with `listen_addr`, RepliStore instances form a cluster:
1.  **Discovery:** Nodes discover each other through the shared SMB backends: each node maintains a peer entry at `.replistore/peers/<nodeID>.json` on every backend (heartbeated every ~10s) and polls that directory to learn cluster membership. No multicast or shared L2 network is required — any node that can reach the backends can join.
2.  **Distributed Locking:** High-level operations (Create, Mkdir, Open for write, Rename, Remove) acquire a distributed lock for the affected path.
3.  **Lease Validation:** Individual `Write` and `Sync` operations do not re-acquire the lock; instead, they verify that the previously acquired **Lease** is still valid before proceeding with backend I/O.
4.  **Consensus:** A lock is granted only if a **quorum** (majority) of discovered nodes agree. This prevents "split-brain" scenarios in the event of a network partition.

---

## Configuration

To enable clustering, add the following to each node's `config.yaml`:

```yaml
listen_addr: ":5050"      # Internal lock server port (UDP)
advertise_addr: "192.168.1.50:5050" # Required: host:port peers use to reach this node
cluster_secret: "<16+ char shared secret>"  # Required: same value on all nodes
```

Ensure all nodes can reach the SMB backends (for discovery) and each other's `advertise_addr` (lock messages are exchanged as UDP datagrams authenticated with HMAC in JWT/JWS format, signed with `cluster_secret`).

---

## Benefits of Distributed Locking

### 1. Write Collision Prevention
Multiple nodes can no longer write to the same file simultaneously. The DLM ensures that only one node holds the "Write Lease" for a specific path, providing strict consistency for cross-node operations.

### 2. Atomic Directory Operations & Deletes
Operations like `Rename`, `Mkdir`, and `Remove` (delete) are coordinated across the cluster. This prevents race conditions where two nodes might attempt to modify or delete the same directory structure simultaneously.

### 3. Repair Coordination (Undelete Race Prevention)
The background **Repair Manager** also utilizes the DLM. Before repairing a degraded file, it must acquire a distributed lock. 
- This prevents the "undelete" race: if Node A is deleting a file, Node B's Repair Manager will fail to acquire the lock and will not attempt to "restore" the file replicas that Node A is currently removing.

### 4. Fencing and Node Recovery
Locks are granted as **Leases** with a short Time-To-Live (TTL).
- If a node crashes, its locks naturally expire and are reclaimed by the cluster.
- **Lease validation:** lease validity is checked before each write *and re-checked after the backend writes complete, before acknowledging to the application* — a write that landed under an expired lease is reported as an I/O error. Note the honest limitation: SMB offers no true fencing primitive, so a write already in flight when the lease expires can still reach the backend; the post-write check shrinks that window to milliseconds but cannot eliminate it.

---

## Limitations & Best Practices

### 1. Metadata Eventual Consistency
While *writes* are strictly consistent via the DLM, the in-memory **Metadata Cache** remains eventually consistent.
- **Scenario:** Node A creates a file. Node B will not see this file in its directory listing until its next `cache_refresh_interval` (default 5m) completes.
- **Best Practice:** If you need immediate visibility across nodes, reduce `cache_refresh_interval` or perform an operation that triggers a cache refresh (like accessing the file directly by path).

### 2. Network Latency
The P2P locking adds a small amount of latency to write operations (one round-trip to peers). For high-performance workloads, ensure low-latency connectivity between RepliStore instances.

### 3. Repair Efficiency (Implicit Leader Election)
RepliStore automatically ensures that only one node in the cluster performs background repairs at any given time.
- **Global Repair Lock:** The `RepairManager` must acquire a cluster-wide lock on `.replistore/repair.lock` before starting a scrub.
- This prevents redundant background network traffic and ensures that the cluster coordinates its maintenance tasks without manual configuration.

### 4. Split-Brain Behavior
RepliStore requires a **majority** to acquire a lock.
- If you have 3 nodes and a network partition splits them 1 vs 2, the partition with 2 nodes will continue to operate, while the single node will become read-only (failing to acquire write locks).
- If you have only 2 nodes, both must be up to reach a quorum ($Q=2$).
