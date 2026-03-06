# Implementation Roadmap

The primary decentralized clustering infrastructure is now complete. Future improvements will focus on further resilience and performance optimizations.

## ✅ Completed: P2P Distributed Lock Manager
- [x] mDNS Discovery Integration (`internal/cluster/discovery.go`)
- [x] Quorum-based RPC & Lock Logic (`internal/cluster/rpc.go`)
- [x] Lamport Clocks for deterministic tie-breaking
- [x] VFS Integration & Lease-based Fencing (`internal/vfs/lock.go` & `internal/fuse/fs.go`)

## 🚀 Future Roadmap

### Phase 4: Cluster-Aware Background Tasks
- Implement Cluster-wide Leader Election.
- Designate a single node to run the `RepairManager` to avoid redundant traffic.
- Coordinate background metadata syncs across the cluster.

### Phase 5: Real-time Metadata Updates
- Implement SMB `CHANGE_NOTIFY` to invalidate cache nodes across the cluster without full rescans.
- Use the P2P RPC layer to broadcast surgical cache updates.
