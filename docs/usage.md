# Usage Guide: Use Cases & Tradeoffs

RepliStore is designed for specific distributed storage scenarios where data durability and high availability are more critical than raw write performance. This document outlines where RepliStore excels and where its architectural choices may present challenges.

## Suitable Use Cases

### 1. High-Availability Media Libraries
RepliStore is excellent for storing large media collections (videos, photos, music) that need to be accessible from multiple clients.
- **Why:** Near-instant directory listings via metadata caching and automatic read failover if one NAS goes offline.

### 2. Distributed Backup & Archiving
When you need to ensure that backups are physically replicated across multiple independent storage units (e.g., two different NAS brands or locations).
- **Why:** Synchronous file-level replication ensures that once a backup write is acknowledged, it exists on multiple backends.

### 3. Shared Document Management
For small to medium-sized teams sharing office documents, PDFs, and design files.
- **Why:** Provides a unified mount point for disparate SMB shares, simplifying the user experience while maintaining a high level of data redundancy.

### 4. Read-Intensive Workloads
Applications that perform frequent reads and metadata lookups but relatively few writes.
- **Why:** The in-memory metadata cache eliminates the "chattiness" of the SMB protocol for common operations like `ls` or `stat`.

---

## Architectural Tradeoffs

### 1. Write Performance vs. Durability
RepliStore prioritizes **durability**. 
- **The Cost:** Every write is synchronously fanned out to multiple backends. A `replication_factor` of 3 means you use 3x the network bandwidth for every write.
- **Latency:** A write operation is only as fast as the slowest backend in the `write_quorum`.

### 2. Consistency vs. Availability
By using a `write_quorum`, RepliStore allows the system to remain "writable" even if some backends are down.
- **The Tradeoff:** If a write succeeds on a quorum but fails on a specific backend, that backend is removed from the file's replica list. The **Background Repair Manager** eventually restores consistency, but there is a window where the file has fewer replicas than desired.

### 3. Memory Usage vs. Metadata Speed
To provide near-instant metadata access, RepliStore keeps the entire directory tree in memory.
- **The Cost:** For filesystems with millions of files, RepliStore's memory footprint will grow significantly. It is not recommended for environments with extremely high file counts and limited RAM.

### 4. Statelessness vs. I/O Latency
RepliStore does not maintain a local disk-based write-back cache.
- **The Tradeoff:** This simplifies deployment and ensures that the SMB shares are the absolute source of truth, but it means the application is always blocked by network round-trip times to the remote shares.

---

## Not Recommended For:
- **Low-Latency Databases:** High-frequency small random writes (e.g., SQLite, PostgreSQL) will perform poorly due to network latency and the lack of block-level caching.
- **High-Bandwidth Ingestion:** If your network is already saturated, the 2x or 3x write amplification from replication will cause severe congestion.
- **Very Large File Counts:** Environments with tens of millions of files may exceed the memory capacity required for the metadata cache.
