# Multi-Client Deployment & Limitations

This document explains the behavior and risks associated with running multiple RepliStore instances (mounts) against the same set of backend SMB shares.

## Current Architecture: Distributed but Independent

RepliStore is designed as a **Single-Writer / Multi-Reader** system at the instance level. While the backends are distributed, the individual RepliStore instances do not communicate with each other.

### Recommended Pattern
The safest way to use RepliStore is to have **one primary read-write mount**. Additional mounts should be treated as **read-only** or used with the understanding that metadata will be eventually consistent.

---

## Known Issues in Multi-Client Scenarios

### 1. Metadata Inconsistency
Since each instance maintains its own in-memory `vfs.Cache`, changes made by one instance are not immediately reflected in others.
- **Symptom:** Files created on Instance A don't appear on Instance B for several minutes.
- **Symptom:** Deleting a file on Instance A leaves a "broken" entry on Instance B that fails when accessed.
- **Resolution:** Changes are only reconciled during the periodic `cache_refresh_interval` scan.

### 2. Write Collisions (No Distributed Locking)
RepliStore does not coordinate writes between instances.
- **Risk:** If two instances write to the same file simultaneously, the file replicas may diverge. Instance A might successfully write to Backend 1, while Instance B successfully writes to Backend 2.
- **Result:** The filesystem enters a "split-brain" state. The next metadata sync will pick the version with the latest `mtime`, causing the other version to be lost.

### 3. Concurrent Repair Conflicts
The `RepairManager` on each instance operates independently.
- **Risk:** Multiple instances may attempt to repair the same degraded file at the same time.
- **Result:** Excessive network overhead and potential performance degradation on the SMB backends. In some cases, if backends handle locks differently, repair operations might fail or collide.

### 4. The "Undelete" Race
- **Scenario:** Instance A deletes a file. Instance B has the file in its cache but hasn't synced yet.
- **Conflict:** Instance B's `RepairManager` runs, sees that the file is "missing" from the backends Instance A just cleared, and copies the file back from a replica that hasn't been deleted yet.
- **Result:** The deleted file is unintentionally restored.

---

## Mitigation Strategies

If you must run multiple instances against the same backends:

1.  **Read-Only Secondary Mounts:** Mount secondary instances with the FUSE `-o ro` (read-only) flag to prevent them from performing writes or repairs.
2.  **Disable Repair on Secondaries:** Set `repair_interval: "0"` in the configuration for all but one "master" instance.
3.  **Short Sync Intervals:** Reduce `cache_refresh_interval` to minimize the window of inconsistency, though this increases SMB metadata overhead.
4.  **Path Partitioning:** Ensure that different instances work in entirely separate directory sub-trees to avoid write and repair collisions.
