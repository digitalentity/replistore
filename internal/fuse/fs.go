// Package fuse implements the FUSE filesystem layer translating OS syscalls to VFS operations.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/observability"
	"github.com/digitalentity/replistore/internal/vfs"
	"golang.org/x/sync/errgroup"
	"log/slog"
)

const (
	replicaRemovalRetryDelay = 5 * time.Second
	replicaRemovalTimeout    = 10 * time.Second
)

type RepliFS struct {
	Cache             *vfs.Cache
	Backends          map[string]backend.Backend
	ReplicationFactor int
	WriteQuorum       int
	Selector          vfs.BackendSelector
	LockManager       *cluster.LockManager
	Discovery         *cluster.Discovery
	HealthMonitor     *backend.HealthMonitor

	// NodeID identifies this node as the writer in version sidecars
	// (diagnostics only). Set from main; an empty string is fine when
	// clustering is off — sidecar correctness depends only on Gen.
	NodeID string

	WriteLeaseBuffer time.Duration
	CacheTTL         time.Duration

	AttrValid time.Duration
	UID       *uint32
	GID       *uint32

	// pathLocks serializes same-path mutations within this process. The
	// distributed lock manager (when enabled) only arbitrates between nodes
	// and rejects a second local acquisition instead of queueing it; when
	// clustering is disabled there is no serialization at all. Ordering
	// invariant: the local path lock is ALWAYS acquired before the
	// distributed lock for the same path. Create and Open release the local
	// lock when they return, while the distributed lock lives on with the
	// file handle; holding the local path lock for the handle's lifetime
	// would block background repair forever.
	pathLocks pathLocks

	// handles tracks open write handles per cache node so node-level Fsync
	// can sync the backend files those handles actually hold.
	handles handleRegistry
}

type fuseContextKey string

const fuseLoggerKey fuseContextKey = "fuse_logger"

func (f *RepliFS) logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(fuseLoggerKey).(*slog.Logger); ok {
		return l
	}

	return observability.Logger(ctx).With(slog.String("component", "fuse"), slog.String("node_id", f.NodeID))
}

func (f *RepliFS) pathLogger(ctx context.Context, path string) *slog.Logger {
	return f.logger(ctx).With(slog.String("path", path))
}

func trace(ctx context.Context) context.Context {
	if id := observability.CorrelationID(ctx); id != "" {
		return ctx
	}
	id := observability.GenerateCorrelationID()

	return observability.WithCorrelationID(ctx, id)
}

// withRequestor tags ctx with the issuing process identity from a FUSE request
// header, so logs name the actual initiator instead of only an opaque
// per-operation correlation_id.
func withRequestor(ctx context.Context, hdr *fuse.Header) context.Context {
	if hdr == nil {
		return ctx
	}

	return observability.WithRequestor(ctx, observability.Requestor{
		PID: hdr.Pid,
		UID: hdr.Uid,
		GID: hdr.Gid,
	})
}

// initCtx initializes the request context with a correlation ID trace and, if available,
// requestor information (PID, UID, GID) and caches a FUSE-specific logger in the context.
func initCtx(ctx context.Context, f *RepliFS, hdr *fuse.Header) context.Context {
	ctx = trace(ctx)
	if hdr != nil {
		ctx = withRequestor(ctx, hdr)
	}
	if f != nil {
		logger := observability.Logger(ctx).With(slog.String("component", "fuse"), slog.String("node_id", f.NodeID))
		ctx = context.WithValue(ctx, fuseLoggerKey, logger)
	}

	return ctx
}

//nolint:ireturn // fs.Node is an interface required by bazil.org/fuse/fs
func (f *RepliFS) Root() (fs.Node, error) {
	return &Dir{fs: f, node: f.Cache.Root}, nil
}

func (f *RepliFS) acquireLock(ctx context.Context, path string) (*vfs.DistributedLock, error) {
	if f.LockManager == nil || f.Discovery == nil {
		f.pathLogger(ctx, path).Debug("Distributed locking is disabled or not configured")

		return nil, nil //nolint:nilnil // Locking disabled or not configured
	}
	lock := vfs.NewDistributedLock(path, f.LockManager, f.Discovery)
	if err := lock.Acquire(ctx); err != nil {
		f.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to acquire distributed lock: %v", err))

		return nil, err
	}

	return lock, nil
}

// removeStaleReplica asynchronously deletes a stale replica of path from the
// named backend after that backend has been dropped from a node's metadata
// (e.g. a failed write or rename). It makes one immediate attempt and one
// retry after 5 seconds, then gives up with a warning. An already-absent
// replica counts as success.
//
// This is best-effort only: the deletion can fail (the backend is likely down,
// since an operation on it just failed), in which case the stale replica
// survives and may resurface via background sync. A tombstone mechanism
// (REVIEW.md C6) is the durable fix for that.
//
//nolint:contextcheck // runs asynchronously in the background
func (f *RepliFS) removeStaleReplica(path string, backendName string) {
	ctx := context.Background()
	b, ok := f.Backends[backendName]
	if !ok {
		return
	}
	go func() {
		var lastErr error
		for attempt := range 2 {
			if attempt > 0 {
				time.Sleep(replicaRemovalRetryDelay)
			}
			ctx, cancel := context.WithTimeout(context.Background(), replicaRemovalTimeout)
			err := b.Remove(ctx, path)
			cancel()
			if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
				return
			}
			lastErr = err
		}
		f.pathLogger(ctx, path).Warn(fmt.Sprintf("Failed to remove stale replica from backend %s: %v", backendName, lastErr))
	}()
}

// writeSidecars writes sc to path's sidecar on the named backends in
// parallel, best-effort. Best-effort rationale: a replica whose sidecar write
// failed reports generation 0 and therefore loses reconciliation against the
// replicas carrying the new generation, after which it gets repaired — no
// data loss.
func (f *RepliFS) writeSidecars(ctx context.Context, path string, sc vfs.Sidecar, backendNames []string) {
	var wg sync.WaitGroup
	for _, bName := range backendNames {
		b, ok := f.Backends[bName]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(bName string, b backend.Backend) {
			defer wg.Done()
			if err := vfs.WriteSidecar(ctx, b, path, sc); err != nil {
				f.pathLogger(ctx, path).Warn(fmt.Sprintf("Failed to write sidecar (gen %d) on backend %s: %v", sc.DataGen, bName, err))
			}
		}(bName, b)
	}
	wg.Wait()
}

// writeTombstones writes sc as path's deletion tombstone on the named
// backends in parallel and returns how many writes succeeded. Unlike
// writeSidecars it reports successes because Dir.Remove requires a write
// quorum of tombstones before destroying data: a deletion that is not durably
// recorded could resurrect from an offline backend (REVIEW.md C6). Failures
// are logged per backend.
func (f *RepliFS) writeTombstones(ctx context.Context, path string, sc vfs.Sidecar, backendNames []string) int {
	var mu sync.Mutex
	var successes int
	var wg sync.WaitGroup
	for _, bName := range backendNames {
		b, ok := f.Backends[bName]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(bName string, b backend.Backend) {
			defer wg.Done()
			if err := vfs.WriteTombstone(ctx, b, path, sc); err != nil {
				f.pathLogger(ctx, path).Warn(fmt.Sprintf("Failed to write tombstone (gen %d) on backend %s: %v", sc.DataGen, bName, err))

				return
			}
			mu.Lock()
			successes++
			mu.Unlock()
		}(bName, b)
	}
	wg.Wait()

	return successes
}

// maxTombstoneGen returns the highest tombstone generation recorded for path
// on any configured backend, reading all backends in parallel. A missing
// tombstone counts as generation 0. Any other read error also degrades to 0
// (with a debug log): an unreachable backend may hide a higher tombstone, and
// a lineage started at or below it is eventually reconciled by repair's
// obsolete-tombstone GC (see RepairManager.enforceTombstone).
//
// Callers use this to start new lineages ABOVE any recorded deletion: a
// tombstone at generation T dooms every replica at generation <= T, so a file
// created or renamed onto a tombstoned path must start at maxTombstoneGen+1.
func (f *RepliFS) maxTombstoneGen(ctx context.Context, path string) int64 {
	var mu sync.Mutex
	var maxGen int64
	var wg sync.WaitGroup
	for bName, b := range f.Backends {
		wg.Add(1)
		go func(bName string, b backend.Backend) {
			defer wg.Done()
			sc, err := vfs.ReadMeta(ctx, b, path)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
					f.pathLogger(ctx, path).Debug(fmt.Sprintf("Metadata read on %s failed: %v", bName, err))
				}

				return
			}
			if !sc.Deleted {
				// A live document is not a tombstone; it dooms nothing.
				return
			}
			mu.Lock()
			if sc.DataGen > maxGen {
				maxGen = sc.DataGen
			}
			mu.Unlock()
		}(bName, b)
	}
	wg.Wait()

	return maxGen
}

// allBackendNames returns the names of every configured backend.
func (f *RepliFS) allBackendNames() []string {
	res := make([]string, 0, len(f.Backends))
	for name := range f.Backends {
		res = append(res, name)
	}

	return res
}

func (f *RepliFS) getBackendList() []backend.Backend {
	res := make([]backend.Backend, 0, len(f.Backends))
	for _, b := range f.Backends {
		res = append(res, b)
	}

	return res
}

type Dir struct {
	fs   *RepliFS
	node *vfs.Node
}

// Link counts reported through Attr: directories carry the conventional 2
// (self + "."), regular files a single link.
const (
	dirNlink  = 2
	fileNlink = 1
)

// fillAttr populates a from node (taken under the node's read lock), applying
// the configured attribute validity window and uid/gid overrides. nlink is
// dirNlink for directories and fileNlink for regular files.
func (f *RepliFS) fillAttr(a *fuse.Attr, node *vfs.Node, nlink uint32) {
	node.Mu.RLock()
	defer node.Mu.RUnlock()

	a.Mode = node.Meta.Mode
	if node.Meta.Size < 0 {
		a.Size = 0
	} else {
		a.Size = uint64(node.Meta.Size) //nolint:gosec // checked non-negative
	}
	a.Mtime = node.Meta.ModTime
	a.Valid = f.AttrValid
	a.Nlink = nlink
	if f.UID != nil {
		a.Uid = *f.UID
	}
	if f.GID != nil {
		a.Gid = *f.GID
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	start := time.Now()
	defer func() {
		observability.RecordFSOp("attr", start)
		err = TranslateError(err)
	}()

	d.fs.fillAttr(a, d.node, dirNlink)

	return nil
}

//nolint:ireturn,nonamedreturns
func (d *Dir) Lookup(ctx context.Context, name string) (node fs.Node, err error) {
	ctx = initCtx(ctx, d.fs, nil)
	start := time.Now()
	defer func() {
		observability.RecordFSOp("lookup", start)
		err = TranslateError(err)
	}()

	d.node.Mu.RLock()
	child, ok := d.node.Children[name]
	path := d.node.Meta.Path
	parentFullyIndexed := d.node.FullyIndexed
	parentStale := d.fs.CacheTTL > 0 && time.Since(d.node.LastUpdated) > d.fs.CacheTTL
	d.node.Mu.RUnlock()

	childPath := name
	if path != "" {
		childPath = path + "/" + name
	}
	if vfs.IsReservedPath(childPath) {
		// Cluster-internal state is invisible through the mount.
		return nil, syscall.ENOENT
	}

	if !ok && parentFullyIndexed && !parentStale {
		// Parent directory is fully indexed and up-to-date. If the child is not in the cache, it definitely does not exist.
		return nil, syscall.ENOENT
	}

	stale := false
	if ok && d.fs.CacheTTL > 0 {
		child.Mu.RLock()
		stale = time.Since(child.LastUpdated) > d.fs.CacheTTL
		child.Mu.RUnlock()
	}

	if !ok || stale {
		node, err := d.fetchChild(ctx, childPath, child, ok)
		if err != nil {
			return nil, err
		}
		child = node
	}

	if child.Meta.IsDir {
		return &Dir{fs: d.fs, node: child}, nil
	}

	return &File{fs: d.fs, node: child}, nil
}

// fetchChild lazily fetches or revalidates a child's metadata. cached is the
// existing cache node (may be nil) and ok reports whether it was cached. On a
// transient backend failure it falls back to the stale cached node rather than
// reporting a misleading ENOENT.
func (d *Dir) fetchChild(ctx context.Context, childPath string, cached *vfs.Node, ok bool) (*vfs.Node, error) {
	if ok {
		d.fs.pathLogger(ctx, childPath).Debug("Re-validating stale metadata")
	} else {
		d.fs.pathLogger(ctx, childPath).Debug("Lazy fetching metadata")
	}

	node, err := d.fs.Cache.FetchEntry(ctx, childPath, d.fs.getBackendList())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.fs.pathLogger(ctx, childPath).Debug("Lazy fetch/Revalidate: child not found on any backend")
			if ok {
				d.fs.Cache.Evict(childPath)
			}

			return nil, syscall.ENOENT
		}
		d.fs.pathLogger(ctx, childPath).Error(fmt.Sprintf("Lazy fetch/Revalidate failed: %v", err))
		if !ok {
			// ErrUnavailable or any other error: we don't know whether the
			// entry exists, so don't lie with ENOENT.
			return nil, syscall.EIO
		}
		d.fs.pathLogger(ctx, childPath).Warn("Using stale cached entry for " + childPath)
		node = cached
	}
	d.fs.pathLogger(ctx, childPath).Debug("Lazy fetch/Revalidate success")

	return node, nil
}

// mkdirOnBackend creates path on a single backend. created reports whether this
// call created the directory (a rollback candidate); satisfied reports whether
// the directory is present afterward (counted toward quorum). A backend failure
// is logged and yields satisfied=false rather than failing the whole operation.
func (d *Dir) mkdirOnBackend(ctx context.Context, name string, b backend.Backend, path, parentPath string, mode os.FileMode) (bool, bool) {
	if parentPath != "" {
		if err := b.MkdirAll(ctx, parentPath, 0755); err != nil {
			observability.Event(ctx, "mkdirall parent failed", slog.String("parent", parentPath), slog.String("backend", name), slog.Any("error", err))
		}
	}

	err := b.Mkdir(ctx, path, mode)
	if err == nil {
		return true, true
	}

	if !os.IsExist(err) && !errors.Is(err, os.ErrExist) {
		observability.Event(ctx, "mkdir failed on backend", slog.String("backend", name), slog.Any("error", err))

		return false, false
	}

	// The directory may already exist on this backend (e.g. created by another
	// cluster node). Verify it really is a directory.
	info, statErr := b.Stat(ctx, path)
	if statErr != nil {
		observability.Event(ctx, "stat existing path failed on backend", slog.String("backend", name), slog.Any("error", statErr))

		return false, false
	}
	if !info.IsDir {
		observability.Event(ctx, "path exists but is not a directory on backend", slog.String("backend", name))

		return false, false
	}

	return false, true
}

//nolint:nonamedreturns
func (d *Dir) ReadDirAll(ctx context.Context) (entries []fuse.Dirent, err error) {
	ctx = initCtx(ctx, d.fs, nil)
	start := time.Now()
	defer func() {
		observability.RecordFSOp("read_dir_all", start)
		err = TranslateError(err)
	}()

	d.node.Mu.RLock()
	fullyIndexed := d.node.FullyIndexed
	stale := d.fs.CacheTTL > 0 && time.Since(d.node.LastUpdated) > d.fs.CacheTTL
	path := d.node.Meta.Path
	d.node.Mu.RUnlock()

	var fetchErr error
	if !fullyIndexed || stale {
		if !fullyIndexed {
			d.fs.pathLogger(ctx, path).Debug("Lazy fetching directory listing")
		} else {
			d.fs.pathLogger(ctx, path).Debug("Re-validating stale directory listing")
		}
		if fetchErr = d.fs.Cache.FetchDir(ctx, path, d.fs.getBackendList()); fetchErr != nil {
			d.fs.pathLogger(ctx, path).Error(fmt.Sprintf("FetchDir failed: %v", fetchErr))
			// Return what we have anyway (unless we have nothing, see below)
		}
	}

	d.node.Mu.RLock()
	defer d.node.Mu.RUnlock()

	// If no backend could give an authoritative answer and we have nothing
	// cached for a never-indexed directory, an empty listing would be a lie.
	if errors.Is(fetchErr, vfs.ErrUnavailable) && len(d.node.Children) == 0 && !d.node.FullyIndexed {
		return nil, syscall.EIO
	}

	var res []fuse.Dirent
	for name, child := range d.node.Children {
		de := fuse.Dirent{
			Name: name,
		}
		if child.Meta.IsDir {
			de.Type = fuse.DT_Dir
		} else {
			de.Type = fuse.DT_File
		}
		res = append(res, de)
	}

	return res, nil
}

//nolint:ireturn,nonamedreturns
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (node fs.Node, handle fs.Handle, err error) {
	ctx = initCtx(ctx, d.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("create", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()

	d.node.Mu.Lock()
	if _, ok := d.node.Children[req.Name]; ok {
		d.node.Mu.Unlock()

		return nil, nil, syscall.EEXIST
	}

	// Snapshot needed data while holding lock
	parentPath := d.node.Meta.Path
	d.node.Mu.Unlock()

	path := parentPath + "/" + req.Name
	if parentPath == "" {
		path = req.Name
	}
	if vfs.IsReservedPath(path) {
		// Cluster-internal state must not be touched through the mount.
		return nil, nil, syscall.EACCES
	}

	// Local serialization of same-path mutations. Held until the backend
	// opens complete and the cache is updated, then released at return: the
	// handle's writes are protected by its own state and the distributed
	// lock, and holding the path lock for the handle's lifetime would block
	// repair forever.
	unlock := d.fs.pathLocks.lock(path)
	defer unlock()

	// Distributed Locking (always after the local path lock, see RepliFS)
	lock, err := d.fs.acquireLock(ctx, path)
	if err != nil {
		return nil, nil, err
	}

	allBackendNames := make([]string, 0, len(d.fs.Backends))
	for name := range d.fs.Backends {
		allBackendNames = append(allBackendNames, name)
	}

	selectedBackends := d.fs.Selector.SelectForWrite(d.fs.ReplicationFactor, allBackendNames)
	d.fs.pathLogger(ctx, path).Info(fmt.Sprintf("Creating file with selected backends: %v (quorum: %d)", selectedBackends, d.fs.WriteQuorum))

	if len(selectedBackends) == 0 {
		if lock != nil {
			lock.Release()
		}
		d.fs.pathLogger(ctx, path).Error("Failed to create file: no healthy backends selected")

		return nil, nil, syscall.EIO
	}

	h := &FileHandle{
		backends: make(map[string]backend.File),
		lock:     lock,
		readOnly: false,
	}

	// Perform I/O outside of VFS lock to prevent deadlock
	observability.Event(ctx, "create started", slog.String("path", path), slog.Int("backends", len(selectedBackends)), slog.Int("quorum", d.fs.WriteQuorum))
	var mu sync.Mutex
	var successfulBackends []string
	var existsCount int
	g, gCtx := errgroup.WithContext(ctx)

	for _, bName := range selectedBackends {
		g.Go(func() error {
			b := d.fs.Backends[bName]
			if parentPath != "" {
				if err := b.MkdirAll(gCtx, parentPath, 0755); err != nil {
					observability.Event(ctx, "mkdirall parent failed", slog.String("parent", parentPath), slog.String("backend", bName), slog.Any("error", err))
				}
			}
			sf, err := b.OpenFile(gCtx, path, os.O_CREATE|os.O_EXCL|os.O_RDWR, req.Mode)
			if err != nil {
				if os.IsExist(err) || errors.Is(err, os.ErrExist) {
					// The file already exists on this backend (e.g. created by
					// another cluster node and not yet in our cache).
					observability.Event(ctx, "create conflict on backend (already exists)", slog.String("backend", bName))
					mu.Lock()
					existsCount++
					mu.Unlock()

					return nil
				}
				observability.Event(ctx, "create failed on backend", slog.String("backend", bName), slog.Any("error", err))

				return nil // Don't fail the whole operation yet
			}
			observability.Event(ctx, "create ok on backend", slog.String("backend", bName))
			mu.Lock()
			h.backends[bName] = sf
			successfulBackends = append(successfulBackends, bName)
			mu.Unlock()

			return nil
		})
	}
	_ = g.Wait() // Ignore errors as we count successes

	if len(successfulBackends) < d.fs.WriteQuorum {
		_ = h.Release(ctx, nil)
		d.removeCreated(ctx, path, successfulBackends)
		if existsCount > 0 {
			// The file already exists on at least one backend. Merge the
			// discovered file into the cache so subsequent lookups see it.
			if _, ferr := d.fs.Cache.FetchEntry(ctx, path, d.fs.getBackendList()); ferr != nil {
				d.fs.pathLogger(ctx, path).Warn(fmt.Sprintf("FetchEntry after create conflict failed: %v", ferr))
			}
			d.fs.pathLogger(ctx, path).Warn("Create conflict: file already exists on backend(s)")

			return nil, nil, syscall.EEXIST
		}
		d.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to create file: write quorum not met (%d/%d)", len(successfulBackends), d.fs.WriteQuorum))

		return nil, nil, fmt.Errorf("could not reach write quorum: %d/%d", len(successfulBackends), d.fs.WriteQuorum)
	}

	// Stamp the new lineage's first generation on every replica this create
	// produced (done before re-acquiring the VFS lock — sidecar writes are
	// backend I/O). The lineage must start ABOVE any tombstone recorded at
	// this path: a tombstone at generation T dooms every replica at gen <= T
	// (dropZombies / enforceTombstone), so a hardcoded gen 1 would be
	// destroyed by the next sync or repair pass. If the conflict check below
	// loses the race, the orphaned sidecars are harmless and get superseded by
	// the winner's writes or by tombstones.
	newGen := d.fs.maxTombstoneGen(ctx, path) + 1
	d.fs.writeSidecars(ctx, path, vfs.Sidecar{DataGen: newGen, Writer: d.fs.NodeID}, successfulBackends)

	// Re-acquire lock to update VFS
	d.node.Mu.Lock()
	defer d.node.Mu.Unlock()

	// Check if someone else created it while we were doing I/O
	if winner, ok := d.node.Children[req.Name]; ok {
		// Conflict! Clean up the replicas this call created, but only on
		// backends the winning entry does not use: another local create may
		// have won the race and have open handles on the same backends.
		winner.Mu.RLock()
		winnerBackends := make(map[string]bool, len(winner.Meta.Backends))
		for _, bName := range winner.Meta.Backends {
			winnerBackends[bName] = true
		}
		winner.Mu.RUnlock()

		var toRemove []string
		for _, bName := range successfulBackends {
			if !winnerBackends[bName] {
				toRemove = append(toRemove, bName)
			}
		}
		_ = h.Release(ctx, nil)
		d.removeCreated(ctx, path, toRemove)
		d.fs.pathLogger(ctx, path).Warn("Create conflict: another operation won the race, cleaning up local orphans")

		return nil, nil, syscall.EEXIST
	}

	meta := vfs.Metadata{
		Name:     req.Name,
		Path:     path,
		Mode:     req.Mode,
		Backends: successfulBackends,
		DataGen:  newGen,
	}

	child := &vfs.Node{
		Meta:     meta,
		Children: make(map[string]*vfs.Node),
	}
	d.node.Children[req.Name] = child
	h.file = &File{fs: d.fs, node: child}
	atomic.AddInt32(&child.OpenHandles, 1)
	d.fs.handles.register(child, h)

	d.fs.pathLogger(ctx, path).Info(fmt.Sprintf("Successfully created file with generation %d", newGen))

	return h.file, h, nil
}

// removeCreated removes replicas of path that a create call produced on the
// given backends. Used to roll back partially completed creates.
func (d *Dir) removeCreated(ctx context.Context, path string, backends []string) {
	for _, bName := range backends {
		b := d.fs.Backends[bName]
		if b == nil {
			continue
		}
		if err := b.Remove(ctx, path); err != nil {
			d.fs.pathLogger(ctx, path).Warn(fmt.Sprintf("Failed to remove partially created file on backend %s: %v", bName, err))
		}
	}
}

//nolint:ireturn,nonamedreturns
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (node fs.Node, err error) {
	ctx = initCtx(ctx, d.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("mkdir", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()

	d.node.Mu.Lock()
	if _, ok := d.node.Children[req.Name]; ok {
		d.node.Mu.Unlock()

		return nil, syscall.EEXIST
	}
	parentPath := d.node.Meta.Path
	d.node.Mu.Unlock()

	path := parentPath + "/" + req.Name
	if parentPath == "" {
		path = req.Name
	}
	if vfs.IsReservedPath(path) {
		// Cluster-internal state must not be touched through the mount.
		return nil, syscall.EACCES
	}

	// Local serialization of same-path mutations.
	unlock := d.fs.pathLocks.lock(path)
	defer unlock()

	// Distributed Locking (always after the local path lock, see RepliFS)
	lock, err := d.fs.acquireLock(ctx, path)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		defer lock.Release()
	}

	// Directories are created on ALL backends to maintain tree structure
	var mu sync.Mutex
	var createdOn []string   // directories this call actually created (rollback candidates)
	var satisfiedOn []string // backends where the directory is present (counted toward quorum)
	g, gCtx := errgroup.WithContext(ctx)

	observability.Event(ctx, "mkdir started", slog.String("path", path), slog.Int("backends", len(d.fs.Backends)), slog.Int("quorum", d.fs.WriteQuorum))
	for name, b := range d.fs.Backends {
		g.Go(func() error {
			created, satisfied := d.mkdirOnBackend(gCtx, name, b, path, parentPath, req.Mode)
			if !satisfied {
				return nil
			}
			mu.Lock()
			if created {
				createdOn = append(createdOn, name)
			}
			satisfiedOn = append(satisfiedOn, name)
			mu.Unlock()

			return nil
		})
	}
	_ = g.Wait()

	if len(satisfiedOn) < d.fs.WriteQuorum {
		// Rollback only directories this call created; never remove pre-existing ones.
		for _, bName := range createdOn {
			_ = d.fs.Backends[bName].Remove(ctx, path)
		}
		d.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to create directory: write quorum not met (%d/%d)", len(satisfiedOn), d.fs.WriteQuorum))

		return nil, fmt.Errorf("could not reach write quorum for mkdir: %d/%d", len(satisfiedOn), d.fs.WriteQuorum)
	}

	newGen := d.fs.maxTombstoneGen(ctx, path) + 1
	d.fs.writeSidecars(ctx, path, vfs.Sidecar{DataGen: newGen, Writer: d.fs.NodeID}, satisfiedOn)

	d.node.Mu.Lock()
	defer d.node.Mu.Unlock()

	// Check conflict
	if _, ok := d.node.Children[req.Name]; ok {
		d.fs.pathLogger(ctx, path).Warn("Mkdir conflict: directory already exists in cache")

		return nil, syscall.EEXIST
	}

	meta := vfs.Metadata{
		Name:     req.Name,
		Path:     path,
		Mode:     req.Mode | os.ModeDir,
		IsDir:    true,
		Backends: satisfiedOn,
		DataGen:  newGen,
	}
	child := &vfs.Node{
		Meta:         meta,
		Children:     make(map[string]*vfs.Node),
		FullyIndexed: true,
		LastUpdated:  time.Now(),
	}
	d.node.Children[req.Name] = child

	return &Dir{fs: d.fs, node: child}, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	ctx = initCtx(ctx, d.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("remove", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()

	d.node.Mu.Lock()
	child, ok := d.node.Children[req.Name]
	d.node.Mu.Unlock()

	if !ok {
		return syscall.ENOENT
	}

	child.Mu.Lock()
	path := child.Meta.Path
	isDir := child.Meta.IsDir
	gen := child.Meta.DataGen
	backends := make([]string, len(child.Meta.Backends))
	copy(backends, child.Meta.Backends)
	child.Mu.Unlock()

	if isDir {
		// M12: Emptiness check in the unified view.
		// Fetch a fresh directory listing from all configured backends to make sure we don't have undiscovered children.
		if err := d.fs.Cache.FetchDir(ctx, path, d.fs.getBackendList()); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, vfs.ErrUnavailable) {
			return err
		}

		child.Mu.RLock()
		hasChildren := len(child.Children) > 0
		child.Mu.RUnlock()

		if hasChildren {
			return syscall.ENOTEMPTY
		}

		// Directories are presence-sets: the cached Meta.Backends list may be
		// incomplete (per-backend mtimes differ), so fan the delete out to ALL
		// configured backends to avoid leaving subtrees that resurrect on the
		// next sync.
		backends = backends[:0]
		for bName := range d.fs.Backends {
			backends = append(backends, bName)
		}
	}

	// Local serialization of same-path mutations.
	unlock := d.fs.pathLocks.lock(path)
	defer unlock()

	// Distributed Locking (always after the local path lock, see RepliFS)
	lock, err := d.fs.acquireLock(ctx, path)
	if err != nil {
		return err
	}
	if lock != nil {
		defer lock.Release()
	}

	// Make the deletion durable BEFORE destroying any data (REVIEW.md C6):
	// write a tombstone at the deletion generation to ALL configured
	// backends, so a replica on a backend that misses the delete cannot
	// resurrect via background sync. Without a tombstone quorum the remove
	// is refused and no data is touched.
	tombGen := gen + 1
	tomb := vfs.Sidecar{DataGen: tombGen, Writer: d.fs.NodeID, Deleted: true}
	observability.Event(ctx, "remove started: writing tombstone to all backends", slog.String("path", path), slog.Int64("tomb_gen", tombGen), slog.Int("quorum", d.fs.WriteQuorum))
	if successes := d.fs.writeTombstones(ctx, path, tomb, d.fs.allBackendNames()); successes < d.fs.WriteQuorum {
		d.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to remove: tombstone write quorum not met (%d/%d)", successes, d.fs.WriteQuorum))

		return fmt.Errorf("could not reach write quorum for tombstone: %d/%d", successes, d.fs.WriteQuorum)
	}

	var successes int
	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		b := d.fs.Backends[bName]
		g.Go(func() error {
			err := b.Remove(gCtx, path)
			mu.Lock()
			defer mu.Unlock()
			// A path that is already absent on a backend is a success for
			// delete purposes (idempotent remove).
			if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
				successes++
			} else {
				observability.Event(ctx, "remove failed on backend", slog.String("backend", bName), slog.Any("error", err))
			}

			return nil
		})
	}
	_ = g.Wait()

	if successes < d.fs.WriteQuorum {
		d.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to remove: file deletion quorum not met (%d/%d)", successes, d.fs.WriteQuorum))

		return fmt.Errorf("could not reach write quorum for remove: %d/%d", successes, d.fs.WriteQuorum)
	}

	// No sidecar cleanup here: sidecars and tombstones share one metadata
	// document per path, and the tombstone written above now occupies that
	// document. Removing it would erase the deletion record and let the path
	// resurrect on the next sync.

	d.node.Mu.Lock()
	delete(d.node.Children, req.Name)
	d.node.Mu.Unlock()

	return nil
}

// clearRenameTarget makes room for a rename onto newPath by durably removing
// an existing target there. It enforces the POSIX type rules (a directory may
// only be replaced by a directory, and only when empty) and tombstones the
// target before deleting its replicas, so a backend that misses the delete
// cannot resurrect it. It is a no-op when nothing exists at the target.
// Caller must already hold the local and distributed locks for newPath.
func (d *Dir) clearRenameTarget(ctx context.Context, targetDir *Dir, name, newPath string, sourceIsDir bool) error {
	targetDir.node.Mu.RLock()
	target, ok := targetDir.node.Children[name]
	targetDir.node.Mu.RUnlock()
	if !ok {
		return nil
	}

	target.Mu.RLock()
	targetIsDir := target.Meta.IsDir
	targetGen := target.Meta.DataGen
	target.Mu.RUnlock()

	switch {
	case targetIsDir && !sourceIsDir:
		return syscall.EISDIR
	case !targetIsDir && sourceIsDir:
		return syscall.ENOTDIR
	case targetIsDir:
		// A directory target may only be replaced when it is empty. Refresh
		// from all backends first so an undiscovered child is not lost.
		if err := d.fs.Cache.FetchDir(ctx, newPath, d.fs.getBackendList()); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, vfs.ErrUnavailable) {
			return err
		}
		target.Mu.RLock()
		hasChildren := len(target.Children) > 0
		target.Mu.RUnlock()
		if hasChildren {
			return syscall.ENOTEMPTY
		}
	}

	// Durable replace (REVIEW.md C6): tombstone the target before destroying
	// any data. The new lineage's generation is lifted above this tombstone by
	// the maxTombstoneGen(newPath) term in Rename.
	all := d.fs.allBackendNames()
	tomb := vfs.Sidecar{DataGen: targetGen + 1, Writer: d.fs.NodeID, Deleted: true}
	if n := d.fs.writeTombstones(ctx, newPath, tomb, all); n < d.fs.WriteQuorum {
		return fmt.Errorf("could not reach tombstone quorum to replace rename target %s: %d/%d", newPath, n, d.fs.WriteQuorum)
	}

	var mu sync.Mutex
	var successes int
	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range all {
		b := d.fs.Backends[bName]
		g.Go(func() error {
			err := b.Remove(gCtx, newPath)
			mu.Lock()
			defer mu.Unlock()
			if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
				successes++
			} else {
				d.fs.pathLogger(ctx, newPath).Error(fmt.Sprintf("Failed to remove rename target on %s: %v", bName, err))
			}

			return nil
		})
	}
	_ = g.Wait()
	if successes < d.fs.WriteQuorum {
		return fmt.Errorf("could not reach remove quorum to replace rename target %s: %d/%d", newPath, successes, d.fs.WriteQuorum)
	}

	// No sidecar cleanup: the tombstone written above shares the target's one
	// metadata document, so removing it would erase the deletion record.

	// Drop the target from the cache tree (the FUSE node is the cache node, so
	// deleting it from the parent's children removes it from the namespace).
	targetDir.node.Mu.Lock()
	delete(targetDir.node.Children, name)
	targetDir.node.Mu.Unlock()

	return nil
}

func (d *Dir) collectDescendants(ctx context.Context, oldPath string, backends []string) ([]string, error) {
	var mu sync.Mutex
	descants := make(map[string]bool)
	g, gCtx := errgroup.WithContext(ctx)

	for _, bName := range backends {
		b, ok := d.fs.Backends[bName]
		if !ok {
			continue
		}
		g.Go(func() error {
			err := b.Walk(gCtx, oldPath, func(p string, info backend.FileInfo) error {
				rel := strings.TrimPrefix(p, oldPath)
				rel = strings.TrimPrefix(rel, "/")
				if rel != "" && rel != "." {
					mu.Lock()
					descants[rel] = true
					mu.Unlock()
				}

				return nil
			})
			if err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
				return err
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	res := make([]string, 0, len(descants))
	for rel := range descants {
		res = append(res, rel)
	}
	slices.Sort(res)

	return res, nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) (err error) {
	ctx = initCtx(ctx, d.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("rename", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()

	d.node.Mu.RLock()
	sourceNode, ok := d.node.Children[req.OldName]
	d.node.Mu.RUnlock()

	if !ok {
		return syscall.ENOENT
	}

	targetDir, ok := newDir.(*Dir)
	if !ok {
		return syscall.EINVAL
	}

	sourceNode.Mu.RLock()
	oldPath := sourceNode.Meta.Path
	isDir := sourceNode.Meta.IsDir
	gen := sourceNode.Meta.DataGen
	backends := make([]string, len(sourceNode.Meta.Backends))
	copy(backends, sourceNode.Meta.Backends)
	sourceNode.Mu.RUnlock()

	if isDir {
		// Directories are presence-sets: the cached Meta.Backends list may be
		// incomplete, so fan the rename out to ALL configured backends to keep
		// the namespace consistent everywhere.
		backends = backends[:0]
		for bName := range d.fs.Backends {
			backends = append(backends, bName)
		}
	}

	targetDir.node.Mu.RLock()
	targetParentPath := targetDir.node.Meta.Path
	targetDir.node.Mu.RUnlock()

	newPath := targetParentPath + "/" + req.NewName
	if targetParentPath == "" {
		newPath = req.NewName
	}
	if vfs.IsReservedPath(oldPath) || vfs.IsReservedPath(newPath) {
		// Renaming into or out of the reserved tree must be impossible.
		return syscall.EACCES
	}
	if newPath == oldPath {
		// Renaming a path onto itself is a no-op success per POSIX.
		return nil
	}

	// Locking for BOTH paths. Acquire in lexicographic order so crossing
	// renames (mv A B vs mv B A) cannot grab one lock each and defeat the
	// other (ABBA conflict). Local path locks first, then the distributed
	// locks (see ordering invariant on RepliFS).
	firstPath, secondPath := oldPath, newPath
	if secondPath < firstPath {
		firstPath, secondPath = secondPath, firstPath
	}

	unlockFirst := d.fs.pathLocks.lock(firstPath)
	defer unlockFirst()
	if firstPath != secondPath {
		unlockSecond := d.fs.pathLocks.lock(secondPath)
		defer unlockSecond()
	}

	firstLock, secondLock, err := d.acquireRenameLocks(ctx, firstPath, secondPath)
	if err != nil {
		return err
	}
	if firstLock != nil {
		defer firstLock.Release()
	}
	if secondLock != nil {
		defer secondLock.Release()
	}

	// POSIX rename must atomically replace an existing target. SMB2 rename
	// fails outright when the target exists, and leaving the target's replicas
	// behind would leak them, so clear the target (durably, with a tombstone)
	// before the source fan-out. Both paths are already locked above.
	if err := d.clearRenameTarget(ctx, targetDir, req.NewName, newPath, isDir); err != nil {
		return err
	}

	var descendants []string
	if isDir {
		var collectErr error
		descendants, collectErr = d.collectDescendants(ctx, oldPath, backends)
		if collectErr != nil {
			d.fs.pathLogger(ctx, oldPath).Warn(fmt.Sprintf("Rename: failed to collect descendants for %s: %v", oldPath, collectErr))
		}
	}

	oldDescendantGens := make(map[string]int64)
	if isDir {
		oldDescendantGens = d.collectDescendantGenerations(ctx, oldPath, descendants, backends)
	}

	successful, failures, err := d.executeBackendRename(ctx, oldPath, newPath, targetParentPath, backends, isDir)
	if err != nil {
		return err
	}

	// The renamed file or directory starts a new lineage at the new path and the old
	// lineage is tombstoned (REVIEW.md C6). Best-effort tombstone at the
	// old path on ALL configured backends: the rename already reached
	// quorum, so a missed tombstone only delays convergence (a stale
	// replica at oldPath survives until the next remove/scrub), it does
	// not lose data — so the rename is not failed. The old-path tombstone
	// stays in the SOURCE lineage: it only needs to exceed the old
	// replicas' generation (gen), so it is gen+1 specifically — not the
	// possibly-larger new-path generation computed below.
	oldTombGen := gen + 1
	tomb := vfs.Sidecar{DataGen: oldTombGen, Writer: d.fs.NodeID, Deleted: true}
	all := d.fs.allBackendNames()
	if n := d.fs.writeTombstones(ctx, oldPath, tomb, all); n < len(all) {
		d.fs.pathLogger(ctx, oldPath).Warn(fmt.Sprintf("Tombstone for renamed path reached only %d/%d backends", n, len(all)))
	}

	// The new path's lineage must start above BOTH the source generation
	// and any tombstone already recorded at newPath: a tombstone at gen T
	// dooms every replica at gen <= T (dropZombies / enforceTombstone), so
	// renaming onto a tombstoned path at gen+1 alone could produce a
	// zombie-in-waiting.
	newGen := max(gen, d.fs.maxTombstoneGen(ctx, newPath)) + 1

	// Fresh sidecar at the new path with the bumped generation on every backend
	// the rename succeeded on (best-effort, like all sidecar writes). The old
	// path's document is NOT removed: the tombstone written above now occupies
	// it, and removing it would erase the deletion record (see Dir.Remove).
	d.fs.writeSidecars(ctx, newPath, vfs.Sidecar{DataGen: newGen, Writer: d.fs.NodeID}, successful)

	// Re-key descendants' metadata documents: write sidecars at the new paths
	// and tombstones at the old paths (REVIEW.md C6-dirs).
	var newDescendantGens map[string]int64
	if isDir {
		newDescendantGens = d.renameDescendants(ctx, oldPath, newPath, descendants, oldDescendantGens, successful, all)
	}

	// Update Cache
	if !d.fs.Cache.Rename(oldPath, newPath) {
		// This should not happen if our VFS logic is correct
		return syscall.EIO
	}

	// Update generations of descendants in the cache
	for rel, newGen := range newDescendantGens {
		newDescendantPath := newPath + "/" + rel
		if node, ok := d.fs.Cache.Get(newDescendantPath); ok {
			node.Mu.Lock()
			node.Meta.DataGen = newGen
			node.Mu.Unlock()
		}
	}

	// The new path carries the bumped generation written above.
	sourceNode.Mu.Lock()
	sourceNode.Meta.DataGen = newGen
	sourceNode.Mu.Unlock()

	if isDir {
		// The renamed directory now exists exactly on the backends where the
		// rename succeeded.
		sourceNode.Mu.Lock()
		sourceNode.Meta.Backends = append([]string(nil), successful...)
		sourceNode.Mu.Unlock()
	}

	// If some backends failed, update the node's backend list
	if len(failures) > 0 {
		if !isDir {
			sourceNode.Mu.Lock()
			newBackends := make([]string, 0, len(sourceNode.Meta.Backends))
			for _, bName := range sourceNode.Meta.Backends {
				failed := slices.Contains(failures, bName)
				if !failed {
					newBackends = append(newBackends, bName)
				}
			}
			sourceNode.Meta.Backends = newBackends
			sourceNode.Mu.Unlock()
		}

		// Asynchronously remove the orphaned file at oldPath from each failed backend.
		for _, bName := range failures {
			d.fs.removeStaleReplica(oldPath, bName)
		}
	}

	return nil
}

func (d *Dir) executeBackendRename(ctx context.Context, oldPath, newPath, targetParentPath string, backends []string, isDir bool) ([]string, []string, error) {
	observability.Event(ctx, "backend rename started", slog.String("old", oldPath), slog.String("new", newPath), slog.Int("backends", len(backends)), slog.Int("quorum", d.fs.WriteQuorum))
	var successful []string
	var failures []string
	var mu sync.Mutex
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		g.Go(func() error {
			b := d.fs.Backends[bName]

			// Ensure destination parent path exists on this backend
			if err := b.MkdirAll(gCtx, targetParentPath, 0755); err != nil {
				observability.Event(ctx, "ensure parent path failed on backend", slog.String("parent", targetParentPath), slog.String("backend", bName), slog.Any("error", err))
				// Continue anyway, maybe it exists
			}

			err := b.Rename(gCtx, oldPath, newPath)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				observability.Event(ctx, "rename ok on backend", slog.String("backend", bName))
				successful = append(successful, bName)
			case isDir && (os.IsNotExist(err) || errors.Is(err, os.ErrNotExist)):
				// The source dir simply doesn't exist on this backend: that is
				// neither a success nor a failure for quorum purposes, and
				// there is no orphan to clean up.
				observability.Event(ctx, "rename skipped on backend (source absent)", slog.String("backend", bName))
			default:
				observability.Event(ctx, "rename failed on backend", slog.String("backend", bName), slog.Any("error", err))
				failures = append(failures, bName)
				lastErr = err
			}

			return nil
		})
	}
	_ = g.Wait()

	if len(successful) < d.fs.WriteQuorum {
		// Rollback successful ones
		for _, bName := range successful {
			b := d.fs.Backends[bName]
			_ = b.Rename(ctx, newPath, oldPath)
		}
		d.fs.pathLogger(ctx, oldPath).Error(fmt.Sprintf("Failed to rename to %s: write quorum not met (%d/%d)", newPath, len(successful), d.fs.WriteQuorum))
		if lastErr != nil {
			return nil, nil, lastErr
		}

		return nil, nil, syscall.EIO
	}

	return successful, failures, nil
}

func (d *Dir) renameDescendants(ctx context.Context, oldPath, newPath string, descendants []string, oldDescendantGens map[string]int64, successful, all []string) map[string]int64 {
	newDescendantGens := make(map[string]int64)
	if len(descendants) > 0 {
		var wg sync.WaitGroup
		var muLock sync.Mutex
		for _, rel := range descendants {
			wg.Add(1)
			go func(rel string) {
				defer wg.Done()
				oldDescendantPath := oldPath + "/" + rel
				newDescendantPath := newPath + "/" + rel
				oldGen := oldDescendantGens[rel]

				newGen := max(oldGen, d.fs.maxTombstoneGen(ctx, newDescendantPath)) + 1
				muLock.Lock()
				newDescendantGens[rel] = newGen
				muLock.Unlock()

				d.fs.writeSidecars(ctx, newDescendantPath, vfs.Sidecar{DataGen: newGen, Writer: d.fs.NodeID}, successful)

				tomb := vfs.Sidecar{DataGen: oldGen + 1, Writer: d.fs.NodeID, Deleted: true}
				if n := d.fs.writeTombstones(ctx, oldDescendantPath, tomb, all); n < len(all) {
					d.fs.pathLogger(ctx, oldDescendantPath).Warn(fmt.Sprintf("Tombstone for renamed descendant path reached only %d/%d backends", n, len(all)))
				}
			}(rel)
		}
		wg.Wait()
	}

	return newDescendantGens
}

func (d *Dir) acquireRenameLocks(ctx context.Context, firstPath, secondPath string) (*vfs.DistributedLock, *vfs.DistributedLock, error) {
	firstLock, err := d.fs.acquireLock(ctx, firstPath)
	if err != nil {
		return nil, nil, err
	}

	if firstPath != secondPath {
		secondLock, err := d.fs.acquireLock(ctx, secondPath)
		if err != nil {
			if firstLock != nil {
				firstLock.Release()
			}

			return nil, nil, err
		}

		return firstLock, secondLock, nil
	}

	return firstLock, nil, nil
}

// collectDescendantGenerations retrieves the current metadata generation for all descendant paths of a directory.
func (d *Dir) collectDescendantGenerations(ctx context.Context, oldPath string, descendants []string, backends []string) map[string]int64 {
	oldDescendantGens := make(map[string]int64)
	if len(descendants) == 0 {
		return oldDescendantGens
	}

	var wg sync.WaitGroup
	var muLock sync.Mutex
	for _, rel := range descendants {
		wg.Add(1)
		go func(rel string) {
			defer wg.Done()
			oldDescendantPath := oldPath + "/" + rel
			oldGen := d.getDescendantGen(ctx, oldDescendantPath, backends)
			muLock.Lock()
			oldDescendantGens[rel] = oldGen
			muLock.Unlock()
		}(rel)
	}
	wg.Wait()

	return oldDescendantGens
}

// getDescendantGen retrieves the generation of a descendant path from cache or backends.
func (d *Dir) getDescendantGen(ctx context.Context, path string, backends []string) int64 {
	if node, ok := d.fs.Cache.Get(path); ok {
		return node.Meta.DataGen
	}
	for _, bName := range backends {
		if b, ok := d.fs.Backends[bName]; ok {
			if sc, err := vfs.ReadSidecar(ctx, b, path); err == nil {
				return sc.DataGen
			}
		}
	}

	return 0
}

type File struct {
	fs   *RepliFS
	node *vfs.Node
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	start := time.Now()
	defer func() {
		observability.RecordFSOp("attr", start)
		err = TranslateError(err)
	}()

	f.fs.fillAttr(a, f.node, fileNlink)

	return nil
}

// Fsync implements fs.NodeFsyncer. bazil dispatches Fsync to the node, not
// the handle, so we sync the backend files held by the node's open write
// handles (which reflect any backends dropped after write failures). If no
// write handle is open — e.g. fsync on a read-only fd — fall back to opening
// each replica read-only and syncing it; the data is already server-side.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	ctx = initCtx(ctx, f.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("fsync", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()
	if hs := f.fs.handles.forNode(f.node); len(hs) > 0 {
		var firstErr error
		for _, h := range hs {
			if err := h.syncBackends(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		return firstErr
	}

	f.node.Mu.RLock()
	backends := make([]string, len(f.node.Meta.Backends))
	copy(backends, f.node.Meta.Backends)
	path := f.node.Meta.Path
	f.node.Mu.RUnlock()

	if len(backends) == 0 {
		return nil
	}

	var mu sync.Mutex
	var successes int
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		g.Go(func() error {
			b, ok := f.fs.Backends[bName]
			if !ok {
				mu.Lock()
				lastErr = fmt.Errorf("backend %s not found", bName)
				mu.Unlock()

				return nil
			}

			sf, err := b.OpenFile(gCtx, path, os.O_RDONLY, 0)
			if err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()

				return nil
			}
			defer sf.Close()

			if err := sf.Sync(gCtx); err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()

				return nil
			}

			mu.Lock()
			successes++
			mu.Unlock()

			return nil
		})
	}
	_ = g.Wait()

	if successes < f.fs.WriteQuorum && len(backends) > 0 {
		return fmt.Errorf("could not reach write quorum during fsync: %d/%d (last error: %w)", successes, f.fs.WriteQuorum, lastErr)
	}

	return nil
}

// Setattr implements fs.NodeSetattrer. Only size changes (truncate) are
// propagated to the backends. Mode/uid/gid/time-only requests are silently
// accepted: SMB cannot represent POSIX permissions and ownership faithfully,
// so we report success without touching the backends rather than break tools
// like cp -p or tar that chmod/chown after writing.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) (err error) {
	ctx = initCtx(ctx, f.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("setattr", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()
	if !req.Valid.Size() {
		return f.Attr(ctx, &resp.Attr)
	}

	f.node.Mu.RLock()
	path := f.node.Meta.Path
	f.node.Mu.RUnlock()

	// Local serialization of same-path mutations, then the distributed lock
	// (always in that order, see RepliFS). Both are released at return.
	unlock := f.fs.pathLocks.lock(path)
	defer unlock()

	lock, err := f.fs.acquireLock(ctx, path)
	if err != nil {
		return err
	}
	if lock != nil {
		defer lock.Release()
	}

	f.node.Mu.RLock()
	backends := make([]string, len(f.node.Meta.Backends))
	copy(backends, f.node.Meta.Backends)
	f.node.Mu.RUnlock()

	if req.Size > math.MaxInt64 {
		return fuse.Errno(syscall.EFBIG)
	}
	size := int64(req.Size) //nolint:gosec // checked bound by MaxInt64
	observability.Event(ctx, "truncate started", slog.String("path", path), slog.Int64("size", size), slog.Int("backends", len(backends)), slog.Int("quorum", f.fs.WriteQuorum))

	var mu sync.Mutex
	var successes int
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		g.Go(func() error {
			b, ok := f.fs.Backends[bName]
			if !ok {
				observability.Event(ctx, "truncate skipped: backend not found", slog.String("backend", bName))
				mu.Lock()
				failures = append(failures, bName)
				lastErr = fmt.Errorf("backend %s not found", bName)
				mu.Unlock()

				return nil
			}
			err := b.Truncate(gCtx, path, size)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				observability.Event(ctx, "truncate failed on backend", slog.String("backend", bName), slog.Any("error", err))
				failures = append(failures, bName)
				lastErr = err
			} else {
				successes++
			}

			return nil
		})
	}
	_ = g.Wait()

	if successes < f.fs.WriteQuorum {
		f.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to truncate: write quorum not met (%d/%d, last error: %v)", successes, f.fs.WriteQuorum, lastErr))

		return fmt.Errorf("could not reach write quorum during truncate: %d/%d (last error: %w)", successes, f.fs.WriteQuorum, lastErr)
	}
	f.fs.pathLogger(ctx, path).Debug(fmt.Sprintf("Truncated file to size %d (quorum %d met)", size, successes))

	f.node.Mu.Lock()
	if len(failures) > 0 {
		// A backend that failed to truncate holds a wrong-length replica —
		// the same hazard as a failed write. Drop it from the metadata and
		// delete the stale replica below (best effort).
		newBackends := make([]string, 0, len(f.node.Meta.Backends))
		for _, bName := range f.node.Meta.Backends {
			failed := slices.Contains(failures, bName)
			if !failed {
				newBackends = append(newBackends, bName)
			}
		}
		f.node.Meta.Backends = newBackends
	}
	f.node.Meta.Size = size
	f.node.Meta.ModTime = time.Now()
	// Truncate is a content mutation: bump the generation once, under the
	// path/distributed lock held above.
	newGen := f.node.Meta.DataGen + 1
	f.node.Meta.DataGen = newGen
	surviving := make([]string, len(f.node.Meta.Backends))
	copy(surviving, f.node.Meta.Backends)
	f.node.Mu.Unlock()

	f.fs.writeSidecars(ctx, path, vfs.Sidecar{DataGen: newGen, Writer: f.fs.NodeID}, surviving)

	for _, bName := range failures {
		f.fs.removeStaleReplica(path, bName)
	}

	return f.Attr(ctx, &resp.Attr)
}

//nolint:ireturn,nonamedreturns
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	ctx = initCtx(ctx, f.fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("open", start)
		err = TranslateError(err)
	}()
	// O_APPEND is not supported: each backend would append at its own
	// current EOF, guaranteeing replica divergence. Supporting it requires
	// handle-tracked write offsets.
	if req.Flags&fuse.OpenAppend != 0 {
		return nil, syscall.ENOTSUP
	}

	f.node.Mu.RLock()
	backends := make([]string, len(f.node.Meta.Backends))
	copy(backends, f.node.Meta.Backends)
	path := f.node.Meta.Path
	f.node.Mu.RUnlock()

	if len(backends) == 0 {
		return nil, syscall.EIO
	}

	h := &FileHandle{
		backends: make(map[string]backend.File),
		readOnly: req.Flags.IsReadOnly(),
	}

	// Locking for writes
	if !req.Flags.IsReadOnly() {
		// Local serialization: held for the duration of Open only (covers
		// the inline healing copy below), released at return. The
		// distributed lock, by contrast, lives with the handle. Always
		// acquired before the distributed lock (see RepliFS).
		unlock := f.fs.pathLocks.lock(path)
		defer unlock()

		lock, err := f.fs.acquireLock(ctx, path)
		if err != nil {
			return nil, err
		}
		h.lock = lock

		// Re-read backends after acquiring the lock to get the latest state
		f.node.Mu.RLock()
		backends = make([]string, len(f.node.Meta.Backends))
		copy(backends, f.node.Meta.Backends)
		f.node.Mu.RUnlock()

		backends, err = f.inlineHeal(ctx, h, path, backends)
		if err != nil {
			return nil, err
		}

		// Generation bump: exactly once per write session, under the path and
		// distributed locks held above. Ordering matters: the inline heal
		// copies above replicate the data at the OLD generation; this single
		// bump+write then stamps the NEW generation onto ALL replicas
		// (including freshly healed targets), so the heal loop needs no
		// per-target sidecar copy of its own.
		f.node.Mu.Lock()
		newGen := f.node.Meta.DataGen + 1
		f.node.Meta.DataGen = newGen
		f.node.Mu.Unlock()
		f.fs.writeSidecars(ctx, path, vfs.Sidecar{DataGen: newGen, Writer: f.fs.NodeID}, backends)
	}

	// Unlock I/O: Open outside of lock (already done since we RUnlocked above)

	if req.Flags.IsReadOnly() {
		if err := f.openReadHandle(ctx, h, path); err != nil {
			return nil, err
		}
	} else {
		for _, bName := range backends {
			b := f.fs.Backends[bName]
			sf, err := b.OpenFile(ctx, path, int(req.Flags), 0)
			if err != nil {
				f.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Failed to open write file on backend %s: %v", bName, err))
				_ = h.Release(ctx, nil)

				return nil, err
			}
			h.backends[bName] = sf
		}

		// The backends just truncated their replicas; reflect that in the
		// cached metadata so Attr does not report the stale size.
		if req.Flags&fuse.OpenTruncate != 0 {
			f.node.Mu.Lock()
			f.node.Meta.Size = 0
			f.node.Meta.ModTime = time.Now()
			f.node.Mu.Unlock()
		}

		// Only write handles matter for node-level Fsync; keep the registry
		// small by not tracking read-only handles.
		f.fs.handles.register(f.node, h)
		f.fs.pathLogger(ctx, path).Debug(fmt.Sprintf("Opened write file handle on backends: %v", backends))
	}

	h.file = f
	atomic.AddInt32(&f.node.OpenHandles, 1)

	return h, nil
}

// inlineHeal replicates a degraded file to additional healthy backends, up to
// the replication factor, before a write session begins. It returns the
// updated backend list. On error it releases h and returns.
func (f *File) inlineHeal(ctx context.Context, h *FileHandle, path string, backends []string) ([]string, error) {
	if len(backends) >= f.fs.ReplicationFactor {
		return backends, nil
	}

	sourceName, targets, ok := f.selectHealTargets(backends)
	if !ok {
		_ = h.Release(ctx, nil)

		return nil, syscall.EIO
	}

	f.fs.pathLogger(ctx, path).Info(fmt.Sprintf("File is degraded (%d/%d backends); starting inline heal from %s to target backends: %v", len(backends), f.fs.ReplicationFactor, sourceName, targets))
	for _, targetName := range targets {
		if err := f.healCopy(ctx, path, sourceName, targetName); err != nil {
			_ = h.Release(ctx, nil)

			return nil, err
		}

		// Update the cache node's metadata: append the target backend name if
		// not already present.
		f.node.Mu.Lock()
		if !slices.Contains(f.node.Meta.Backends, targetName) {
			f.node.Meta.Backends = append(f.node.Meta.Backends, targetName)
		}
		f.node.Mu.Unlock()

		f.fs.pathLogger(ctx, path).Info("Inline heal: successfully replicated to backend " + targetName)
		// Include the new backend so it is opened for writing below.
		backends = append(backends, targetName)
	}
	f.fs.pathLogger(ctx, path).Info("Inline heal completed")

	return backends, nil
}

// selectHealTargets picks a healthy source replica that holds the file and the
// healthy target backends that do not, up to the replication factor. ok is
// false when no healthy source replica exists.
func (f *File) selectHealTargets(backends []string) (string, []string, bool) {
	allBackendNames := make([]string, 0, len(f.fs.Backends))
	for name := range f.fs.Backends {
		allBackendNames = append(allBackendNames, name)
	}

	healthyBackends := f.fs.Selector.SelectForWrite(f.fs.ReplicationFactor, allBackendNames)

	healthyMap := make(map[string]bool)
	for _, hb := range healthyBackends {
		healthyMap[hb] = true
	}

	var source string
	for _, bName := range backends {
		if healthyMap[bName] {
			source = bName

			break
		}
	}
	if source == "" {
		return "", nil, false
	}

	currentBackendsMap := make(map[string]bool)
	for _, bName := range backends {
		currentBackendsMap[bName] = true
	}

	var targets []string
	for _, hb := range healthyBackends {
		if currentBackendsMap[hb] {
			continue
		}
		targets = append(targets, hb)
		// Stop once existing plus new targets reach the replication factor.
		if len(backends)+len(targets) == f.fs.ReplicationFactor {
			break
		}
	}

	return source, targets, true
}

// healCopy replicates path's content from the source backend to the target
// backend and preserves the source replica's mtime.
func (f *File) healCopy(ctx context.Context, path, sourceName, targetName string) error {
	sourceBackend := f.fs.Backends[sourceName]
	srcFile, err := sourceBackend.OpenFile(ctx, path, os.O_RDONLY, 0)
	if err != nil {
		f.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Inline heal: failed to open source file on %s: %v", sourceName, err))

		return err
	}
	defer func() { _ = srcFile.Close() }()

	f.node.Mu.RLock()
	mode := f.node.Meta.Mode
	f.node.Mu.RUnlock()

	targetBackend := f.fs.Backends[targetName]
	dstFile, err := targetBackend.OpenFile(ctx, path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, mode)
	if err != nil {
		f.fs.pathLogger(ctx, path).Error(fmt.Sprintf("Inline heal: failed to create target file on %s: %v", targetName, err))

		return err
	}
	defer func() { _ = dstFile.Close() }()

	// Content checksums are deliberately not computed here (unlike
	// RepairManager.repairNode): the post-heal generation bump in Open blanks
	// Sidecar.FileHash anyway, since the write session is about to mutate content.
	reader := &offsetReader{ctx: ctx, f: srcFile}
	writer := &offsetWriter{ctx: ctx, f: dstFile}
	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}

	// Preserve the source replica's mtime on the destination so the replicas
	// compare as the same version during reconciliation.
	f.node.Mu.RLock()
	cachedModTime := f.node.Meta.ModTime
	f.node.Mu.RUnlock()
	mtime := sourceModTime(ctx, sourceBackend, path, cachedModTime)
	if err := targetBackend.Chtimes(ctx, path, mtime, mtime); err != nil {
		f.fs.pathLogger(ctx, path).Warn(fmt.Sprintf("Failed to preserve mtime on %s: %v", targetName, err))
	}

	return nil
}

// openReadHandle opens a read-only replica for h, trying the selector's
// preferred backend first and falling back to the remaining replicas.
func (f *File) openReadHandle(ctx context.Context, h *FileHandle, path string) error {
	f.node.Mu.RLock()
	meta := f.node.Meta
	f.node.Mu.RUnlock()

	// Candidate order: the selector's pick first (health-aware/random
	// placement), then any remaining replicas as listed in the metadata.
	var candidates []string
	if bName := f.fs.Selector.SelectForRead(meta); bName != "" {
		candidates = append(candidates, bName)
	}
	for _, bName := range meta.Backends {
		if len(candidates) > 0 && bName == candidates[0] {
			continue
		}
		candidates = append(candidates, bName)
	}
	if len(candidates) == 0 {
		return syscall.EIO
	}

	var lastErr error
	for _, bName := range candidates {
		b, ok := f.fs.Backends[bName]
		if !ok {
			f.fs.pathLogger(ctx, path).Warn(fmt.Sprintf("Backend %s listed is not configured", bName))

			continue
		}
		sf, err := b.OpenFile(ctx, path, os.O_RDONLY, 0)
		if err != nil {
			f.fs.pathLogger(ctx, path).Warn(fmt.Sprintf("Read-only open failed on backend %s: %v", bName, err))
			lastErr = err

			continue
		}
		h.backends[bName] = sf
		f.fs.pathLogger(ctx, path).Debug(fmt.Sprintf("Opened read-only file handle on backends: %v", candidates))

		return nil
	}

	f.fs.pathLogger(ctx, path).Error("Failed to open read-only file on any backend")
	if lastErr == nil {
		return syscall.EIO
	}

	return lastErr
}

type FileHandle struct {
	file     *File
	mu       sync.Mutex
	backends map[string]backend.File
	lock     *vfs.DistributedLock
	readOnly bool
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	var fs *RepliFS
	if h.file != nil {
		fs = h.file.fs
	}
	ctx = initCtx(ctx, fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("read", start)
		err = TranslateError(err)
	}()
	buf := make([]byte, req.Size)
	tried := make(map[string]bool)

	// Retry loop for failover
	for {
		h.mu.Lock()
		var sf backend.File
		var currentBackendName string
		for name, f := range h.backends {
			if !tried[name] {
				sf = f
				currentBackendName = name

				break
			}
		}
		h.mu.Unlock()

		if sf == nil {
			// Try to open a fallback backend
			if !h.openFallbackBackend(ctx, tried) {
				return syscall.EIO
			}

			continue
		}

		n, err := sf.ReadAt(ctx, buf, req.Offset)
		if err != nil && !errors.Is(err, io.EOF) {
			h.file.fs.pathLogger(ctx, h.file.node.Meta.Path).Warn(fmt.Sprintf("Read failed on backend %s: %v. Retrying...", currentBackendName, err))

			tried[currentBackendName] = true

			h.mu.Lock()
			// Close and remove the failed backend
			_ = sf.Close()
			delete(h.backends, currentBackendName)
			h.mu.Unlock()

			// Loop will retry with next available or try to open new one
			continue
		}

		resp.Data = buf[:n]

		return nil
	}
}

func (h *FileHandle) openFallbackBackend(ctx context.Context, tried map[string]bool) bool {
	h.file.node.Mu.RLock()
	allBackends := h.file.node.Meta.Backends
	path := h.file.node.Meta.Path
	h.file.node.Mu.RUnlock()

	// Find a backend that we haven't tried yet
	for _, bName := range allBackends {
		if tried[bName] {
			continue
		}

		h.mu.Lock()
		if _, ok := h.backends[bName]; ok {
			h.mu.Unlock()

			return true
		}
		h.mu.Unlock()

		b := h.file.fs.Backends[bName]
		sf, err := b.OpenFile(ctx, path, os.O_RDONLY, 0)
		if err == nil {
			h.mu.Lock()
			h.backends[bName] = sf
			h.mu.Unlock()

			return true
		}
		// Mark as tried if Open fails too
		tried[bName] = true
	}

	return false
}

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	var fs *RepliFS
	if h.file != nil {
		fs = h.file.fs
	}
	ctx = initCtx(ctx, fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("write", start)
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()
	if h.lock != nil && !h.lock.IsValidWithBuffer(h.file.fs.WriteLeaseBuffer) {
		return syscall.EIO // Lock lost or too close to expiry
	}

	h.mu.Lock()
	backends := make(map[string]backend.File, len(h.backends))
	maps.Copy(backends, h.backends)
	h.mu.Unlock()

	observability.Event(ctx, "write started", slog.String("path", h.file.node.Meta.Path), slog.Int("backends", len(backends)), slog.Int("bytes", len(req.Data)), slog.Int64("offset", req.Offset), slog.Int("quorum", h.file.fs.WriteQuorum))
	var mu sync.Mutex
	var successes int
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for name, sf := range backends {
		g.Go(func() error {
			_, err := sf.WriteAt(gCtx, req.Data, req.Offset)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				observability.Event(ctx, "write failed on backend", slog.String("backend", name), slog.Any("error", err))
				failures = append(failures, name)
				lastErr = err
			} else {
				successes++
			}

			return nil
		})
	}
	_ = g.Wait()

	if successes < h.file.fs.WriteQuorum {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if len(failures) > 0 {
			h.cleanupFailedBackends(failures)
		}

		return fmt.Errorf("could not reach write quorum: %d/%d (last error: %w)", successes, h.file.fs.WriteQuorum, lastErr)
	}

	// Remove failed backends from the handle and from VFS metadata
	if len(failures) > 0 {
		h.cleanupFailedBackends(failures)
	}

	// Re-check the lease AFTER the backend writes complete, before
	// acknowledging to the kernel. The pre-write check above is racy: the
	// lease can expire while the SMB writes are in flight and a new owner may
	// have been granted the lock (REVIEW C3). SMB offers no fencing
	// primitive, so this post-write check is the strongest guarantee
	// available — it shrinks the stale-holder window from a full I/O
	// duration to the gap between the last WriteAt and this line. A write
	// that landed under a lost lease is reported as an error so the
	// application does not build on unfenced state.
	if h.lock != nil && !h.lock.IsValid() {
		return syscall.EIO
	}

	resp.Size = len(req.Data)

	// Update cache size and mtime
	h.file.node.Mu.Lock()
	newSize := req.Offset + int64(len(req.Data))
	if newSize > h.file.node.Meta.Size {
		h.file.node.Meta.Size = newSize
	}
	h.file.node.Meta.ModTime = time.Now()
	h.file.node.Mu.Unlock()

	return nil
}

//nolint:contextcheck // invokes removeStaleReplica asynchronously in the background
func (h *FileHandle) cleanupFailedBackends(failures []string) {
	h.mu.Lock()
	for _, name := range failures {
		if sf, ok := h.backends[name]; ok {
			_ = sf.Close()
			delete(h.backends, name)
		}
	}
	h.mu.Unlock()

	h.file.node.Mu.Lock()
	newBackends := make([]string, 0, len(h.file.node.Meta.Backends))
	for _, bName := range h.file.node.Meta.Backends {
		failed := slices.Contains(failures, bName)
		if !failed {
			newBackends = append(newBackends, bName)
		}
	}
	h.file.node.Meta.Backends = newBackends
	path := h.file.node.Meta.Path
	h.file.node.Mu.Unlock()

	// The replica on each failed backend still holds partial content from
	// earlier successful writes in this session. Now that it is untracked,
	// delete it so the background sync cannot resurrect it as a same-path
	// candidate (where clock skew could even elect the stale partial as the
	// LWW winner). Best-effort: see removeStaleReplica.
	for _, name := range failures {
		h.file.fs.removeStaleReplica(path, name)
	}
}

func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	if h.readOnly {
		return nil
	}

	var fs *RepliFS
	if h.file != nil {
		fs = h.file.fs
	}
	ctx = initCtx(ctx, fs, req.Hdr())
	start := time.Now()
	defer func() {
		observability.RecordFSOp("flush", start)
		// Dump the request's breadcrumbs only if the flush failed.
		observability.TraceFrom(ctx).FlushOnError(ctx, observability.Logger(ctx), slog.LevelWarn, err)
		err = TranslateError(err)
	}()

	return h.syncBackends(ctx)
}

func (h *FileHandle) syncBackends(ctx context.Context) error {
	if h.lock != nil && !h.lock.IsValidWithBuffer(h.file.fs.WriteLeaseBuffer) {
		return syscall.EIO // Lock lost or too close to expiry
	}

	h.mu.Lock()
	backends := make(map[string]backend.File, len(h.backends))
	maps.Copy(backends, h.backends)
	h.mu.Unlock()

	if len(backends) == 0 {
		return nil
	}
	observability.Event(ctx, "sync started", slog.String("path", h.file.node.Meta.Path), slog.Int("backends", len(backends)), slog.Int("quorum", h.file.fs.WriteQuorum))

	var mu sync.Mutex
	var successes int
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for name, sf := range backends {
		g.Go(func() error {
			err := sf.Sync(gCtx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// A canceled context means the FUSE request was aborted (writer
				// killed/interrupted or the mount is shutting down), not that the
				// backend itself misbehaved; record it as such rather than blaming
				// the backend. The breadcrumb surfaces only if the op ultimately
				// fails (see Flush/Fsync trace dump).
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					observability.Event(ctx, "sync aborted on backend (request canceled)", slog.String("backend", name), slog.Any("error", err))
				} else {
					observability.Event(ctx, "sync failed on backend", slog.String("backend", name), slog.Any("error", err))
				}
				failures = append(failures, name)
				lastErr = err
			} else {
				observability.Event(ctx, "sync ok on backend", slog.String("backend", name))
				successes++
			}

			return nil
		})
	}
	_ = g.Wait()

	if successes < h.file.fs.WriteQuorum {
		// Separate an aborted flush (client closed the file or the mount is going
		// away) from a genuine durability failure: the former is expected churn
		// and should not read as a data-loss error.
		if cerr := ctx.Err(); cerr != nil {
			h.file.fs.pathLogger(ctx, h.file.node.Meta.Path).Warn(fmt.Sprintf("Sync aborted before write quorum (%d/%d): request canceled (%v); writer exited or mount shutting down, data not flushed", successes, h.file.fs.WriteQuorum, cerr))

			return fmt.Errorf("sync aborted before write quorum: %d/%d (%w)", successes, h.file.fs.WriteQuorum, cerr)
		}

		h.file.fs.pathLogger(ctx, h.file.node.Meta.Path).Error(fmt.Sprintf("Failed to sync: write quorum not met (%d/%d, last error: %v)", successes, h.file.fs.WriteQuorum, lastErr))

		return fmt.Errorf("could not reach write quorum during sync: %d/%d (last error: %w)", successes, h.file.fs.WriteQuorum, lastErr)
	}

	observability.Event(ctx, "sync quorum met", slog.Int("successes", successes), slog.Int("quorum", h.file.fs.WriteQuorum))
	if len(failures) > 0 {
		h.cleanupFailedBackends(failures)
	}

	return nil
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	var fs *RepliFS
	if h.file != nil {
		fs = h.file.fs
	}
	var hdr *fuse.Header
	if req != nil {
		hdr = req.Hdr()
	}
	ctx = initCtx(ctx, fs, hdr)
	start := time.Now()
	defer func() {
		observability.RecordFSOp("release", start)
		err = TranslateError(err)
	}()
	// Dir.Create's abort paths release the handle before h.file is set, and
	// read-only handles were never registered; deregister tolerates both.
	if h.file != nil {
		h.file.fs.handles.deregister(h.file.node, h)
		atomic.AddInt32(&h.file.node.OpenHandles, -1)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, sf := range h.backends {
		_ = sf.Close()
	}
	h.backends = nil
	if h.lock != nil {
		h.lock.Release()
	}
	if h.file != nil {
		h.file.fs.pathLogger(ctx, h.file.node.Meta.Path).Debug("Released file handle")
	}

	return nil
}

func (f *RepliFS) WriteSidecars(ctx context.Context, path string, sc vfs.Sidecar, backendNames []string) {
	f.writeSidecars(ctx, path, sc, backendNames)
}

func (f *RepliFS) WriteTombstones(ctx context.Context, path string, sc vfs.Sidecar, backendNames []string) int {
	return f.writeTombstones(ctx, path, sc, backendNames)
}

func (f *RepliFS) MaxTombstoneGen(ctx context.Context, path string) int64 {
	return f.maxTombstoneGen(ctx, path)
}

func (f *RepliFS) AllBackendNames() []string {
	return f.allBackendNames()
}
