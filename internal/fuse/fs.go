package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type RepliFS struct {
	Cache             *vfs.Cache
	Backends          map[string]backend.Backend
	ReplicationFactor int
	WriteQuorum       int
	Selector          vfs.BackendSelector
	LockManager       *cluster.LockManager
	Discovery         *cluster.Discovery

	// NodeID identifies this node as the writer in version sidecars
	// (diagnostics only). Set from main; an empty string is fine when
	// clustering is off — sidecar correctness depends only on Gen.
	NodeID string

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

func (f *RepliFS) Root() (fs.Node, error) {
	return &Dir{fs: f, node: f.Cache.Root}, nil
}

func (f *RepliFS) acquireLock(ctx context.Context, path string) (*vfs.DistributedLock, error) {
	if f.LockManager == nil || f.Discovery == nil {
		return nil, nil // Locking disabled or not configured
	}
	lock := vfs.NewDistributedLock(path, f.LockManager, f.Discovery)
	if err := lock.Acquire(ctx); err != nil {
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
func (f *RepliFS) removeStaleReplica(path string, backendName string) {
	b, ok := f.Backends[backendName]
	if !ok {
		return
	}
	go func() {
		var lastErr error
		for attempt := 0; attempt < 2; attempt++ {
			if attempt > 0 {
				time.Sleep(5 * time.Second)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := b.Remove(ctx, path)
			cancel()
			if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
				return
			}
			lastErr = err
		}
		logrus.Warnf("Failed to remove stale replica %s from backend %s: %v", path, backendName, lastErr)
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
				logrus.Warnf("Failed to write sidecar (gen %d) for %s on backend %s: %v", sc.Gen, path, bName, err)
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
				logrus.Warnf("Failed to write tombstone (gen %d) for %s on backend %s: %v", sc.Gen, path, bName, err)
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
			sc, err := vfs.ReadTombstone(ctx, b, path)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
					logrus.Debugf("Tombstone read for %q on %s failed: %v", path, bName, err)
				}
				return
			}
			mu.Lock()
			if sc.Gen > maxGen {
				maxGen = sc.Gen
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

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	d.node.Mu.RLock()
	defer d.node.Mu.RUnlock()

	a.Mode = d.node.Meta.Mode
	a.Size = uint64(d.node.Meta.Size)
	a.Mtime = d.node.Meta.ModTime
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.node.Mu.RLock()
	child, ok := d.node.Children[name]
	fullyIndexed := d.node.FullyIndexed
	path := d.node.Meta.Path
	d.node.Mu.RUnlock()

	childPath := name
	if path != "" {
		childPath = path + "/" + name
	}
	if vfs.IsReservedPath(childPath) {
		// Cluster-internal state is invisible through the mount.
		return nil, syscall.ENOENT
	}

	if !ok {
		if fullyIndexed {
			return nil, syscall.ENOENT
		}

		// Try lazy fetch
		logrus.Debugf("Lazy fetching metadata for %s", childPath)
		node, err := d.fs.Cache.FetchEntry(ctx, childPath, d.fs.getBackendList())
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, syscall.ENOENT
			}
			// ErrUnavailable or any other error: we don't know whether the
			// entry exists, so don't lie with ENOENT.
			return nil, syscall.EIO
		}
		child = node
	}

	if child.Meta.IsDir {
		return &Dir{fs: d.fs, node: child}, nil
	}
	return &File{fs: d.fs, node: child}, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.node.Mu.RLock()
	fullyIndexed := d.node.FullyIndexed
	path := d.node.Meta.Path
	d.node.Mu.RUnlock()

	var fetchErr error
	if !fullyIndexed {
		logrus.Debugf("Lazy fetching directory listing for %s", path)
		if fetchErr = d.fs.Cache.FetchDir(ctx, path, d.fs.getBackendList()); fetchErr != nil {
			logrus.Errorf("FetchDir failed for %s: %v", path, fetchErr)
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

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
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

	if len(selectedBackends) == 0 {
		if lock != nil {
			lock.Release()
		}
		return nil, nil, syscall.EIO
	}

	h := &FileHandle{
		backends: make(map[string]backend.File),
		lock:     lock,
	}

	// Perform I/O outside of VFS lock to prevent deadlock
	var mu sync.Mutex
	var successfulBackends []string
	var existsCount int
	g, gCtx := errgroup.WithContext(ctx)

	for _, bName := range selectedBackends {
		bName := bName // capture for goroutine
		g.Go(func() error {
			b := d.fs.Backends[bName]
			if parentPath != "" {
				if err := b.MkdirAll(gCtx, parentPath, 0755); err != nil {
					logrus.Warnf("Failed to MkdirAll parent %s on backend %s: %v", parentPath, bName, err)
				}
			}
			sf, err := b.OpenFile(gCtx, path, os.O_CREATE|os.O_EXCL|os.O_RDWR, req.Mode)
			if err != nil {
				if os.IsExist(err) || errors.Is(err, os.ErrExist) {
					// The file already exists on this backend (e.g. created by
					// another cluster node and not yet in our cache).
					mu.Lock()
					existsCount++
					mu.Unlock()
					return nil
				}
				logrus.Warnf("Failed to create file %s on backend %s: %v", path, bName, err)
				return nil // Don't fail the whole operation yet
			}
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
				logrus.Warnf("FetchEntry after create conflict for %s failed: %v", path, ferr)
			}
			return nil, nil, syscall.EEXIST
		}
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
	d.fs.writeSidecars(ctx, path, vfs.Sidecar{Gen: newGen, Writer: d.fs.NodeID}, successfulBackends)

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
		return nil, nil, syscall.EEXIST
	}

	meta := vfs.Metadata{
		Name:     req.Name,
		Path:     path,
		Mode:     req.Mode,
		Backends: successfulBackends,
		Gen:      newGen,
	}

	child := &vfs.Node{
		Meta:     meta,
		Children: make(map[string]*vfs.Node),
	}
	d.node.Children[req.Name] = child
	h.file = &File{fs: d.fs, node: child}
	d.fs.handles.register(child, h)

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
			logrus.Warnf("Failed to remove partially created file %s on backend %s: %v", path, bName, err)
		}
	}
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
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

	for name, b := range d.fs.Backends {
		name, b := name, b
		g.Go(func() error {
			if parentPath != "" {
				if err := b.MkdirAll(gCtx, parentPath, 0755); err != nil {
					logrus.Warnf("Failed to MkdirAll parent %s on backend %s: %v", parentPath, name, err)
				}
			}
			err := b.Mkdir(gCtx, path, req.Mode)
			if err != nil {
				if os.IsExist(err) || errors.Is(err, os.ErrExist) {
					// The directory may already exist on this backend (e.g. created
					// by another cluster node). Verify it really is a directory.
					info, statErr := b.Stat(gCtx, path)
					if statErr != nil {
						logrus.Warnf("Failed to stat existing path %s on %s: %v", path, name, statErr)
						return nil
					}
					if !info.IsDir {
						logrus.Warnf("Path %s already exists on %s but is not a directory", path, name)
						return nil
					}
					mu.Lock()
					satisfiedOn = append(satisfiedOn, name)
					mu.Unlock()
					return nil
				}
				logrus.Warnf("Failed to create directory %s on %s: %v", path, name, err)
				return nil // Don't fail the whole operation if one backend fails
			}
			mu.Lock()
			createdOn = append(createdOn, name)
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
		return nil, fmt.Errorf("could not reach write quorum for mkdir: %d/%d", len(satisfiedOn), d.fs.WriteQuorum)
	}

	d.node.Mu.Lock()
	defer d.node.Mu.Unlock()

	// Check conflict
	if _, ok := d.node.Children[req.Name]; ok {
		return nil, syscall.EEXIST
	}

	meta := vfs.Metadata{
		Name:     req.Name,
		Path:     path,
		Mode:     req.Mode | os.ModeDir,
		IsDir:    true,
		Backends: satisfiedOn,
	}
	child := &vfs.Node{
		Meta:     meta,
		Children: make(map[string]*vfs.Node),
	}
	d.node.Children[req.Name] = child
	return &Dir{fs: d.fs, node: child}, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.node.Mu.Lock()
	child, ok := d.node.Children[req.Name]
	d.node.Mu.Unlock()

	if !ok {
		return syscall.ENOENT
	}

	child.Mu.Lock()
	path := child.Meta.Path
	isDir := child.Meta.IsDir
	gen := child.Meta.Gen
	backends := make([]string, len(child.Meta.Backends))
	copy(backends, child.Meta.Backends)
	child.Mu.Unlock()

	if isDir {
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

	if !isDir {
		// Make the deletion durable BEFORE destroying any data (REVIEW.md C6):
		// write a tombstone at the deletion generation to ALL configured
		// backends, so a replica on a backend that misses the delete cannot
		// resurrect via background sync. Without a tombstone quorum the remove
		// is refused and no data is touched.
		// TODO(C6-dirs): directory deletions still rely on the all-backends
		// fan-out below and can resurrect from an offline backend.
		tombGen := gen + 1
		tomb := vfs.Sidecar{Gen: tombGen, Writer: d.fs.NodeID, Deleted: true}
		if successes := d.fs.writeTombstones(ctx, path, tomb, d.fs.allBackendNames()); successes < d.fs.WriteQuorum {
			return fmt.Errorf("could not reach write quorum for tombstone: %d/%d", successes, d.fs.WriteQuorum)
		}
	}

	var successes int
	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		bName := bName
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
				logrus.Errorf("Failed to remove %s from %s: %v", path, bName, err)
			}
			return nil
		})
	}
	_ = g.Wait()

	if successes < d.fs.WriteQuorum {
		return fmt.Errorf("could not reach write quorum for remove: %d/%d", successes, d.fs.WriteQuorum)
	}

	if !isDir {
		// Best-effort sidecar cleanup on the backends that held the file.
		// Directories have no sidecars. Durability is carried by the
		// tombstones written above, not by this cleanup.
		for _, bName := range backends {
			b, okB := d.fs.Backends[bName]
			if !okB {
				continue
			}
			if err := vfs.RemoveSidecar(ctx, b, path); err != nil {
				logrus.Warnf("Failed to remove sidecar for %s on backend %s: %v", path, bName, err)
			}
		}
	}

	d.node.Mu.Lock()
	delete(d.node.Children, req.Name)
	d.node.Mu.Unlock()

	return nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
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
	gen := sourceNode.Meta.Gen
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

	firstLock, err := d.fs.acquireLock(ctx, firstPath)
	if err != nil {
		return err
	}
	if firstLock != nil {
		defer firstLock.Release()
	}

	if firstPath != secondPath {
		secondLock, err := d.fs.acquireLock(ctx, secondPath)
		if err != nil {
			return err
		}
		if secondLock != nil {
			defer secondLock.Release()
		}
	}

	var mu sync.Mutex
	var successful []string
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		bName := bName
		g.Go(func() error {
			b := d.fs.Backends[bName]

			// Ensure destination parent path exists on this backend
			if err := b.MkdirAll(gCtx, targetParentPath, 0755); err != nil {
				logrus.Warnf("Failed to ensure parent path %s on backend %s: %v", targetParentPath, bName, err)
				// Continue anyway, maybe it exists
			}

			err := b.Rename(gCtx, oldPath, newPath)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successful = append(successful, bName)
			case isDir && (os.IsNotExist(err) || errors.Is(err, os.ErrNotExist)):
				// The source dir simply doesn't exist on this backend: that is
				// neither a success nor a failure for quorum purposes, and
				// there is no orphan to clean up.
				logrus.Debugf("Skipping rename of %s on %s: source does not exist", oldPath, bName)
			default:
				logrus.Warnf("Failed to rename %s to %s on %s: %v", oldPath, newPath, bName, err)
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
		if lastErr != nil {
			return lastErr
		}
		return syscall.EIO
	}

	var newGen int64
	if !isDir {
		// The renamed file starts a new lineage at the new path and the old
		// lineage is tombstoned (REVIEW.md C6). Best-effort tombstone at the
		// old path on ALL configured backends: the rename already reached
		// quorum, so a missed tombstone only delays convergence (a stale
		// replica at oldPath survives until the next remove/scrub), it does
		// not lose data — so the rename is not failed. The old-path tombstone
		// stays in the SOURCE lineage: it only needs to exceed the old
		// replicas' generation (gen), so it is gen+1 specifically — not the
		// possibly-larger new-path generation computed below.
		oldTombGen := gen + 1
		tomb := vfs.Sidecar{Gen: oldTombGen, Writer: d.fs.NodeID, Deleted: true}
		all := d.fs.allBackendNames()
		if n := d.fs.writeTombstones(ctx, oldPath, tomb, all); n < len(all) {
			logrus.Warnf("Tombstone for renamed path %s reached only %d/%d backends", oldPath, n, len(all))
		}

		// The new path's lineage must start above BOTH the source generation
		// and any tombstone already recorded at newPath: a tombstone at gen T
		// dooms every replica at gen <= T (dropZombies / enforceTombstone), so
		// renaming onto a tombstoned path at gen+1 alone could produce a
		// zombie-in-waiting.
		newGen = max(gen, d.fs.maxTombstoneGen(ctx, newPath)) + 1

		// Fresh sidecar at the new path with the bumped generation on every
		// backend the rename succeeded on (best-effort, like all sidecar
		// writes), plus best-effort cleanup of the now-orphaned old sidecar.
		d.fs.writeSidecars(ctx, newPath, vfs.Sidecar{Gen: newGen, Writer: d.fs.NodeID}, successful)
		for _, bName := range successful {
			if err := vfs.RemoveSidecar(ctx, d.fs.Backends[bName], oldPath); err != nil {
				logrus.Debugf("Failed to remove old sidecar for %s on %s: %v", oldPath, bName, err)
			}
		}
	}
	// TODO(meta-dir-rename): the sidecar tree mirrors the data tree, so
	// renaming a data directory D leaves the sidecars under
	// .replistore/meta/D stale (replicas of moved files report generation 0
	// until rewritten). TODO(C6-dirs): directory renames also write no
	// tombstones, so a subtree on an offline backend can resurrect.

	// Update Cache
	if !d.fs.Cache.Rename(oldPath, newPath) {
		// This should not happen if our VFS logic is correct
		return syscall.EIO
	}

	if !isDir {
		// The new path carries the bumped generation written above.
		sourceNode.Mu.Lock()
		sourceNode.Meta.Gen = newGen
		sourceNode.Mu.Unlock()
	}

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
				failed := false
				for _, fName := range failures {
					if bName == fName {
						failed = true
						break
					}
				}
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

type File struct {
	fs   *RepliFS
	node *vfs.Node
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.node.Mu.RLock()
	defer f.node.Mu.RUnlock()

	a.Mode = f.node.Meta.Mode
	a.Size = uint64(f.node.Meta.Size)
	a.Mtime = f.node.Meta.ModTime
	return nil
}

// Fsync implements fs.NodeFsyncer. bazil dispatches Fsync to the node, not
// the handle, so we sync the backend files held by the node's open write
// handles (which reflect any backends dropped after write failures). If no
// write handle is open — e.g. fsync on a read-only fd — fall back to opening
// each replica read-only and syncing it; the data is already server-side.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
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
		bName := bName
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
		return fmt.Errorf("could not reach write quorum during fsync: %d/%d (last error: %v)", successes, f.fs.WriteQuorum, lastErr)
	}

	return nil
}

// Setattr implements fs.NodeSetattrer. Only size changes (truncate) are
// propagated to the backends. Mode/uid/gid/time-only requests are silently
// accepted: SMB cannot represent POSIX permissions and ownership faithfully,
// so we report success without touching the backends rather than break tools
// like cp -p or tar that chmod/chown after writing.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
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

	size := int64(req.Size)

	var mu sync.Mutex
	var successes int
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		bName := bName
		g.Go(func() error {
			b, ok := f.fs.Backends[bName]
			if !ok {
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
				logrus.Warnf("Truncate failed on backend %s: %v", bName, err)
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
		return fmt.Errorf("could not reach write quorum during truncate: %d/%d (last error: %v)", successes, f.fs.WriteQuorum, lastErr)
	}

	f.node.Mu.Lock()
	if len(failures) > 0 {
		// A backend that failed to truncate holds a wrong-length replica —
		// the same hazard as a failed write. Drop it from the metadata and
		// delete the stale replica below (best effort).
		newBackends := make([]string, 0, len(f.node.Meta.Backends))
		for _, bName := range f.node.Meta.Backends {
			failed := false
			for _, fName := range failures {
				if bName == fName {
					failed = true
					break
				}
			}
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
	newGen := f.node.Meta.Gen + 1
	f.node.Meta.Gen = newGen
	surviving := make([]string, len(f.node.Meta.Backends))
	copy(surviving, f.node.Meta.Backends)
	f.node.Mu.Unlock()

	f.fs.writeSidecars(ctx, path, vfs.Sidecar{Gen: newGen, Writer: f.fs.NodeID}, surviving)

	for _, bName := range failures {
		f.fs.removeStaleReplica(path, bName)
	}

	return f.Attr(ctx, &resp.Attr)
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
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
		file:     f,
		backends: make(map[string]backend.File),
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

		if len(backends) < f.fs.ReplicationFactor {
			allBackendNames := make([]string, 0, len(f.fs.Backends))
			for name := range f.fs.Backends {
				allBackendNames = append(allBackendNames, name)
			}

			healthyBackends := f.fs.Selector.SelectForWrite(f.fs.ReplicationFactor, allBackendNames)

			healthyMap := make(map[string]bool)
			for _, hb := range healthyBackends {
				healthyMap[hb] = true
			}

			// Find if there is at least one healthy backend that currently contains the file (source backend)
			var sourceBackendName string
			for _, bName := range backends {
				if healthyMap[bName] {
					sourceBackendName = bName
					break
				}
			}

			if sourceBackendName == "" {
				_ = h.Release(ctx, nil)
				return nil, syscall.EIO
			}

			// Find target backends (healthy backends that do not currently have the file) up to the replication factor.
			currentBackendsMap := make(map[string]bool)
			for _, bName := range backends {
				currentBackendsMap[bName] = true
			}

			var targetBackends []string
			for _, hb := range healthyBackends {
				if !currentBackendsMap[hb] {
					targetBackends = append(targetBackends, hb)
					// Stop if we have reached the replication factor in total (existing backends + new targets)
					if len(backends)+len(targetBackends) == f.fs.ReplicationFactor {
						break
					}
				}
			}

			// For each target backend:
			for _, targetName := range targetBackends {
				sourceBackend := f.fs.Backends[sourceBackendName]
				srcFile, err := sourceBackend.OpenFile(ctx, path, os.O_RDONLY, 0)
				if err != nil {
					_ = h.Release(ctx, nil)
					return nil, err
				}

				f.node.Mu.RLock()
				mode := f.node.Meta.Mode
				f.node.Mu.RUnlock()

				targetBackend := f.fs.Backends[targetName]
				dstFile, err := targetBackend.OpenFile(ctx, path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, mode)
				if err != nil {
					_ = srcFile.Close()
					_ = h.Release(ctx, nil)
					return nil, err
				}

				// Copy the data from the source file to the target file.
				// Content checksums are deliberately not computed here
				// (unlike RepairManager.repairNode): the post-heal generation
				// bump below blanks Sidecar.Sum anyway, since the write
				// session is about to mutate the content.
				reader := &offsetReader{ctx: ctx, f: srcFile}
				writer := &offsetWriter{ctx: ctx, f: dstFile}
				if _, err := io.Copy(writer, reader); err != nil {
					_ = srcFile.Close()
					_ = dstFile.Close()
					_ = h.Release(ctx, nil)
					return nil, err
				}

				_ = srcFile.Close()
				_ = dstFile.Close()

				// Preserve the source replica's mtime on the destination so
				// the replicas compare as the same version during
				// reconciliation (see RepairManager.repairNode).
				f.node.Mu.RLock()
				cachedModTime := f.node.Meta.ModTime
				f.node.Mu.RUnlock()
				mtime := sourceModTime(ctx, sourceBackend, path, cachedModTime)
				if err := targetBackend.Chtimes(ctx, path, mtime, mtime); err != nil {
					logrus.Warnf("Failed to preserve mtime for %s on %s: %v", path, targetName, err)
				}

				// Update the cache node's metadata: acquire f.node.Mu.Lock(), append the target backend name to f.node.Meta.Backends if not already present, and unlock.
				f.node.Mu.Lock()
				found := false
				for _, b := range f.node.Meta.Backends {
					if b == targetName {
						found = true
						break
					}
				}
				if !found {
					f.node.Meta.Backends = append(f.node.Meta.Backends, targetName)
				}
				f.node.Mu.Unlock()

				// Update the local backends slice variable to include the new backend so that it is opened for writing by the subsequent loop.
				backends = append(backends, targetName)
			}
		}

		// Generation bump: exactly once per write session, under the path and
		// distributed locks held above. Ordering matters: the inline heal
		// copies above replicate the data at the OLD generation; this single
		// bump+write then stamps the NEW generation onto ALL replicas
		// (including freshly healed targets), so the heal loop needs no
		// per-target sidecar copy of its own.
		f.node.Mu.Lock()
		newGen := f.node.Meta.Gen + 1
		f.node.Meta.Gen = newGen
		f.node.Mu.Unlock()
		f.fs.writeSidecars(ctx, path, vfs.Sidecar{Gen: newGen, Writer: f.fs.NodeID}, backends)
	}

	// Unlock I/O: Open outside of lock (already done since we RUnlocked above)

	if req.Flags.IsReadOnly() {
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
			return nil, syscall.EIO
		}

		var lastErr error
		opened := false
		for _, bName := range candidates {
			b, ok := f.fs.Backends[bName]
			if !ok {
				logrus.Warnf("Backend %s listed for %s is not configured", bName, path)
				continue
			}
			sf, err := b.OpenFile(ctx, path, os.O_RDONLY, 0)
			if err != nil {
				logrus.Warnf("Read-only open of %s failed on backend %s: %v", path, bName, err)
				lastErr = err
				continue
			}
			h.backends[bName] = sf
			opened = true
			break
		}
		if !opened {
			if lastErr == nil {
				return nil, syscall.EIO
			}
			return nil, lastErr
		}
	} else {
		for _, bName := range backends {
			b := f.fs.Backends[bName]
			sf, err := b.OpenFile(ctx, path, int(req.Flags), 0)
			if err != nil {
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
	}

	return h, nil
}

type FileHandle struct {
	file     *File
	mu       sync.Mutex
	backends map[string]backend.File
	lock     *vfs.DistributedLock
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
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
		if err != nil && err != io.EOF {
			logrus.Warnf("Read failed on backend %s: %v. Retrying...", currentBackendName, err)

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

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if h.lock != nil && !h.lock.IsValid() {
		return syscall.EIO // Lock lost
	}

	h.mu.Lock()
	backends := make(map[string]backend.File, len(h.backends))
	for k, v := range h.backends {
		backends[k] = v
	}
	h.mu.Unlock()

	var mu sync.Mutex
	var successes int
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for name, sf := range backends {
		name, sf := name, sf
		g.Go(func() error {
			_, err := sf.WriteAt(gCtx, req.Data, req.Offset)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				logrus.Warnf("Write failed on backend %s: %v", name, err)
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
		if len(failures) > 0 {
			h.cleanupFailedBackends(failures)
		}
		return fmt.Errorf("could not reach write quorum: %d/%d (last error: %v)", successes, h.file.fs.WriteQuorum, lastErr)
	}

	// Remove failed backends from the handle and from VFS metadata
	if len(failures) > 0 {
		h.cleanupFailedBackends(failures)
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
		failed := false
		for _, fName := range failures {
			if bName == fName {
				failed = true
				break
			}
		}
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

func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return h.syncBackends(ctx)
}

func (h *FileHandle) syncBackends(ctx context.Context) error {
	if h.lock != nil && !h.lock.IsValid() {
		return syscall.EIO // Lock lost
	}

	h.mu.Lock()
	backends := make(map[string]backend.File, len(h.backends))
	for k, v := range h.backends {
		backends[k] = v
	}
	h.mu.Unlock()

	if len(backends) == 0 {
		return nil
	}

	var mu sync.Mutex
	var successes int
	var failures []string
	var lastErr error

	g, gCtx := errgroup.WithContext(ctx)
	for name, sf := range backends {
		name, sf := name, sf
		g.Go(func() error {
			err := sf.Sync(gCtx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				logrus.Warnf("Sync failed on backend %s: %v", name, err)
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
		return fmt.Errorf("could not reach write quorum during sync: %d/%d (last error: %v)", successes, h.file.fs.WriteQuorum, lastErr)
	}

	if len(failures) > 0 {
		h.cleanupFailedBackends(failures)
	}

	return nil
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// Dir.Create's abort paths release the handle before h.file is set, and
	// read-only handles were never registered; deregister tolerates both.
	if h.file != nil {
		h.file.fs.handles.deregister(h.file.node, h)
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
	return nil
}
