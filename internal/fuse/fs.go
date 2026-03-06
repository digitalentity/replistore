package fuse

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

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
	d.node.Mu.RUnlock()

	if !ok {
		return nil, syscall.ENOENT
	}

	if child.Meta.IsDir {
		return &Dir{fs: d.fs, node: child}, nil
	}
	return &File{fs: d.fs, node: child}, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.node.Mu.RLock()
	defer d.node.Mu.RUnlock()

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

	// Distributed Locking
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
	g, gCtx := errgroup.WithContext(ctx)

	for _, bName := range selectedBackends {
		bName := bName // capture for goroutine
		g.Go(func() error {
			b := d.fs.Backends[bName]
			sf, err := b.OpenFile(gCtx, path, os.O_CREATE|os.O_RDWR, req.Mode)
			if err != nil {
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
		return nil, nil, fmt.Errorf("could not reach write quorum: %d/%d", len(successfulBackends), d.fs.WriteQuorum)
	}

	// Re-acquire lock to update VFS
	d.node.Mu.Lock()
	defer d.node.Mu.Unlock()

	// Check if someone else created it while we were doing I/O
	if _, ok := d.node.Children[req.Name]; ok {
		// Conflict!
		_ = h.Release(ctx, nil)
		return nil, nil, syscall.EEXIST
	}

	meta := vfs.Metadata{
		Name:     req.Name,
		Path:     path,
		Mode:     req.Mode,
		Backends: successfulBackends,
	}
	
	child := &vfs.Node{
		Meta:     meta,
		Children: make(map[string]*vfs.Node),
	}
	d.node.Children[req.Name] = child
	h.file = &File{fs: d.fs, node: child}

	return h.file, h, nil
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

	// Distributed Locking
	lock, err := d.fs.acquireLock(ctx, path)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		defer lock.Release()
	}

	// Directories are created on ALL backends to maintain tree structure
	var mu sync.Mutex
	var createdOn []string
	g, gCtx := errgroup.WithContext(ctx)

	for name, b := range d.fs.Backends {
		name, b := name, b
		g.Go(func() error {
			err := b.Mkdir(gCtx, path, req.Mode)
			if err != nil {
				logrus.Warnf("Failed to create directory %s on %s: %v", path, name, err)
				return nil // Don't fail the whole operation if one backend fails
			}
			mu.Lock()
			createdOn = append(createdOn, name)
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	if len(createdOn) == 0 {
		return nil, syscall.EIO
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
		Backends: createdOn,
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
	backends := make([]string, len(child.Meta.Backends))
	copy(backends, child.Meta.Backends)
	child.Mu.Unlock()

	// Distributed Locking
	lock, err := d.fs.acquireLock(ctx, path)
	if err != nil {
		return err
	}
	if lock != nil {
		defer lock.Release()
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, bName := range backends {
		bName := bName
		b := d.fs.Backends[bName]
		g.Go(func() error {
			err := b.Remove(gCtx, path)
			if err != nil {
				logrus.Errorf("Failed to remove %s from %s: %v", path, bName, err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
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
	backends := make([]string, len(sourceNode.Meta.Backends))
	copy(backends, sourceNode.Meta.Backends)
	sourceNode.Mu.RUnlock()

	targetDir.node.Mu.RLock()
	targetParentPath := targetDir.node.Meta.Path
	targetDir.node.Mu.RUnlock()

	newPath := targetParentPath + "/" + req.NewName
	if targetParentPath == "" {
		newPath = req.NewName
	}

	// Distributed Locking for BOTH paths
	oldLock, err := d.fs.acquireLock(ctx, oldPath)
	if err != nil {
		return err
	}
	if oldLock != nil {
		defer oldLock.Release()
	}

	newLock, err := d.fs.acquireLock(ctx, newPath)
	if err != nil {
		return err
	}
	if newLock != nil {
		defer newLock.Release()
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
			if err != nil {
				logrus.Warnf("Failed to rename %s to %s on %s: %v", oldPath, newPath, bName, err)
				failures = append(failures, bName)
				lastErr = err
			} else {
				successful = append(successful, bName)
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

	// Update Cache
	if !d.fs.Cache.Rename(oldPath, newPath) {
		// This should not happen if our VFS logic is correct
		return syscall.EIO
	}

	// If some backends failed, update the node's backend list
	if len(failures) > 0 {
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

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// Node fsync is tricky without a handle.
	// For now, we rely on Handle.Fsync which is what most apps use.
	// We can implement a "sync on all backends" in the future if needed.
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
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

	// Distributed Locking for writes
	if !req.Flags.IsReadOnly() {
		lock, err := f.fs.acquireLock(ctx, path)
		if err != nil {
			return nil, err
		}
		h.lock = lock
	}

	// Unlock I/O: Open outside of lock (already done since we RUnlocked above)
	
	if req.Flags.IsReadOnly() {
		bName := f.fs.Selector.SelectForRead(f.node.Meta)
		if bName == "" {
			return nil, syscall.EIO
		}
		b := f.fs.Backends[bName]
		sf, err := b.OpenFile(ctx, path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		h.backends[bName] = sf
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
		return fmt.Errorf("could not reach write quorum: %d/%d (last error: %v)", successes, h.file.fs.WriteQuorum, lastErr)
	}

	// Remove failed backends from the handle and from VFS metadata
	if len(failures) > 0 {
		h.cleanupFailedBackends(failures)
	}

	resp.Size = len(req.Data)

	// Update cache size
	h.file.node.Mu.Lock()
	newSize := req.Offset + int64(len(req.Data))
	if newSize > h.file.node.Meta.Size {
		h.file.node.Meta.Size = newSize
	}
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
	h.file.node.Mu.Unlock()
}

func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return h.syncBackends(ctx)
}

func (h *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
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
