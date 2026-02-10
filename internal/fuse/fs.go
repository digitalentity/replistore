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
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type RepliFS struct {
	Cache             *vfs.Cache
	Backends          map[string]backend.Backend
	ReplicationFactor int
	Selector          vfs.BackendSelector
}

func (f *RepliFS) Root() (fs.Node, error) {
	return &Dir{fs: f, node: f.Cache.Root}, nil
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

	allBackendNames := make([]string, 0, len(d.fs.Backends))
	for name := range d.fs.Backends {
		allBackendNames = append(allBackendNames, name)
	}

	selectedBackends := d.fs.Selector.SelectForWrite(d.fs.ReplicationFactor, allBackendNames)

	if len(selectedBackends) == 0 {
		return nil, nil, syscall.EIO
	}

	path := parentPath + "/" + req.Name
	if parentPath == "" {
		path = req.Name
	}

	h := &FileHandle{
		backends: make(map[string]backend.File),
	}

	// Perform I/O outside of VFS lock to prevent deadlock
	var openErr error
	for _, bName := range selectedBackends {
		b := d.fs.Backends[bName]
		sf, err := b.OpenFile(path, os.O_CREATE|os.O_RDWR, req.Mode)
		if err != nil {
			openErr = err
			break
		}
		h.backends[bName] = sf
	}

	if openErr != nil {
		h.Close(ctx, nil)
		// Cleanup partial creates? For now just fail.
		return nil, nil, openErr
	}

	// Re-acquire lock to update VFS
	d.node.Mu.Lock()
	defer d.node.Mu.Unlock()

	// Check if someone else created it while we were doing I/O
	if _, ok := d.node.Children[req.Name]; ok {
		// Conflict!
		h.Close(ctx, nil)
		// Ideally we should delete the files we created on backends...
		return nil, nil, syscall.EEXIST
	}

	meta := vfs.Metadata{
		Name:     req.Name,
		Path:     path,
		Mode:     req.Mode,
		Backends: selectedBackends,
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

	// Directories are created on ALL backends to maintain tree structure
	var mu sync.Mutex
	var createdOn []string
	g, _ := errgroup.WithContext(ctx)

	for name, b := range d.fs.Backends {
		g.Go(func() error {
			err := b.Mkdir(path, req.Mode)
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
	g.Wait()

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

	g, _ := errgroup.WithContext(ctx)
	for _, bName := range backends {
		b := d.fs.Backends[bName]
		g.Go(func() error {
			err := b.Remove(path)
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

	// Unlock I/O: Open outside of lock (already done since we RUnlocked above)
	
	if req.Flags.IsReadOnly() {
		bName := f.fs.Selector.SelectForRead(f.node.Meta)
		if bName == "" {
			return nil, syscall.EIO
		}
		b := f.fs.Backends[bName]
		sf, err := b.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		h.backends[bName] = sf
	} else {
		for _, bName := range backends {
			b := f.fs.Backends[bName]
			sf, err := b.OpenFile(path, int(req.Flags), 0)
			if err != nil {
				h.Close(ctx, nil)
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

		n, err := sf.ReadAt(buf, req.Offset)
		if err != nil && err != io.EOF {
			logrus.Warnf("Read failed on backend %s: %v. Retrying...", currentBackendName, err)

			tried[currentBackendName] = true

			h.mu.Lock()
			// Close and remove the failed backend
			sf.Close()
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
		sf, err := b.OpenFile(path, os.O_RDONLY, 0)
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
	h.mu.Lock()
	backends := make(map[string]backend.File, len(h.backends))
	for k, v := range h.backends {
		backends[k] = v
	}
	h.mu.Unlock()

	g, _ := errgroup.WithContext(ctx)
	for name, sf := range backends {
		g.Go(func() error {
			_, err := sf.WriteAt(req.Data, req.Offset)
			if err != nil {
				return fmt.Errorf("backend %s: %w", name, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		// STRICT consistency: If any replica failed, return error.
		// This signals the application that the write was not fully replicated.
		return err
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

func (h *FileHandle) Close(ctx context.Context, req *fuse.FlushRequest) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	for _, sf := range h.backends {
		sf.Close()
	}
	h.backends = nil
	return nil
}