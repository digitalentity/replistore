package fuse

import (
	"context"
	"io"
	"os"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/sirupsen/logrus"
)

type RepliFS struct {
	Cache             *vfs.Cache
	Backends          map[string]backend.Backend
	ReplicationFactor int
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
	defer d.node.Mu.Unlock()

	if _, ok := d.node.Children[req.Name]; ok {
		return nil, nil, syscall.EEXIST
	}

	// Select RF backends (simple: first RF available)
	var selectedBackends []string
	for name := range d.fs.Backends {
		selectedBackends = append(selectedBackends, name)
		if len(selectedBackends) >= d.fs.ReplicationFactor {
			break
		}
	}

	if len(selectedBackends) == 0 {
		return nil, nil, syscall.EIO
	}

	path := d.node.Meta.Path + "/" + req.Name
	if d.node.Meta.Path == "" {
		path = req.Name
	}

	h := &FileHandle{
		backends: make(map[string]backend.File),
	}

	for _, bName := range selectedBackends {
		b := d.fs.Backends[bName]
		sf, err := b.OpenFile(path, os.O_CREATE|os.O_RDWR, req.Mode)
		if err != nil {
			h.Close(ctx, nil)
			return nil, nil, err
		}
		h.backends[bName] = sf
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
	defer d.node.Mu.Unlock()

	if _, ok := d.node.Children[req.Name]; ok {
		return nil, syscall.EEXIST
	}

	path := d.node.Meta.Path + "/" + req.Name
	if d.node.Meta.Path == "" {
		path = req.Name
	}

	// Directories are created on ALL backends to maintain tree structure
	var createdOn []string
	for name, b := range d.fs.Backends {
		err := b.Mkdir(path, req.Mode)
		if err != nil {
			logrus.Warnf("Failed to create directory %s on %s: %v", path, name, err)
			continue
		}
		createdOn = append(createdOn, name)
	}

	if len(createdOn) == 0 {
		return nil, syscall.EIO
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
	defer child.Mu.Unlock()

	path := child.Meta.Path
	var lastErr error
	for _, bName := range child.Meta.Backends {
		b := d.fs.Backends[bName]
		err := b.Remove(path)
		if err != nil {
			lastErr = err
			logrus.Errorf("Failed to remove %s from %s: %v", path, bName, err)
		}
	}

	if lastErr != nil {
		return lastErr
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
	backends := f.node.Meta.Backends
	path := f.node.Meta.Path
	f.node.Mu.RUnlock()

	if len(backends) == 0 {
		return nil, syscall.EIO
	}

	h := &FileHandle{
		file:     f,
		backends: make(map[string]backend.File),
	}

	// For reading, we only need one backend. For writing, we need all.
	// To keep it simple, if it's WriteOnly or ReadWrite, open all.
	// If ReadOnly, open one.
	
	if req.Flags.IsReadOnly() {
		b := f.fs.Backends[backends[0]]
		sf, err := b.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		h.backends[backends[0]] = sf
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
	backends map[string]backend.File
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// Pick any open backend
	var sf backend.File
	for _, f := range h.backends {
		sf = f
		break
	}
	if sf == nil {
		return syscall.EIO
	}

	buf := make([]byte, req.Size)
	n, err := sf.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = buf[:n]
	return nil
}

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	var wg sync.WaitGroup
	var lastErr error
	var mu sync.Mutex

	for _, sf := range h.backends {
		wg.Add(1)
		go func(f backend.File) {
			defer wg.Done()
			_, err := f.WriteAt(req.Data, req.Offset)
			if err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()
			}
		}(sf)
	}
	wg.Wait()

	if lastErr != nil {
		return lastErr
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
	for _, sf := range h.backends {
		sf.Close()
	}
	return nil
}
