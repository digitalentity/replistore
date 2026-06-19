package fuse

import (
	"sync"

	"github.com/digitalentity/replistore/internal/vfs"
)

// handleRegistry tracks the open write handles for each cache node. bazil
// dispatches Fsync to the node (fs.NodeFsyncer), and Dir.Lookup builds a new
// File value per lookup, so the registry is keyed by the stable *vfs.Node
// and lives on RepliFS. Node-level Fsync uses it to sync the backend files
// the active write handles actually hold instead of opening fresh ones.
//
// The zero value is ready to use; the map is initialized lazily under mu so
// handleRegistry can be embedded by value in a struct built with a struct
// literal (same pattern as pathLocks).
type handleRegistry struct {
	mu      sync.Mutex
	handles map[*vfs.Node][]*FileHandle
}

// register records h as an open write handle for node.
func (r *handleRegistry) register(node *vfs.Node, h *FileHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handles == nil {
		r.handles = make(map[*vfs.Node][]*FileHandle)
	}
	r.handles[node] = append(r.handles[node], h)
}

// deregister removes h from node's handle list. It is a no-op if h was never
// registered. Empty lists are deleted so the map size is bounded by the
// number of nodes with open write handles.
func (r *handleRegistry) deregister(node *vfs.Node, h *FileHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := r.handles[node]
	for i, cand := range hs {
		if cand == h {
			hs = append(hs[:i], hs[i+1:]...)

			break
		}
	}
	if len(hs) == 0 {
		delete(r.handles, node)
	} else {
		r.handles[node] = hs
	}
}

// hasOpenWriteHandle reports whether any write handle is currently open for
// node. Cheaper than forNode: no snapshot is allocated.
func (r *handleRegistry) hasOpenWriteHandle(node *vfs.Node) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.handles[node]) > 0
}

// forNode returns a snapshot of the write handles currently open for node.
func (r *handleRegistry) forNode(node *vfs.Node) []*FileHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := r.handles[node]
	if len(hs) == 0 {
		return nil
	}
	out := make([]*FileHandle, len(hs))
	copy(out, hs)

	return out
}
