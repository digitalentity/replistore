package vfs

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// ErrUnavailable indicates that no backend was able to give an authoritative
// answer (e.g. all backends were unreachable), so the result must not be
// treated as a definitive "does not exist".
var ErrUnavailable = errors.New("vfs: no backend available for authoritative answer")

// ReservedDir is the directory on each backend that holds RepliStore's
// cluster-internal state (peer registry, version metadata). It is never
// exposed through the unified namespace.
const ReservedDir = ".replistore"

// IsReservedPath reports whether path (relative, slash-separated, as used
// throughout the cache) is the reserved RepliStore state directory or
// anything inside it.
func IsReservedPath(path string) bool {
	return path == ReservedDir || strings.HasPrefix(path, ReservedDir+"/")
}

type Metadata struct {
	Name     string
	Path     string // Relative path from share root
	Size     int64
	Mode     os.FileMode
	ModTime  time.Time
	IsDir    bool
	Backends []string // Names of backends containing this file
}

type Node struct {
	Meta         Metadata
	Children     map[string]*Node
	FullyIndexed bool // true if this directory has been fully scanned from backends
	Mu           sync.RWMutex
}

type Cache struct {
	Root *Node
	Mu   sync.RWMutex
}

func NewCache() *Cache {
	return &Cache{
		Root: &Node{
			Meta: Metadata{
				Name:  "/",
				Path:  "",
				IsDir: true,
				Mode:  os.ModeDir | 0755,
			},
			Children:     make(map[string]*Node),
			FullyIndexed: false,
		},
	}
}

// mergeMeta reconciles incoming metadata (present on incomingBackends) into
// existing.
//
// Files use last-writer-wins semantics: a strictly newer ModTime, or an equal
// ModTime with a larger Size, replaces the backend list; an identical version
// unions the backend lists; a stale version is ignored.
//
// Directories are presence-sets, not LWW registers: a directory's ModTime
// legitimately differs per backend (it changes whenever children change), so
// backend names are always unioned, and ModTime is only bumped forward for
// display purposes — never used to replace the backend list.
//
// Type conflict rule: if the path is a directory on at least one backend and
// a file on another, the directory wins (directory presence masks the file).
// The node becomes a directory and the file's backend is still unioned into
// the presence set, so namespace operations fan out everywhere the path is
// occupied. A warning is logged per occurrence.
func mergeMeta(existing *Metadata, incoming Metadata, incomingBackends []string) {
	if existing.IsDir || incoming.IsDir {
		if existing.IsDir != incoming.IsDir {
			p := existing.Path
			if p == "" {
				p = incoming.Path
			}
			if p == "" {
				p = existing.Name
			}
			logrus.Warnf("vfs: type conflict at %q: file on some backends, directory on others; treating as directory", p)
		}
		if !existing.IsDir {
			// Directory wins the type conflict: convert the node to a directory.
			existing.IsDir = true
			existing.Size = 0
			existing.Mode = incoming.Mode
		}
		existing.Backends = unionBackends(existing.Backends, incomingBackends)
		if incoming.ModTime.After(existing.ModTime) {
			existing.ModTime = incoming.ModTime
		}
		return
	}

	isNewer := incoming.ModTime.After(existing.ModTime)
	isSameTime := incoming.ModTime.Equal(existing.ModTime)
	isLarger := incoming.Size > existing.Size
	isSameSize := incoming.Size == existing.Size

	switch {
	case isNewer || (isSameTime && isLarger):
		// A better/newer version: its backends replace the list.
		existing.Size = incoming.Size
		existing.ModTime = incoming.ModTime
		existing.Backends = append([]string(nil), incomingBackends...)
	case isSameTime && isSameSize:
		// Same version: union the backends.
		existing.Backends = unionBackends(existing.Backends, incomingBackends)
	default:
		// Stale version: ignored.
	}
}

// unionBackends returns the deduplicated union of two backend name lists,
// preserving first-seen order.
func unionBackends(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	res := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, name := range list {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				res = append(res, name)
			}
		}
	}
	return res
}

func (c *Cache) Upsert(path string, meta Metadata, backendName string) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	parts := splitPath(path)
	curr := c.Root

	for i, part := range parts {
		curr.Mu.Lock()
		next, ok := curr.Children[part]
		if !ok {
			// Create intermediate directory if it doesn't exist
			isLast := i == len(parts)-1
			newMeta := Metadata{
				Name:  part,
				Path:  strings.Join(parts[:i+1], "/"),
				IsDir: true,
				Mode:  os.ModeDir | 0755,
			}
			if isLast {
				newMeta = meta
				newMeta.Path = path
			}
			next = &Node{
				Meta:         newMeta,
				Children:     make(map[string]*Node),
				FullyIndexed: false,
			}
			curr.Children[part] = next
		}

		if i == len(parts)-1 {
			mergeMeta(&next.Meta, meta, []string{backendName})
		}

		parent := curr
		curr = next
		parent.Mu.Unlock()
	}
}

// StartSync starts the background synchronization loop.
func (c *Cache) StartSync(ctx context.Context, backends []backend.Backend, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.syncAll(ctx, backends)
			}
		}
	}()
}

func (c *Cache) syncAll(ctx context.Context, backends []backend.Backend) {
	// globalState entries carry their backend presence in Meta.Backends.
	globalState := make(map[string]*Metadata)
	var stateMu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
		b := b
		g.Go(func() error {
			logrus.Debugf("Background syncing backend: %s", b.GetName())
			seenPaths := make(map[string]bool)
			err := b.Walk(gCtx, "", func(path string, info backend.FileInfo) error {
				if IsReservedPath(path) {
					// Cluster-internal state is never indexed, so it must not
					// appear in seenPaths either (Reconcile only sweeps paths
					// that are in the cache).
					return nil
				}
				seenPaths[path] = true
				meta := Metadata{
					Name:    info.Name,
					Size:    info.Size,
					Mode:    info.Mode,
					ModTime: info.ModTime,
					IsDir:   info.IsDir,
				}

				stateMu.Lock()
				defer stateMu.Unlock()

				s, ok := globalState[path]
				if !ok {
					m := meta
					m.Backends = []string{b.GetName()}
					globalState[path] = &m
					return nil
				}

				mergeMeta(s, meta, []string{b.GetName()})
				return nil
			})
			if err != nil {
				logrus.Errorf("Background sync error on %s: %v", b.GetName(), err)
				return nil // Don't fail other backends
			}
			c.Reconcile(b.GetName(), seenPaths)
			return nil
		})
	}
	_ = g.Wait()

	// Update cache with global winners
	for path, s := range globalState {
		c.UpsertMulti(path, *s, s.Backends)
	}
	c.markAllIndexed(c.Root)
}

func (c *Cache) UpsertMulti(path string, meta Metadata, backends []string) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	parts := splitPath(path)
	curr := c.Root

	for i, part := range parts {
		curr.Mu.Lock()
		next, ok := curr.Children[part]
		if !ok {
			isLast := i == len(parts)-1
			newMeta := Metadata{
				Name:  part,
				Path:  strings.Join(parts[:i+1], "/"),
				IsDir: true,
				Mode:  os.ModeDir | 0755,
			}
			if isLast {
				newMeta = meta
				newMeta.Path = path
			}
			next = &Node{
				Meta:         newMeta,
				Children:     make(map[string]*Node),
				FullyIndexed: false,
			}
			curr.Children[part] = next
		}

		if i == len(parts)-1 {
			mergeMeta(&next.Meta, meta, backends)
		}

		parent := curr
		curr = next
		parent.Mu.Unlock()
	}
}

func (c *Cache) Reconcile(backendName string, seenPaths map[string]bool) {
	c.sweep(c.Root, "", backendName, seenPaths)
}

func (c *Cache) sweep(node *Node, path string, backendName string, seenPaths map[string]bool) {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	for name, child := range node.Children {
		childPath := name
		if path != "" {
			childPath = path + "/" + name
		}

		c.sweep(child, childPath, backendName, seenPaths)

		child.Mu.Lock()
		// If this node was NOT seen on this backend, remove this backend from its metadata
		if !seenPaths[childPath] {
			newBackends := make([]string, 0, len(child.Meta.Backends))
			for _, b := range child.Meta.Backends {
				if b != backendName {
					newBackends = append(newBackends, b)
				}
			}
			child.Meta.Backends = newBackends
		}

		// Prune node if it no longer has any backends and no children
		if len(child.Meta.Backends) == 0 && len(child.Children) == 0 {
			delete(node.Children, name)
		}
		child.Mu.Unlock()
	}
}

func splitPath(path string) []string {
	if path == "" || path == "." || path == "/" {
		return nil
	}
	var res []string
	for _, s := range append([]string{}, split(path, "/")...) {
		if s != "" {
			res = append(res, s)
		}
	}
	return res
}

func split(s, sep string) []string {
	var res []string
	start := 0
	for i := 0; i < len(s); i++ {
		if string(s[i]) == sep {
			res = append(res, s[start:i])
			start = i + 1
		}
	}
	res = append(res, s[start:])
	return res
}

func (c *Cache) Rename(oldPath, newPath string) bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	oldParts := splitPath(oldPath)
	newParts := splitPath(newPath)
	if len(oldParts) == 0 || len(newParts) == 0 {
		return false
	}

	// Find source node and its parent
	sourceParentPath := strings.Join(oldParts[:len(oldParts)-1], "/")
	sourceNodeName := oldParts[len(oldParts)-1]
	sourceParent, ok := c.getWithLock(sourceParentPath)
	if !ok {
		return false
	}

	sourceParent.Mu.Lock()
	node, ok := sourceParent.Children[sourceNodeName]
	if !ok {
		sourceParent.Mu.Unlock()
		return false
	}
	delete(sourceParent.Children, sourceNodeName)
	sourceParent.Mu.Unlock()

	// Find/Create destination parent
	destParentPath := strings.Join(newParts[:len(newParts)-1], "/")
	destNodeName := newParts[len(newParts)-1]

	// Create destination parents if they don't exist
	destParent := c.ensurePathWithLock(destParentPath)

	destParent.Mu.Lock()
	// Update node metadata
	node.Mu.Lock()
	node.Meta.Name = destNodeName
	node.Meta.Path = newPath
	if node.Meta.IsDir {
		c.updatePaths(node, newPath)
	}
	node.Mu.Unlock()

	destParent.Children[destNodeName] = node
	destParent.Mu.Unlock()

	return true
}

func (c *Cache) getWithLock(path string) (*Node, bool) {
	parts := splitPath(path)
	curr := c.Root

	for _, part := range parts {
		curr.Mu.RLock()
		next, ok := curr.Children[part]
		curr.Mu.RUnlock()
		if !ok {
			return nil, false
		}
		curr = next
	}
	return curr, true
}

func (c *Cache) ensurePathWithLock(path string) *Node {
	parts := splitPath(path)
	curr := c.Root

	for i, part := range parts {
		curr.Mu.Lock()
		next, ok := curr.Children[part]
		if !ok {
			next = &Node{
				Meta: Metadata{
					Name:  part,
					Path:  strings.Join(parts[:i+1], "/"),
					IsDir: true,
					Mode:  os.ModeDir | 0755,
				},
				Children:     make(map[string]*Node),
				FullyIndexed: false,
			}
			curr.Children[part] = next
		}
		parent := curr
		curr = next
		parent.Mu.Unlock()
	}
	return curr
}

// FetchEntry performs a synchronous, parallel Stat call to backends for a missing path.
func (c *Cache) FetchEntry(ctx context.Context, path string, backends []backend.Backend) (*Node, error) {
	if path == "" || path == "/" {
		return c.Root, nil
	}
	if IsReservedPath(path) {
		// Cluster-internal state is invisible in the unified namespace.
		return nil, os.ErrNotExist
	}

	var stateMu sync.Mutex
	var bestMeta Metadata // backend presence tracked in bestMeta.Backends
	var found bool
	var notExistCount int

	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
		b := b
		g.Go(func() error {
			info, err := b.Stat(gCtx, path)
			if err != nil {
				if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
					// Definitive answer: this backend doesn't have it.
					stateMu.Lock()
					notExistCount++
					stateMu.Unlock()
				}
				return nil // backend doesn't have it or errored
			}

			meta := Metadata{
				Name:    info.Name,
				Size:    info.Size,
				Mode:    info.Mode,
				ModTime: info.ModTime,
				IsDir:   info.IsDir,
				Path:    path,
			}

			stateMu.Lock()
			defer stateMu.Unlock()

			if !found {
				bestMeta = meta
				bestMeta.Backends = []string{b.GetName()}
				found = true
				return nil
			}

			mergeMeta(&bestMeta, meta, []string{b.GetName()})
			return nil
		})
	}
	_ = g.Wait()

	if !found {
		if notExistCount > 0 {
			// At least one backend gave a definitive "not found".
			return nil, os.ErrNotExist
		}
		// Every backend errored non-definitively (or there were no backends):
		// we have no authoritative answer.
		return nil, ErrUnavailable
	}

	c.UpsertMulti(path, bestMeta, bestMeta.Backends)
	node, ok := c.Get(path)
	if !ok {
		return nil, os.ErrNotExist
	}
	return node, nil
}

// FetchDir performs a synchronous, parallel ReadDir call to backends for a partial directory.
func (c *Cache) FetchDir(ctx context.Context, path string, backends []backend.Backend) error {
	node, ok := c.Get(path)
	if !ok || !node.Meta.IsDir {
		return os.ErrNotExist
	}

	var stateMu sync.Mutex
	var definitiveCount int

	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
		b := b
		g.Go(func() error {
			entries, err := b.ReadDir(gCtx, path)
			if err != nil {
				if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
					// Definitive answer: this directory doesn't exist on this
					// backend. Counts towards indexing completeness, but
					// there are no entries to add.
					stateMu.Lock()
					definitiveCount++
					stateMu.Unlock()
				}
				return nil
			}
			stateMu.Lock()
			definitiveCount++
			stateMu.Unlock()
			for _, info := range entries {
				if path == "" && info.Name == ReservedDir {
					// Cluster-internal state is invisible in the unified
					// namespace. Only the root can contain the reserved dir.
					continue
				}
				childPath := strings.TrimPrefix(path+"/"+info.Name, "/")
				c.Upsert(childPath, Metadata{
					Name:    info.Name,
					Size:    info.Size,
					Mode:    info.Mode,
					ModTime: info.ModTime,
					IsDir:   info.IsDir,
				}, b.GetName())
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if definitiveCount == 0 {
		// No backend gave a definitive answer; don't pretend the listing is
		// authoritative.
		return ErrUnavailable
	}

	node.Mu.Lock()
	node.FullyIndexed = true
	node.Mu.Unlock()
	return nil
}

func (c *Cache) updatePaths(node *Node, newPath string) {
	// Assumes node.Mu is locked or we are in a safe context
	for name, child := range node.Children {
		child.Mu.Lock()
		child.Meta.Path = newPath + "/" + name
		if child.Meta.IsDir {
			c.updatePaths(child, child.Meta.Path)
		}
		child.Mu.Unlock()
	}
}

func (c *Cache) Get(path string) (*Node, bool) {
	parts := splitPath(path)
	curr := c.Root

	for _, part := range parts {
		curr.Mu.RLock()
		next, ok := curr.Children[part]
		curr.Mu.RUnlock()
		if !ok {
			return nil, false
		}
		curr = next
	}
	return curr, true
}

func (c *Cache) Warmup(ctx context.Context, backends []backend.Backend) {
	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
		b := b
		g.Go(func() error {
			logrus.Infof("Scanning backend: %s", b.GetName())
			err := b.Walk(gCtx, "", func(path string, info backend.FileInfo) error {
				if IsReservedPath(path) {
					// Cluster-internal state is never indexed.
					return nil
				}
				c.Upsert(path, Metadata{
					Name:    info.Name,
					Size:    info.Size,
					Mode:    info.Mode,
					ModTime: info.ModTime,
					IsDir:   info.IsDir,
				}, b.GetName())

				// If it's a directory, ensure it exists in the cache nodes we just walked
				// and mark as FullyIndexed = false initially if we want, but since we are walking
				// the whole tree, we can mark them FullyIndexed when the walk for THIS backend is done.
				// Actually, we should mark as FullyIndexed only after WE ARE SURE we have the full view.
				return nil
			})
			if err != nil {
				logrus.Errorf("Error scanning backend %s: %v", b.GetName(), err)
			} else {
				logrus.Infof("Finished scanning backend: %s", b.GetName())
			}
			return nil
		})
	}
	_ = g.Wait()

	// Mark all directories as FullyIndexed once we finished the whole scan
	c.markAllIndexed(c.Root)
}

func (c *Cache) markAllIndexed(node *Node) {
	node.Mu.Lock()
	if node.Meta.IsDir {
		node.FullyIndexed = true
	}
	for _, child := range node.Children {
		c.markAllIndexed(child)
	}
	node.Mu.Unlock()
}

// FindDegraded returns all file nodes that have fewer backends than the required replication factor.
func (c *Cache) FindDegraded(rf int) []*Node {
	var degraded []*Node
	c.walk(c.Root, func(n *Node) {
		n.Mu.RLock()
		if !n.Meta.IsDir && len(n.Meta.Backends) > 0 && len(n.Meta.Backends) < rf {
			degraded = append(degraded, n)
		}
		n.Mu.RUnlock()
	})
	return degraded
}

func (c *Cache) walk(node *Node, fn func(*Node)) {
	fn(node)
	node.Mu.RLock()
	defer node.Mu.RUnlock()
	for _, child := range node.Children {
		c.walk(child, fn)
	}
}
