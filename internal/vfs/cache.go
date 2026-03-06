package vfs

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/sirupsen/logrus"
)

type Metadata struct {
	Name     string
	Path     string   // Relative path from share root
	Size     int64
	Mode     os.FileMode
	ModTime  time.Time
	IsDir    bool
	Backends []string // Names of backends containing this file
}

type Node struct {
	Meta     Metadata
	Children map[string]*Node
	Mu       sync.RWMutex
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
			Children: make(map[string]*Node),
		},
	}
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
				Meta:     newMeta,
				Children: make(map[string]*Node),
			}
			curr.Children[part] = next
		}
		
		if i == len(parts)-1 {
			isNewer := meta.ModTime.After(next.Meta.ModTime)
			isSameTime := meta.ModTime.Equal(next.Meta.ModTime)
			isLarger := meta.Size > next.Meta.Size
			isSameSize := meta.Size == next.Meta.Size

			if isNewer || (isSameTime && isLarger) {
				// We found a better/newer version. 
				// This backend becomes the sole winner for now.
				next.Meta.Size = meta.Size
				next.Meta.ModTime = meta.ModTime
				next.Meta.Backends = []string{backendName}
			} else if isSameTime && isSameSize {
				// This backend matches the current best version.
				found := false
				for _, b := range next.Meta.Backends {
					if b == backendName {
						found = true
						break
					}
				}
				if !found {
					next.Meta.Backends = append(next.Meta.Backends, backendName)
				}
			} else {
				// This backend is stale. We don't add it to the list.
				// If it was already there (e.g. from a previous sync),
				// the winner-reset logic above would have already handled it if we encountered the winner first.
				// However, if we encounter the stale one AFTER the winner in the same sync cycle,
				// we just ignore it.
			}
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
	for _, b := range backends {
		logrus.Debugf("Background syncing backend: %s", b.GetName())
		seenPaths := make(map[string]bool)
		err := b.Walk(ctx, "", func(path string, info backend.FileInfo) error {
			seenPaths[path] = true
			c.Upsert(path, Metadata{
				Name:    info.Name,
				Size:    info.Size,
				Mode:    info.Mode,
				ModTime: info.ModTime,
				IsDir:   info.IsDir,
			}, b.GetName())
			return nil
		})
		if err != nil {
			logrus.Errorf("Background sync error on %s: %v", b.GetName(), err)
			continue
		}
		c.Reconcile(b.GetName(), seenPaths)
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
	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func(smb backend.Backend) {
			defer wg.Done()
			logrus.Infof("Scanning backend: %s", smb.GetName())
			err := smb.Walk(ctx, "", func(path string, info backend.FileInfo) error {
				c.Upsert(path, Metadata{
					Name:    info.Name,
					Size:    info.Size,
					Mode:    info.Mode,
					ModTime: info.ModTime,
					IsDir:   info.IsDir,
				}, smb.GetName())
				return nil
			})
			if err != nil {
				logrus.Errorf("Error scanning backend %s: %v", smb.GetName(), err)
			} else {
				logrus.Infof("Finished scanning backend: %s", smb.GetName())
			}
		}(b)
	}
	wg.Wait()
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