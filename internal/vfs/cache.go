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
			// Update backend list for the target node
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
			// Update size/mtime to the latest/largest
			if meta.Size > next.Meta.Size {
				next.Meta.Size = meta.Size
			}
			if meta.ModTime.After(next.Meta.ModTime) {
				next.Meta.ModTime = meta.ModTime
			}
		}
		
		parent := curr
		curr = next
		parent.Mu.Unlock()
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