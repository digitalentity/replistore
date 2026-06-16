// Package vfs implements the virtual filesystem cache, replication logic, and selector components.
package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	Gen      int64    // version generation from the sidecar; 0 = legacy/unknown
}

type Node struct {
	Meta         Metadata
	Children     map[string]*Node
	FullyIndexed bool // true if this directory has been fully scanned from backends
	Mu           sync.RWMutex
	OpenHandles  int32     // atomic count of open file handles (read or write)
	LastUpdated  time.Time // timestamp when this cache entry was last created or updated
}

type Cache struct {
	Root           *Node
	LastReconciled time.Time
	Mu             sync.RWMutex
}

func (c *Cache) logger() *logrus.Entry {
	return logrus.WithField("component", "vfs")
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
			LastUpdated:  time.Now(),
		},
	}
}

// mergeMeta reconciles incoming metadata (present on incomingBackends) into
// existing.
//
// File merge rules (generation-aware). Generations are comparable only when
// both sides carry one (both Gen > 0) or neither does (both Gen == 0, legacy
// replicas):
//
//  1. Higher Gen wins outright: its meta and backend set replace the existing
//     ones, regardless of ModTime/Size. A lower Gen is ignored. (Server-
//     stamped mtimes are untrustworthy version vectors — REVIEW C4 — so a
//     skewed clock can no longer promote a stale replica.)
//  2. Equal Gen, equal Size: the same version observed on different backends —
//     union the backend sets and keep the newest ModTime for display only.
//     This kills post-write churn: replicated writes leave equal-gen
//     equal-size replicas with divergent server-stamped mtimes, which must
//     merge, not split. With both sides at Gen 0 this also merges
//     same-size-different-mtime legacy replicas (accepted C4 mitigation).
//  3. Equal Gen, different Size: fall back to (ModTime, Size) last-writer-wins
//     — a strictly newer ModTime, or an equal ModTime with a larger Size,
//     replaces the backend set; a stale version is ignored. (Crash mid-write;
//     checksums will improve this later.)
//
// Mixed knowledge (exactly one side has Gen 0): a Gen-0 meta is usually a
// report taken without sidecar knowledge (Warmup/FetchDir/Upsert see only
// FileInfo), not proof of an older version, so generations cannot be
// compared. The legacy (ModTime, Size) rules of case 3 (including the
// same-version union on equal ModTime and Size) decide the outcome, but the
// node's stored Gen is never downgraded: the result keeps the maximum of the
// two Gens.
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
// mergeDirMeta merges a node where at least one side is a directory. Directory
// presence masks files (the directory wins any type conflict), backend names
// are unioned into the presence set, and ModTime is only bumped forward for
// display. See mergeMeta for the full rationale.
func mergeDirMeta(existing *Metadata, incoming Metadata, incomingBackends []string) {
	if existing.IsDir != incoming.IsDir {
		p := existing.Path
		if p == "" {
			p = incoming.Path
		}
		if p == "" {
			p = existing.Name
		}
		logrus.WithField("component", "vfs").WithField("path", p).Warn("Type conflict: file on some backends, directory on others; treating as directory")
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
}

func mergeMeta(existing *Metadata, incoming Metadata, incomingBackends []string) {
	if existing.IsDir || incoming.IsDir {
		mergeDirMeta(existing, incoming, incomingBackends)
		return
	}

	maxGen := max(incoming.Gen, existing.Gen)

	// Generations are comparable only when both sides have one (or neither
	// does); a lone Gen 0 just means "observed without sidecar knowledge".
	if (existing.Gen > 0) == (incoming.Gen > 0) {
		switch {
		case incoming.Gen > existing.Gen:
			// Rule 1: higher generation wins outright.
			existing.Size = incoming.Size
			existing.ModTime = incoming.ModTime
			existing.Gen = incoming.Gen
			existing.Backends = append([]string(nil), incomingBackends...)
			return
		case incoming.Gen < existing.Gen:
			// Rule 1: lower generation is ignored.
			return
		case incoming.Size == existing.Size:
			// Rule 2: same version on more backends; newest mtime for display.
			existing.Backends = unionBackends(existing.Backends, incomingBackends)
			if incoming.ModTime.After(existing.ModTime) {
				existing.ModTime = incoming.ModTime
			}
			return
		}
		// Rule 3: equal Gen, different Size — fall through to (mtime, size) LWW.
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
	// Never downgrade the node's stored generation (mixed-knowledge rule).
	existing.Gen = maxGen
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
				LastUpdated:  time.Now(),
			}
			curr.Children[part] = next
		}

		if i == len(parts)-1 {
			mergeMeta(&next.Meta, meta, []string{backendName})
			next.LastUpdated = time.Now()
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

// syncAll performs one full reconciliation pass over all backends in three
// phases:
//
//	A. Walk every backend in parallel, collecting per-backend metadata
//	   (presence only — walks see FileInfo, never sidecars). Alongside, the
//	   tombstone trees of all backends are gathered (cost proportional to
//	   live tombstones).
//	B. Fold the per-backend candidates into one winning Metadata per path.
//	   Sidecars are read only for genuine file conflicts (different sizes)
//	   or tombstoned paths, so the steady-state pass costs zero sidecar
//	   reads. Candidates at or below a path's tombstone generation are
//	   zombies and are dropped; a path with no surviving candidate is dead.
//	C. Upsert the resolved metadata (including its Gen) into the cache and
//	   evict dead paths. The eviction is explicit because a dead path's data
//	   still exists on the zombie backend, so Reconcile's per-backend sweep
//	   (which only removes paths that vanished from a backend) never fires.
func (c *Cache) syncAll(ctx context.Context, backends []backend.Backend) {
	tombstones := GatherTombstones(ctx, backends)
	perBackend := c.walkBackends(ctx, backends)
	resolved, dead := resolveGlobalState(ctx, perBackend, backendsByName(backends), tombstones)

	for path, meta := range resolved {
		c.UpsertMulti(path, *meta, meta.Backends)
	}
	for _, path := range dead {
		c.evict(path)
	}
	c.markAllIndexed(c.Root)

	c.Mu.Lock()
	c.LastReconciled = time.Now()
	c.Mu.Unlock()
}

// evict unlinks the node at path from its parent. Used when sync determines a
// path is dead (every replica is at or below the path's tombstone generation):
// Reconcile's per-backend sweep only removes paths that vanished from a
// backend, and a zombie replica is still physically present, so the eviction
// must be explicit. Evicting an uncached or root path is a no-op.
func (c *Cache) evict(path string) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return
	}

	c.Mu.Lock()
	defer c.Mu.Unlock()

	parent, ok := c.getWithLock(strings.Join(parts[:len(parts)-1], "/"))
	if !ok {
		return
	}
	parent.Mu.Lock()
	delete(parent.Children, parts[len(parts)-1])
	parent.Mu.Unlock()
}

// listTombstones walks backend b's metadata tree and returns data path →
// tombstone generation for every document that carries a deletion marker. The
// data path is read from the document itself (the hash key is one-way). A
// missing metadata tree means no recorded deletions (empty map); other walk
// errors only log, keeping partial results. Because sidecars and tombstones
// share one tree, the cost is proportional to the total number of metadata
// documents, not just to live tombstones.
func listTombstones(ctx context.Context, b backend.Backend) map[string]int64 {
	res := make(map[string]int64)
	err := b.Walk(ctx, metaDir, func(p string, info backend.FileInfo) error {
		if info.IsDir || !strings.HasSuffix(p, ".json") {
			return nil
		}
		sc, err := readMetaDoc(ctx, b, p)
		if err != nil {
			logrus.WithField("component", "vfs").WithField("doc", p).Debugf("Metadata read on %s failed: %v", b.GetName(), err)
			return nil
		}
		if sc.Deleted {
			res[sc.Path] = sc.Gen
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
		logrus.WithField("component", "vfs").Debugf("Metadata walk on %s failed: %v", b.GetName(), err)
	}
	return res
}

// GatherTombstones lists the tombstone trees of all backends in parallel and
// merges them, keeping the maximum recorded deletion generation per path.
func GatherTombstones(ctx context.Context, backends []backend.Backend) map[string]int64 {
	var mu sync.Mutex
	merged := make(map[string]int64)

	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
		g.Go(func() error {
			tombs := listTombstones(gCtx, b)
			mu.Lock()
			defer mu.Unlock()
			for path, gen := range tombs {
				if gen > merged[path] {
					merged[path] = gen
				}
			}
			return nil
		})
	}
	_ = g.Wait()
	return merged
}

// backendsByName indexes backends by their name for conflict-time sidecar
// reads.
func backendsByName(backends []backend.Backend) map[string]backend.Backend {
	m := make(map[string]backend.Backend, len(backends))
	for _, b := range backends {
		m[b.GetName()] = b
	}
	return m
}

// walkBackends is syncAll's phase A: every backend's tree is walked in
// parallel, collecting the observed Metadata per path (Backends holds just
// that backend, Gen is unknown/0). Walk errors only log — partial results are
// kept — and Reconcile sweeps vanished paths only for backends whose walk
// succeeded, as before.
func (c *Cache) walkBackends(ctx context.Context, backends []backend.Backend) map[string]map[string]Metadata {
	perBackend := make(map[string]map[string]Metadata, len(backends))
	for _, b := range backends {
		perBackend[b.GetName()] = make(map[string]Metadata)
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
		state := perBackend[b.GetName()] // each goroutine writes only its own map
		g.Go(func() error {
			walkStart := time.Now()
			c.logger().Debugf("Background syncing backend: %s", b.GetName())
			seenPaths := make(map[string]bool)
			err := b.Walk(gCtx, "", func(path string, info backend.FileInfo) error {
				if IsReservedPath(path) {
					// Cluster-internal state is never indexed, so it must not
					// appear in seenPaths either (Reconcile only sweeps paths
					// that are in the cache).
					return nil
				}
				seenPaths[path] = true
				state[path] = Metadata{
					Name:     info.Name,
					Path:     path,
					Size:     info.Size,
					Mode:     info.Mode,
					ModTime:  info.ModTime,
					IsDir:    info.IsDir,
					Backends: []string{b.GetName()},
				}
				return nil
			})
			if err != nil {
				c.logger().Errorf("Background sync error on %s: %v", b.GetName(), err)
				return nil // Don't fail other backends
			}
			c.Reconcile(b.GetName(), seenPaths, walkStart)
			return nil
		})
	}
	_ = g.Wait()
	return perBackend
}

// resolveGlobalState is syncAll's phase B: the per-backend walk results are
// folded into one winning Metadata per path via mergeMeta. Sidecars are
// consulted only for paths where file candidates disagree on size (and no
// directory is involved — directories win type conflicts on presence alone)
// or that carry a tombstone.
//
// tombstones maps path → max recorded deletion generation. For a tombstoned
// file path the candidates' generations are loaded and every candidate at or
// below the tombstone generation is dropped as a zombie; if no candidate
// survives the path is dead and is returned in the second result instead of
// being resolved. Candidates strictly above the tombstone generation are a
// genuinely newer write and resolve normally.
func resolveGlobalState(ctx context.Context, perBackend map[string]map[string]Metadata, byName map[string]backend.Backend, tombstones map[string]int64) (map[string]*Metadata, []string) {
	candidates := make(map[string][]Metadata)
	for _, state := range perBackend {
		for path, meta := range state {
			candidates[path] = append(candidates[path], meta)
		}
	}

	resolved := make(map[string]*Metadata, len(candidates))
	var dead []string
	for path, cands := range candidates {
		if tombGen, ok := tombstones[path]; ok {
			loadGenerations(ctx, path, cands, byName)
			cands = dropZombies(cands, tombGen)
			if len(cands) == 0 {
				dead = append(dead, path)
				continue
			}
		} else if isFileConflict(cands) {
			loadGenerations(ctx, path, cands, byName)
		}
		resolved[path] = foldCandidates(cands)
	}
	return resolved, dead
}

// dropZombies returns the candidates whose generation is strictly above the
// tombstone generation; the rest are zombie replicas of a deleted version.
// Generations must already be loaded.
func dropZombies(cands []Metadata, tombGen int64) []Metadata {
	live := cands[:0]
	for _, m := range cands {
		if m.Gen > tombGen {
			live = append(live, m)
		}
	}
	return live
}

// isFileConflict reports whether the candidates for one path form a real file
// conflict that generations must arbitrate: all candidates are files and at
// least two disagree on size. Directory candidates never need sidecars (the
// dir-wins rule resolves on presence), and same-size file replicas merge as
// one version without sidecar reads — the overwhelmingly common case.
func isFileConflict(cands []Metadata) bool {
	if len(cands) < 2 {
		return false
	}
	conflict := false
	for _, m := range cands {
		if m.IsDir {
			return false
		}
		if m.Size != cands[0].Size {
			conflict = true
		}
	}
	return conflict
}

// loadGenerations reads, in parallel, path's sidecar on each candidate's
// backend and stamps the candidate's Gen (0 on a missing sidecar or error).
func loadGenerations(ctx context.Context, path string, cands []Metadata, byName map[string]backend.Backend) {
	var wg sync.WaitGroup
	for i := range cands {
		b, ok := byName[cands[i].Backends[0]]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(m *Metadata) {
			defer wg.Done()
			m.Gen = sidecarGen(ctx, b, path)
		}(&cands[i])
	}
	wg.Wait()
}

// foldCandidates reduces a path's candidates to the winning Metadata by
// folding them through mergeMeta.
func foldCandidates(cands []Metadata) *Metadata {
	res := cands[0]
	res.Backends = append([]string(nil), cands[0].Backends...)
	for _, m := range cands[1:] {
		mergeMeta(&res, m, m.Backends)
	}
	return &res
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
				LastUpdated:  time.Now(),
			}
			curr.Children[part] = next
		}

		if i == len(parts)-1 {
			mergeMeta(&next.Meta, meta, backends)
			next.LastUpdated = time.Now()
		}

		parent := curr
		curr = next
		parent.Mu.Unlock()
	}
}

func (c *Cache) Reconcile(backendName string, seenPaths map[string]bool, walkStart time.Time) {
	c.sweep(c.Root, "", backendName, seenPaths, walkStart)
}

func (c *Cache) sweep(node *Node, path string, backendName string, seenPaths map[string]bool, walkStart time.Time) {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	for name, child := range node.Children {
		childPath := joinPath(path, name)

		c.sweep(child, childPath, backendName, seenPaths, walkStart)

		child.Mu.Lock()
		// Skip sweeping if the node was created/updated after the walk started (REVIEW M3)
		if child.LastUpdated.Before(walkStart) {
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
		}

		// Prune node if it no longer has any backends, no children, and no open handles (REVIEW M4)
		if len(child.Meta.Backends) == 0 && len(child.Children) == 0 && atomic.LoadInt32(&child.OpenHandles) == 0 {
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
	for s := range strings.SplitSeq(path, "/") {
		if s != "" {
			res = append(res, s)
		}
	}
	return res
}

// joinPath appends name to a slash-separated relative parent path, treating
// the empty parent as the namespace root.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
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
	node.LastUpdated = time.Now()
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
				LastUpdated:  time.Now(),
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
	var tombFound bool
	var maxTombGen int64

	g, gCtx := errgroup.WithContext(ctx)
	for _, b := range backends {
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
			// One extra read for a single path is cheap and gives mergeMeta
			// real version knowledge (missing document → Gen 0). The same
			// document carries the deletion marker: a replica at or below the
			// recorded deletion generation is a zombie and must not be
			// admitted (REVIEW.md C6).
			switch sc, scErr := ReadMeta(gCtx, b, path); {
			case scErr == nil:
				meta.Gen = sc.Gen
				if sc.Deleted {
					stateMu.Lock()
					if !tombFound || sc.Gen > maxTombGen {
						tombFound = true
						maxTombGen = sc.Gen
					}
					stateMu.Unlock()
				}
			case !os.IsNotExist(scErr) && !errors.Is(scErr, os.ErrNotExist):
				c.logger().WithField("path", path).Debugf("Metadata read on %s failed: %v", b.GetName(), scErr)
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

	if tombFound && maxTombGen >= bestMeta.Gen {
		// The winning replica is a zombie of a deleted version: the recorded
		// deletion generation is at least as new as anything found.
		return nil, os.ErrNotExist
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
		child.Meta.Path = joinPath(newPath, name)
		child.LastUpdated = time.Now()
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
		g.Go(func() error {
			c.logger().Infof("Scanning backend: %s", b.GetName())
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
				c.logger().Errorf("Error scanning backend %s: %v", b.GetName(), err)
			} else {
				c.logger().Infof("Finished scanning backend: %s", b.GetName())
			}
			return nil
		})
	}
	_ = g.Wait()

	// Mark all directories as FullyIndexed once we finished the whole scan
	c.markAllIndexed(c.Root)

	c.Mu.Lock()
	c.LastReconciled = time.Now()
	c.Mu.Unlock()
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

// FindOverReplicated returns all file nodes that have more backends than the required replication factor.
func (c *Cache) FindOverReplicated(rf int) []*Node {
	var overReplicated []*Node
	c.walk(c.Root, func(n *Node) {
		n.Mu.RLock()
		if !n.Meta.IsDir && len(n.Meta.Backends) > rf {
			overReplicated = append(overReplicated, n)
		}
		n.Mu.RUnlock()
	})
	return overReplicated
}

func (c *Cache) walk(node *Node, fn func(*Node)) {
	fn(node)
	node.Mu.RLock()
	defer node.Mu.RUnlock()
	for _, child := range node.Children {
		c.walk(child, fn)
	}
}

type serializedNode struct {
	Meta         Metadata                   `json:"meta"`
	FullyIndexed bool                       `json:"fully_indexed"`
	LastUpdated  time.Time                  `json:"last_updated"`
	Children     map[string]*serializedNode `json:"children,omitempty"`
}

func (n *Node) toSerialized() *serializedNode {
	n.Mu.RLock()
	defer n.Mu.RUnlock()

	sn := &serializedNode{
		Meta:         n.Meta,
		FullyIndexed: n.FullyIndexed,
		LastUpdated:  n.LastUpdated,
		Children:     make(map[string]*serializedNode),
	}

	for k, child := range n.Children {
		sn.Children[k] = child.toSerialized()
	}

	return sn
}

func (sn *serializedNode) toNode() *Node {
	n := &Node{
		Meta:         sn.Meta,
		FullyIndexed: sn.FullyIndexed,
		LastUpdated:  sn.LastUpdated,
		Children:     make(map[string]*Node),
	}

	for k, child := range sn.Children {
		n.Children[k] = child.toNode()
	}

	return n
}

type serializedCache struct {
	Root           *serializedNode `json:"root"`
	LastReconciled time.Time       `json:"last_reconciled"`
}

func (c *Cache) SaveToFile(path string) error {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	sc := &serializedCache{
		Root:           c.Root.toSerialized(),
		LastReconciled: c.LastReconciled,
	}
	data, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

func (c *Cache) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var sc serializedCache
	if err := json.Unmarshal(data, &sc); err != nil {
		return err
	}

	c.Mu.Lock()
	c.Root = sc.Root.toNode()
	c.LastReconciled = sc.LastReconciled
	c.Mu.Unlock()
	return nil
}

func (c *Cache) Evict(path string) {
	c.evict(path)
}
