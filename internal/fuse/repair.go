package fuse

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type RepairManager struct {
	fs          *RepliFS
	interval    time.Duration
	concurrency int
}

func NewRepairManager(fs *RepliFS, interval time.Duration, concurrency int) *RepairManager {
	return &RepairManager{
		fs:          fs,
		interval:    interval,
		concurrency: concurrency,
	}
}

func (m *RepairManager) Start(ctx context.Context) {
	if m.interval <= 0 {
		return
	}

	ticker := time.NewTicker(m.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.performScrub(ctx)
			}
		}
	}()
}

const GlobalRepairLockPath = ".replistore/repair.lock"

func (m *RepairManager) performScrub(ctx context.Context) {
	// Attempt to acquire the global repair lock.
	// If it fails, another node is already performing repairs.
	lock, err := m.fs.acquireLock(ctx, GlobalRepairLockPath)
	if err != nil {
		logrus.Debug("Another node is currently performing repairs, skipping this cycle")
		return
	}
	if lock != nil {
		defer lock.Release()
	}

	logrus.Info("Starting background repair scrub...")
	degraded := m.fs.Cache.FindDegraded(m.fs.ReplicationFactor)
	if len(degraded) == 0 {
		logrus.Info("No degraded files found")
		return
	}

	logrus.Infof("Found %d degraded files, starting repair...", len(degraded))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(m.concurrency)

	for _, node := range degraded {
		node := node
		g.Go(func() error {
			if err := m.repairNode(gCtx, node); err != nil {
				logrus.Errorf("Failed to repair %s: %v", node.Meta.Path, err)
			}
			return nil // Don't fail the whole scrub on single file error
		})
	}
	_ = g.Wait()
	logrus.Info("Background repair scrub completed")
}

func (m *RepairManager) repairNode(ctx context.Context, node *vfs.Node) error {
	node.Mu.Lock()
	path := node.Meta.Path
	cachedModTime := node.Meta.ModTime
	currentBackends := make(map[string]bool)
	for _, b := range node.Meta.Backends {
		currentBackends[b] = true
	}

	if len(currentBackends) >= m.fs.ReplicationFactor {
		node.Mu.Unlock()
		return nil // Already repaired or not degraded
	}

	// 1. Identify source
	var sourceName string
	for bName := range currentBackends {
		sourceName = bName
		break
	}
	node.Mu.Unlock()

	if sourceName == "" {
		return io.ErrUnexpectedEOF
	}

	// Local serialization: held for the whole repair of this file, acquired
	// before the distributed lock (see ordering invariant on RepliFS). This
	// serializes repair against local create/remove/rename and against the
	// inline healing copy in File.Open. Known residual gap: it does NOT
	// serialize against in-flight writes on an already-open handle (the
	// handle releases the path lock at the end of Open); a full fix needs
	// per-file open-handle tracking and is out of scope here.
	unlock := m.fs.pathLocks.lock(path)
	defer unlock()

	// Distributed Locking to prevent conflicts with concurrent deletes/writes
	lock, err := m.fs.acquireLock(ctx, path)
	if err != nil {
		return err
	}
	if lock != nil {
		defer lock.Release()
	}

	// Double check if still degraded after acquiring lock
	node.Mu.Lock()
	if len(node.Meta.Backends) >= m.fs.ReplicationFactor {
		node.Mu.Unlock()
		return nil
	}
	// Re-sync currentBackends in case they changed
	currentBackends = make(map[string]bool)
	for _, b := range node.Meta.Backends {
		currentBackends[b] = true
	}
	node.Mu.Unlock()

	// 2. Identify targets
	var targets []string
	allBackendNames := make([]string, 0, len(m.fs.Backends))
	for name := range m.fs.Backends {
		allBackendNames = append(allBackendNames, name)
	}

	// We need backends that DON'T have the file
	// Select for write gives us healthy backends.
	potentialTargets := m.fs.Selector.SelectForWrite(m.fs.ReplicationFactor, allBackendNames)
	for _, bName := range potentialTargets {
		if !currentBackends[bName] {
			targets = append(targets, bName)
			if len(currentBackends)+len(targets) == m.fs.ReplicationFactor {
				break
			}
		}
	}

	if len(targets) == 0 {
		return nil // No healthy targets available
	}

	logrus.Infof("Repairing %s: copying from %s to %v", path, sourceName, targets)

	// 3. Perform copy
	sourceBackend := m.fs.Backends[sourceName]
	srcFile, err := sourceBackend.OpenFile(ctx, path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// For each target, open and copy
	for _, targetName := range targets {
		targetBackend := m.fs.Backends[targetName]

		dstFile, err := targetBackend.OpenFile(ctx, path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, node.Meta.Mode)
		if err != nil {
			logrus.Warnf("Repair failed for %s on target %s: %v", path, targetName, err)
			continue
		}

		// Simple implementation: sequential copy for each target
		if _, err := io.Copy(&offsetWriter{ctx: ctx, f: dstFile}, &offsetReader{ctx: ctx, f: srcFile}); err != nil {
			_ = dstFile.Close()
			logrus.Warnf("Copy failed for %s to %s: %v", path, targetName, err)
			continue
		}
		_ = dstFile.Close()

		// Preserve the source replica's mtime on the destination so the two
		// replicas compare as the same version (same mtime, same size) during
		// reconciliation. Without this, the fresh copy looks newer than the
		// source and anti-entropy never converges.
		mtime := sourceModTime(ctx, sourceBackend, path, cachedModTime)
		if err := targetBackend.Chtimes(ctx, path, mtime, mtime); err != nil {
			logrus.Warnf("Failed to preserve mtime for %s on %s: %v", path, targetName, err)
		}

		// Replicate the source's version sidecar so the new copy reports the
		// same generation. A missing sidecar means a legacy (pre-versioning)
		// file — nothing to copy. Sidecar failures don't fail the repair: the
		// target just reports generation 0 until the next write.
		sc, scErr := vfs.ReadSidecar(ctx, sourceBackend, path)
		switch {
		case scErr == nil:
			if err := vfs.WriteSidecar(ctx, targetBackend, path, sc); err != nil {
				logrus.Warnf("Failed to copy sidecar for %s to %s: %v", path, targetName, err)
			}
		case errors.Is(scErr, os.ErrNotExist):
			// Legacy file without a sidecar; skip.
		default:
			logrus.Warnf("Failed to read sidecar for %s from %s: %v", path, sourceName, scErr)
		}

		// Update metadata
		node.Mu.Lock()
		found := false
		for _, b := range node.Meta.Backends {
			if b == targetName {
				found = true
				break
			}
		}
		if !found {
			node.Meta.Backends = append(node.Meta.Backends, targetName)
		}
		node.Mu.Unlock()
		logrus.Infof("Successfully repaired %s on backend %s", path, targetName)
	}

	return nil
}

// sourceModTime returns the current ModTime of path on the source backend,
// falling back to the cached metadata value if the Stat fails.
func sourceModTime(ctx context.Context, source backend.Backend, path string, fallback time.Time) time.Time {
	fi, err := source.Stat(ctx, path)
	if err != nil {
		logrus.Debugf("Stat of %s on %s failed, using cached mtime: %v", path, source.GetName(), err)
		return fallback
	}
	return fi.ModTime
}

// Helpers to adapt backend.File to io.Reader/Writer
type offsetReader struct {
	ctx    context.Context
	f      backend.File
	offset int64
}

func (r *offsetReader) Read(p []byte) (n int, err error) {
	n, err = r.f.ReadAt(r.ctx, p, r.offset)
	r.offset += int64(n)
	return
}

type offsetWriter struct {
	ctx    context.Context
	f      backend.File
	offset int64
}

func (w *offsetWriter) Write(p []byte) (n int, err error) {
	n, err = w.f.WriteAt(w.ctx, p, w.offset)
	w.offset += int64(n)
	return
}
