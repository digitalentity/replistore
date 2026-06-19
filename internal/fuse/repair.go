package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/observability"
	"github.com/digitalentity/replistore/internal/vfs"
	"golang.org/x/sync/errgroup"
)

type ActiveRepair struct {
	Path            string  `json:"path"`
	SourceBackend   string  `json:"source_backend"`
	TargetBackend   string  `json:"target_backend"`
	ProgressPercent float64 `json:"progress_percent"`
}

type RepairManager struct {
	fs          *RepliFS
	interval    time.Duration
	grace       time.Duration
	concurrency int

	// divergenceCount counts same-generation replica pairs observed with
	// different non-empty content sums during repair (REVIEW.md C4).
	// Incremented atomically; a diagnostic counter intended to feed future
	// metrics.
	divergenceCount atomic.Int64

	// status fields for API
	scrubActive        atomic.Bool
	lastScrubDuration  atomic.Int64 // in nanoseconds
	degradedFilesCount atomic.Int64

	activeRepairsMu sync.RWMutex
	activeRepairs   map[string]*ActiveRepair
}

// NewRepairManager builds a repair manager. grace is the minimum time a file
// must remain under- or over-replicated before repair acts on it, which absorbs
// transient backend outages; pass 0 to act immediately.
func NewRepairManager(fs *RepliFS, interval, grace time.Duration, concurrency int) *RepairManager {
	return &RepairManager{
		fs:            fs,
		interval:      interval,
		grace:         grace,
		concurrency:   concurrency,
		activeRepairs: make(map[string]*ActiveRepair),
	}
}

type RepairStatus struct {
	ScrubActive              bool           `json:"scrub_active"`
	LastScrubDurationSeconds float64        `json:"last_scrub_duration_seconds"`
	DegradedFilesCount       int64          `json:"degraded_files_count"`
	DivergentFilesCount      int64          `json:"divergent_files_count"`
	ActiveRepairs            []ActiveRepair `json:"active_repairs"`
}

func (m *RepairManager) GetStatus() RepairStatus {
	m.activeRepairsMu.RLock()
	activeList := make([]ActiveRepair, 0, len(m.activeRepairs))
	for _, ar := range m.activeRepairs {
		activeList = append(activeList, *ar)
	}
	m.activeRepairsMu.RUnlock()

	return RepairStatus{
		ScrubActive:              m.scrubActive.Load(),
		LastScrubDurationSeconds: float64(m.lastScrubDuration.Load()) / float64(time.Second),
		DegradedFilesCount:       m.degradedFilesCount.Load(),
		DivergentFilesCount:      m.divergenceCount.Load(),
		ActiveRepairs:            activeList,
	}
}

func (m *RepairManager) logger(ctx context.Context) *slog.Logger {
	return observability.Logger(ctx).With(slog.String("component", "repair"), slog.String("node_id", m.fs.NodeID))
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
	// Generate unique correlation ID for this scrub cycle
	ctx = observability.WithCorrelationID(ctx, "scrub-"+observability.GenerateCorrelationID())

	// Attempt to acquire the global repair lock.
	// If it fails, another node is already performing repairs.
	lock, err := m.fs.acquireLock(ctx, GlobalRepairLockPath)
	if err != nil {
		m.logger(ctx).Debug("Another node is currently performing repairs, skipping this cycle")

		return
	}
	if lock != nil {
		defer lock.Release()
	}

	m.scrubActive.Store(true)
	start := time.Now()
	defer func() {
		m.lastScrubDuration.Store(int64(time.Since(start)))
		m.scrubActive.Store(false)
	}()

	m.enforceTombstones(ctx)

	m.logger(ctx).Info("Starting background repair scrub...")
	now := time.Now()
	degraded := m.fs.Cache.FindDegraded(m.fs.ReplicationFactor, m.grace, now)
	m.degradedFilesCount.Store(int64(len(degraded)))
	overReplicated := m.fs.Cache.FindOverReplicated(m.fs.ReplicationFactor, m.grace, now)

	if len(degraded) == 0 && len(overReplicated) == 0 {
		m.logger(ctx).Info("No degraded or over-replicated files found")

		return
	}

	if len(degraded) > 0 {
		m.logger(ctx).Info("Found degraded files, starting repair...", slog.Int("count", len(degraded)))
	}
	if len(overReplicated) > 0 {
		m.logger(ctx).Info("Found over-replicated files, starting pruning...", slog.Int("count", len(overReplicated)))
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(m.concurrency)

	for _, node := range degraded {
		g.Go(func() error {
			if err := m.repairNode(gCtx, node); err != nil {
				m.logger(gCtx).Error("Failed to repair", slog.String("path", node.Meta.Path), slog.Any("error", err))
			}

			return nil // Don't fail the whole scrub on single file error
		})
	}

	for _, node := range overReplicated {
		g.Go(func() error {
			if err := m.pruneNode(gCtx, node); err != nil {
				m.logger(gCtx).Error("Failed to prune", slog.String("path", node.Meta.Path), slog.Any("error", err))
			}

			return nil
		})
	}

	_ = g.Wait()
	m.logger(ctx).Info("Background repair scrub completed")
}

// enforceTombstones makes recorded deletions stick (REVIEW.md C6): for every
// tombstone found on any backend it deletes zombie replicas (data at or below
// the tombstone generation), retires tombstones that a genuinely newer write
// has obsoleted, and garbage-collects tombstones whose path is absent on every
// responding backend. Runs under the global repair lock.
func (m *RepairManager) enforceTombstones(ctx context.Context) {
	tombstones := vfs.GatherTombstones(ctx, m.fs.getBackendList())
	for path, tombGen := range tombstones {
		m.enforceTombstone(ctx, path, tombGen)
	}
}

// enforceTombstone applies one path's tombstone (at generation tombGen)
// against every backend. See enforceTombstones for the rules.
func (m *RepairManager) enforceTombstone(ctx context.Context, path string, tombGen int64) {
	// Serialize against concurrent local mutations of this path before any
	// backend I/O: Dir.Create writes data before its sidecar, and a replica
	// seen in that gap looks like a gen-0 zombie that enforcement would
	// delete mid-creation. Local path lock first, then the distributed lock
	// (same ordering invariant and pattern as repairNode).
	unlock := m.fs.pathLocks.lock(path)
	defer unlock()

	lock, err := m.fs.acquireLock(ctx, path)
	if err != nil {
		// Another node is working on this path; skip it this cycle.
		m.logger(ctx).Debug("Tombstone enforcement: could not lock, skipping",
			slog.String("path", path),
			slog.Any("error", err),
		)

		return
	}
	if lock != nil {
		defer lock.Release()
	}

	var failed bool   // some backend gave no authoritative answer or a removal failed
	var obsolete bool // a replica newer than the tombstone exists

	for name, b := range m.fs.Backends {
		info, err := b.Stat(ctx, path)
		if err != nil {
			if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
				continue // definitively absent here
			}
			m.logger(ctx).Debug("Tombstone enforcement: stat failed",
				slog.String("path", path),
				slog.String("backend", name),
				slog.Any("error", err),
			)
			failed = true

			continue
		}

		gen := int64(0) // missing sidecar = legacy replica at generation 0
		if sc, scErr := vfs.ReadSidecar(ctx, b, path); scErr == nil {
			gen = sc.Gen
		} else if !errors.Is(scErr, os.ErrNotExist) && !os.IsNotExist(scErr) {
			m.logger(ctx).Debug("Tombstone enforcement: sidecar read failed",
				slog.String("path", path),
				slog.String("backend", name),
				slog.Any("error", scErr),
			)
			failed = true

			continue
		}

		if gen > tombGen {
			// A write newer than the deletion: the tombstone is obsolete.
			obsolete = true

			continue
		}

		// Zombie replica of the deleted version: remove data and meta sidecar.
		m.logger(ctx).Info("Tombstone enforcement: removing zombie replica",
			slog.String("path", path),
			slog.Int64("gen", gen),
			slog.Int64("tomb_gen", tombGen),
			slog.String("backend", name),
		)
		var removeErr error
		if info.IsDir {
			removeErr = removeAll(ctx, b, path)
		} else {
			removeErr = b.Remove(ctx, path)
		}
		if removeErr != nil && !os.IsNotExist(removeErr) && !errors.Is(removeErr, os.ErrNotExist) {
			m.logger(ctx).Warn("Tombstone enforcement: failed to remove zombie",
				slog.String("path", path),
				slog.String("backend", name),
				slog.Any("error", removeErr),
			)
			failed = true

			continue
		}
		if err := vfs.RemoveSidecar(ctx, b, path); err != nil {
			m.logger(ctx).Warn("Tombstone enforcement: failed to remove sidecar",
				slog.String("path", path),
				slog.String("backend", name),
				slog.Any("error", err),
			)
		}
	}

	switch {
	case obsolete:
		// A newer-generation replica exists, so the tombstone must not keep
		// killing it: retire the tombstone everywhere.
		m.removeTombstones(ctx, path)
	case !failed:
		// The path is absent on every backend and all of them responded: the
		// deletion has fully converged, garbage-collect the tombstone. (Zombies
		// removed just above count: they are gone now.)
		m.removeTombstones(ctx, path)
	default:
		// Some backend gave no authoritative answer: keep the tombstone so the
		// deletion can still be enforced once that backend is reachable.
	}
}

// removeTombstones retires path's tombstone on every backend, best-effort.
// Because a sidecar and a tombstone share one metadata document per path, this
// removes the document only where it still carries the deletion marker: a
// backend whose document has been overwritten by a newer live write must keep
// it. The read-then-remove is racy against a concurrent write; the cost of
// losing that race is a sidecar dropping to generation 0 (legacy), which repair
// re-stamps — never data loss.
func (m *RepairManager) removeTombstones(ctx context.Context, path string) {
	for name, b := range m.fs.Backends {
		sc, err := vfs.ReadMeta(ctx, b, path)
		if err != nil || !sc.Deleted {
			// Absent, unreadable, or a live sidecar: nothing to retire here.
			continue
		}
		if err := vfs.RemoveMeta(ctx, b, path); err != nil {
			m.logger(ctx).Warn("Failed to remove tombstone",
				slog.String("path", path),
				slog.String("backend", name),
				slog.Any("error", err),
			)
		}
	}
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

	m.logger(ctx).Info("Repairing path",
		slog.String("path", path),
		slog.String("source", sourceName),
		slog.Any("targets", targets),
	)

	// 3. Perform copy
	sourceBackend := m.fs.Backends[sourceName]
	srcFile, err := sourceBackend.OpenFile(ctx, path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Source version sidecar, read once: it seeds the target sidecars,
	// anchors divergence detection, and receives the content sum computed
	// while copying. A missing sidecar means a legacy (pre-versioning) file.
	srcSC, srcSCErr := vfs.ReadSidecar(ctx, sourceBackend, path)
	if srcSCErr != nil && !errors.Is(srcSCErr, os.ErrNotExist) {
		m.logger(ctx).Warn("Failed to read sidecar",
			slog.String("path", path),
			slog.String("backend", sourceName),
			slog.Any("error", srcSCErr),
		)
	}

	// For each target, open and copy
	for _, targetName := range targets {
		targetBackend := m.fs.Backends[targetName]

		// Divergence detection (REVIEW.md C4): if the target already holds a
		// replica at the same generation as the source but with a different
		// non-empty content sum, the replicas have divergent content (crash
		// artifact or bit rot). The source still wins, exactly as before —
		// the checksum only makes the event visible.
		if srcSCErr == nil && srcSC.Sum != "" {
			if _, statErr := targetBackend.Stat(ctx, path); statErr == nil {
				if tsc, tErr := vfs.ReadSidecar(ctx, targetBackend, path); tErr == nil &&
					tsc.Gen == srcSC.Gen && tsc.Sum != "" && tsc.Sum != srcSC.Sum {
					m.divergenceCount.Add(1)
					m.logger(ctx).Error("Divergent replicas detected; overwriting target from source",
						slog.String("path", path),
						slog.Int64("gen", srcSC.Gen),
						slog.String("source", sourceName),
						slog.String("source_sum", srcSC.Sum),
						slog.String("target", targetName),
						slog.String("target_sum", tsc.Sum),
					)
				}
			}
		}

		activeKey := path + ":" + targetName
		ar := &ActiveRepair{
			Path:            path,
			SourceBackend:   sourceName,
			TargetBackend:   targetName,
			ProgressPercent: 0.0,
		}
		m.activeRepairsMu.Lock()
		m.activeRepairs[activeKey] = ar
		m.activeRepairsMu.Unlock()

		cleanupActive := func() {
			m.activeRepairsMu.Lock()
			delete(m.activeRepairs, activeKey)
			m.activeRepairsMu.Unlock()
		}

		dstFile, err := targetBackend.OpenFile(ctx, path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, node.Meta.Mode)
		if err != nil {
			cleanupActive()
			m.logger(ctx).Warn("Repair failed on target",
				slog.String("path", path),
				slog.String("backend", targetName),
				slog.Any("error", err),
			)

			continue
		}

		fileSize := node.Meta.Size
		if fileSize <= 0 {
			fileSize = 1
		}
		var copiedBytes int64

		hasher := sha256.New()
		pw := &progressWriter{
			w: &offsetWriter{ctx: ctx, f: dstFile},
			onProgress: func(n int) {
				const maxPercentage = 100.0
				copiedBytes += int64(n)
				pct := (float64(copiedBytes) / float64(fileSize)) * maxPercentage
				if pct > maxPercentage {
					pct = maxPercentage
				}
				m.activeRepairsMu.Lock()
				ar.ProgressPercent = pct
				m.activeRepairsMu.Unlock()
			},
		}

		_, copyErr := io.Copy(pw, io.TeeReader(&offsetReader{ctx: ctx, f: srcFile}, hasher))
		_ = dstFile.Close()
		cleanupActive()

		if copyErr != nil {
			m.logger(ctx).Warn("Copy failed to target",
				slog.String("path", path),
				slog.String("backend", targetName),
				slog.Any("error", copyErr),
			)

			continue
		}

		// Preserve the source replica's mtime on the destination so the two
		// replicas compare as the same version (same mtime, same size) during
		// reconciliation. Without this, the fresh copy looks newer than the
		// source and anti-entropy never converges.
		mtime := sourceModTime(ctx, sourceBackend, path, cachedModTime)
		if err := targetBackend.Chtimes(ctx, path, mtime, mtime); err != nil {
			m.logger(ctx).Warn("Failed to preserve mtime",
				slog.String("path", path),
				slog.String("backend", targetName),
				slog.Any("error", err),
			)
		}

		// Replicate the source's version sidecar so the new copy reports the
		// same generation, stamping in the content sum just computed. A
		// missing sidecar means a legacy (pre-versioning) file — nothing to
		// copy and nowhere to record the sum. Sidecar failures don't fail the
		// repair: the target just reports generation 0 until the next write.
		if srcSCErr == nil {
			sum := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
			m.copyRepairedSidecar(ctx, sourceBackend, targetBackend, path, sourceName, targetName, &srcSC, sum)
		}

		// Update metadata
		node.Mu.Lock()
		found := slices.Contains(node.Meta.Backends, targetName)
		if !found {
			node.Meta.Backends = append(node.Meta.Backends, targetName)
		}
		node.Mu.Unlock()
		m.logger(ctx).Info("Successfully repaired on backend",
			slog.String("path", path),
			slog.String("backend", targetName),
		)
	}

	return nil
}

// copyRepairedSidecar replicates the source's version sidecar to the target,
// stamping in the freshly computed content sum, and records that sum on the
// source too if its own sidecar was unknown or stale (updating srcSC). Sidecar
// failures are non-fatal: the target reports generation 0 until the next write.
func (m *RepairManager) copyRepairedSidecar(ctx context.Context, sourceBackend, targetBackend backend.Backend, path, sourceName, targetName string, srcSC *vfs.Sidecar, sum string) {
	sc := *srcSC
	sc.Sum = sum
	if err := vfs.WriteSidecar(ctx, targetBackend, path, sc); err != nil {
		m.logger(ctx).Warn("Failed to copy sidecar",
			slog.String("path", path),
			slog.String("backend", targetName),
			slog.Any("error", err),
		)
	}

	// The source content was just read in full, so record what we saw on the
	// source too if its sum was unknown or stale. Non-fatal.
	if srcSC.Sum == sum {
		return
	}
	if err := vfs.WriteSidecar(ctx, sourceBackend, path, sc); err != nil {
		m.logger(ctx).Warn("Failed to record content sum",
			slog.String("path", path),
			slog.String("backend", sourceName),
			slog.Any("error", err),
		)

		return
	}
	srcSC.Sum = sum
}

func (m *RepairManager) pruneNode(ctx context.Context, node *vfs.Node) error {
	node.Mu.Lock()
	path := node.Meta.Path
	current := append([]string(nil), node.Meta.Backends...)
	node.Mu.Unlock()

	if len(current) <= m.fs.ReplicationFactor {
		return nil // Not over-replicated
	}

	// Local serialization first, then distributed lock (ordering invariant on RepliFS)
	unlock := m.fs.pathLocks.lock(path)
	defer unlock()

	lock, err := m.fs.acquireLock(ctx, path)
	if err != nil {
		return err
	}
	if lock != nil {
		defer lock.Release()
	}

	// Double check under lock
	node.Mu.Lock()
	if len(node.Meta.Backends) <= m.fs.ReplicationFactor {
		node.Mu.Unlock()

		return nil
	}
	current = append([]string(nil), node.Meta.Backends...)
	node.Mu.Unlock()

	// Select which ones to keep using the configured backend selector
	selectedToKeep := m.fs.Selector.SelectForWrite(m.fs.ReplicationFactor, current)
	keepMap := make(map[string]bool)
	for _, bName := range selectedToKeep {
		keepMap[bName] = true
	}

	// Fallback to preserve replication factor if selector returned fewer
	if len(selectedToKeep) < m.fs.ReplicationFactor {
		for _, bName := range current {
			if !keepMap[bName] {
				keepMap[bName] = true
				selectedToKeep = append(selectedToKeep, bName)
				if len(selectedToKeep) == m.fs.ReplicationFactor {
					break
				}
			}
		}
	}

	// Identify replicas to prune
	var toPrune []string
	for _, bName := range current {
		if !keepMap[bName] {
			toPrune = append(toPrune, bName)
		}
	}

	if len(toPrune) == 0 {
		return nil
	}

	m.logger(ctx).Info("Pruning over-replicated file",
		slog.String("path", path),
		slog.Any("keeping", selectedToKeep),
		slog.Any("pruning", toPrune),
	)

	var prunedSuccessfully []string
	for _, targetName := range toPrune {
		targetBackend := m.fs.Backends[targetName]
		if targetBackend == nil {
			continue
		}

		// Remove the replica
		err := targetBackend.Remove(ctx, path)
		if err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
			m.logger(ctx).Warn("Failed to prune replica",
				slog.String("path", path),
				slog.String("backend", targetName),
				slog.Any("error", err),
			)

			continue
		}

		// Remove sidecar
		if err := vfs.RemoveSidecar(ctx, targetBackend, path); err != nil {
			m.logger(ctx).Warn("Failed to remove sidecar during pruning",
				slog.String("path", path),
				slog.String("backend", targetName),
				slog.Any("error", err),
			)
		}

		prunedSuccessfully = append(prunedSuccessfully, targetName)
	}

	if len(prunedSuccessfully) > 0 {
		node.Mu.Lock()
		newBackends := make([]string, 0, len(node.Meta.Backends))
		for _, b := range node.Meta.Backends {
			pruned := slices.Contains(prunedSuccessfully, b)
			if !pruned {
				newBackends = append(newBackends, b)
			}
		}
		node.Meta.Backends = newBackends
		node.Mu.Unlock()
		m.logger(ctx).Info("Successfully pruned replicas",
			slog.String("path", path),
			slog.Any("backends", prunedSuccessfully),
		)
	}

	return nil
}

// sourceModTime returns the current ModTime of path on the source backend,
// falling back to the cached metadata value if the Stat fails.
func sourceModTime(ctx context.Context, source backend.Backend, path string, fallback time.Time) time.Time {
	fi, err := source.Stat(ctx, path)
	if err != nil {
		slog.Debug("Stat failed, using cached mtime",
			slog.String("component", "repair"),
			slog.String("path", path),
			slog.String("backend", source.GetName()),
			slog.Any("error", err),
		)

		return fallback
	}

	return fi.ModTime
}

// Helpers to adapt backend.File to io.Reader/Writer.
type offsetReader struct {
	ctx    context.Context //nolint:containedctx // Needed to adapt context-aware backend.File to io.Reader
	f      backend.File
	offset int64
}

func (r *offsetReader) Read(p []byte) (int, error) {
	n, err := r.f.ReadAt(r.ctx, p, r.offset)
	r.offset += int64(n)

	return n, err
}

type offsetWriter struct {
	ctx    context.Context //nolint:containedctx // Needed to adapt context-aware backend.File to io.Writer
	f      backend.File
	offset int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(w.ctx, p, w.offset)
	w.offset += int64(n)

	return n, err
}

func removeAll(ctx context.Context, b backend.Backend, path string) error {
	var files []string
	var dirs []string
	err := b.Walk(ctx, path, func(p string, info backend.FileInfo) error {
		if info.IsDir {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
		}

		return nil
	})
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	// Remove all files
	for _, f := range files {
		if err := b.Remove(ctx, f); err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		_ = vfs.RemoveSidecar(ctx, b, f)
	}

	// Remove dirs in reverse order (bottom-up)
	for _, v := range slices.Backward(dirs) {
		d := v
		if err := b.Remove(ctx, d); err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	// Finally, remove the directory itself
	if err := b.Remove(ctx, path); err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

type progressWriter struct {
	w          io.Writer
	onProgress func(n int)
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.onProgress(n)
	}

	return n, err
}
