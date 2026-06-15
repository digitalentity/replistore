package vfs

// Per-file version metadata ("sidecars").
//
// Sidecars are the version substrate that replaces mtime-based last-writer-
// wins reconciliation (REVIEW.md C4/C6): backend mtimes are stamped by the
// SMB servers' own clocks and cannot be trusted as version vectors, so
// RepliStore maintains its own per-file generation counter, bumped under the
// path lock once per write session. The sidecar tree mirrors the data tree on
// each backend that holds a replica: data file "a/b.txt" has its sidecar at
// ".replistore/meta/a/b.txt.json". A replica with no sidecar reports
// generation 0 and is treated as a legacy (pre-versioning) file. Sidecars
// live under the reserved tree (ReservedDir), which is invisible in the
// unified namespace, so they never leak through the mount.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	gopath "path"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/sirupsen/logrus"
)

// Sidecar is the per-file version metadata stored at
// .replistore/meta/<path>.json on each backend that holds a replica. The same
// document format, with Deleted set, serves as the tombstone stored at
// .replistore/tombstones/<path>.json (REVIEW.md C6): a tombstone records that
// the path was deleted at generation Gen, so any replica at Gen or below is a
// zombie that must not be re-admitted.
type Sidecar struct {
	V       int    `json:"v"`       // format version, 1
	Gen     int64  `json:"gen"`     // generation counter, bumped per write session under the path lock
	Writer  string `json:"writer"`  // node that produced this generation (diagnostics only)
	Deleted bool   `json:"deleted"` // tombstone marker (written by delete/rename, Phase 2)

	// Sum is the content checksum of the replica, "sha256:<hex>"; empty
	// means the content hash is unknown. Writers blank it on every
	// generation bump (random-access FUSE writes make continuous hashing
	// infeasible); repair fills it in while copying the stream. Two
	// same-generation replicas with different non-empty sums hold divergent
	// content (crash artifacts or bit rot).
	Sum string `json:"sum,omitempty"`
}

// sidecarFormatVersion is the only sidecar format version this build
// understands.
const sidecarFormatVersion = 1

// tombstonesDir is the subtree of the reserved directory that holds deletion
// tombstones. Tombstones live in their own tree (not next to the meta
// sidecars) so sync can enumerate all recorded deletions at a cost
// proportional to the number of live tombstones, not the size of the tree.
const tombstonesDir = ReservedDir + "/tombstones"

// SidecarPath maps a data path to the path of its sidecar on a backend.
func SidecarPath(path string) string {
	return ReservedDir + "/meta/" + path + ".json"
}

// TombstonePath maps a data path to the path of its tombstone on a backend.
func TombstonePath(path string) string {
	return tombstonesDir + "/" + path + ".json"
}

// ReadSidecar reads and decodes the sidecar of path from backend b. A missing
// sidecar surfaces as the backend's not-exist error (satisfying
// errors.Is(err, os.ErrNotExist)), which callers interpret as generation 0
// (legacy file).
func ReadSidecar(ctx context.Context, b backend.Backend, path string) (Sidecar, error) {
	return readMetaDoc(ctx, b, SidecarPath(path))
}

// ReadTombstone reads and decodes the tombstone of path from backend b. A
// missing tombstone surfaces as the backend's not-exist error (satisfying
// errors.Is(err, os.ErrNotExist)) — the path has no recorded deletion.
func ReadTombstone(ctx context.Context, b backend.Backend, path string) (Sidecar, error) {
	return readMetaDoc(ctx, b, TombstonePath(path))
}

// readMetaDoc is the shared open/read/decode machinery behind ReadSidecar and
// ReadTombstone.
func readMetaDoc(ctx context.Context, b backend.Backend, scPath string) (Sidecar, error) {
	var sc Sidecar

	f, err := b.OpenFile(ctx, scPath, os.O_RDONLY, 0)
	if err != nil {
		// Pass the backend's error through unchanged so a missing sidecar
		// still satisfies os.IsNotExist / errors.Is(err, os.ErrNotExist).
		return sc, err
	}
	defer f.Close()

	buf := make([]byte, 4096)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && (err != io.EOF || n <= 0) {
		return sc, err
	}

	if err := json.Unmarshal(buf[:n], &sc); err != nil {
		return Sidecar{}, fmt.Errorf("sidecar %s on %s: %w", scPath, b.GetName(), err)
	}
	if sc.V != sidecarFormatVersion {
		return Sidecar{}, fmt.Errorf("sidecar %s on %s: unsupported format version %d", scPath, b.GetName(), sc.V)
	}
	return sc, nil
}

// sidecarGen returns the generation recorded in path's sidecar on backend b.
// A missing sidecar means a legacy (pre-versioning) replica and reports
// generation 0; any other error also degrades to generation 0 (with a debug
// log) so reconciliation can proceed without that backend's version knowledge.
func sidecarGen(ctx context.Context, b backend.Backend, path string) int64 {
	sc, err := ReadSidecar(ctx, b, path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
			logrus.Debugf("vfs: sidecar read for %q on %s failed: %v", path, b.GetName(), err)
		}
		return 0
	}
	return sc.Gen
}

// WriteSidecar encodes sc and writes it to path's sidecar on backend b,
// creating the sidecar's parent directories as needed. The format version is
// set internally.
func WriteSidecar(ctx context.Context, b backend.Backend, path string, sc Sidecar) error {
	return writeMetaDoc(ctx, b, SidecarPath(path), sc)
}

// WriteTombstone encodes sc and writes it as path's tombstone on backend b,
// creating parent directories as needed. Deleted is forced to true and the
// format version is set internally.
func WriteTombstone(ctx context.Context, b backend.Backend, path string, sc Sidecar) error {
	sc.Deleted = true
	return writeMetaDoc(ctx, b, TombstonePath(path), sc)
}

// writeMetaDoc is the shared mkdir/encode/write machinery behind WriteSidecar
// and WriteTombstone.
func writeMetaDoc(ctx context.Context, b backend.Backend, scPath string, sc Sidecar) error {
	sc.V = sidecarFormatVersion
	data, err := json.Marshal(sc)
	if err != nil {
		return err
	}

	if dir := gopath.Dir(scPath); dir != "." {
		if err := b.MkdirAll(ctx, dir, 0755); err != nil {
			return fmt.Errorf("sidecar dir %s on %s: %w", dir, b.GetName(), err)
		}
	}

	f, err := b.OpenFile(ctx, scPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// RemoveSidecar deletes path's sidecar from backend b. An already-absent
// sidecar counts as success (idempotent remove).
func RemoveSidecar(ctx context.Context, b backend.Backend, path string) error {
	return removeMetaDoc(ctx, b, SidecarPath(path))
}

// RemoveTombstone deletes path's tombstone from backend b. An already-absent
// tombstone counts as success (idempotent remove).
func RemoveTombstone(ctx context.Context, b backend.Backend, path string) error {
	return removeMetaDoc(ctx, b, TombstonePath(path))
}

// removeMetaDoc is the shared idempotent-remove machinery behind
// RemoveSidecar and RemoveTombstone.
func removeMetaDoc(ctx context.Context, b backend.Backend, scPath string) error {
	err := b.Remove(ctx, scPath)
	if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
