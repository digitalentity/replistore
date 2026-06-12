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
)

// Sidecar is the per-file version metadata stored at
// .replistore/meta/<path>.json on each backend that holds a replica.
type Sidecar struct {
	V       int    `json:"v"`       // format version, 1
	Gen     int64  `json:"gen"`     // generation counter, bumped per write session under the path lock
	Writer  string `json:"writer"`  // node that produced this generation (diagnostics only)
	Deleted bool   `json:"deleted"` // tombstone marker (written by delete/rename, Phase 2)
}

// sidecarFormatVersion is the only sidecar format version this build
// understands.
const sidecarFormatVersion = 1

// SidecarPath maps a data path to the path of its sidecar on a backend.
func SidecarPath(path string) string {
	return ReservedDir + "/meta/" + path + ".json"
}

// ReadSidecar reads and decodes the sidecar of path from backend b. A missing
// sidecar surfaces as the backend's not-exist error (satisfying
// errors.Is(err, os.ErrNotExist)), which callers interpret as generation 0
// (legacy file).
func ReadSidecar(ctx context.Context, b backend.Backend, path string) (Sidecar, error) {
	var sc Sidecar
	scPath := SidecarPath(path)

	f, err := b.OpenFile(ctx, scPath, os.O_RDONLY, 0)
	if err != nil {
		// Pass the backend's error through unchanged so a missing sidecar
		// still satisfies os.IsNotExist / errors.Is(err, os.ErrNotExist).
		return sc, err
	}
	defer f.Close()

	buf := make([]byte, 4096)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && !(err == io.EOF && n > 0) {
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

// WriteSidecar encodes sc and writes it to path's sidecar on backend b,
// creating the sidecar's parent directories as needed. The format version is
// set internally.
func WriteSidecar(ctx context.Context, b backend.Backend, path string, sc Sidecar) error {
	sc.V = sidecarFormatVersion
	data, err := json.Marshal(sc)
	if err != nil {
		return err
	}

	scPath := SidecarPath(path)
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
	err := b.Remove(ctx, SidecarPath(path))
	if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
