package vfs

// Per-path version metadata ("sidecars") and deletion tombstones.
//
// Sidecars are the version substrate that replaces mtime-based last-writer-
// wins reconciliation (REVIEW.md C4/C6): backend mtimes are stamped by the
// SMB servers' own clocks and cannot be trusted as version vectors, so
// RepliStore maintains its own per-path generation counter, bumped under the
// path lock once per write session. A replica with no metadata document
// reports generation 0 and is treated as a legacy (pre-versioning) file.
//
// Sidecars and tombstones share one document per path: a tombstone is simply a
// document with Deleted set. The document lives under the reserved tree
// (ReservedDir), invisible in the unified namespace, keyed by the SHA-256 of
// the data path rather than by mirroring the data tree. The hash is one-way,
// so the data path is also recorded inside the document (Sidecar.Path); this
// is exactly the primary-key shape a key/value metadata store would want, and
// it lets tombstone enumeration recover the data path without a reverse map.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	gopath "path"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/observability"
	"log/slog"
)

// Sidecar is the per-path version metadata stored under metaDir, keyed by the
// hash of the data path. With Deleted set it doubles as the deletion tombstone
// (REVIEW.md C6): a tombstone records that the path was deleted at generation
// Gen, so any replica at Gen or below is a zombie that must not be re-admitted.
type Sidecar struct {
	V       int    `json:"v"`       // format version, 1
	Path    string `json:"path"`    // data path this document describes (the hash key is one-way)
	Gen     int64  `json:"gen"`     // generation counter, bumped per write session under the path lock
	Writer  string `json:"writer"`  // node that produced this generation (diagnostics only)
	Deleted bool   `json:"deleted"` // tombstone marker (written by delete/rename)

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
const sidecarBufSize = 8192

// metaDir is the subtree of the reserved directory that holds all metadata
// documents (sidecars and tombstones alike).
const metaDir = ReservedDir + "/meta"

// MetaPath maps a data path to its metadata document key on a backend. The
// data path is hashed (SHA-256, hex) and sharded two levels deep so no single
// directory accumulates every entry: meta/<h0>/<h1>/<hash>.json. The hash is
// one-way, so the data path is stored inside the document (see Sidecar.Path).
func MetaPath(path string) string {
	sum := sha256.Sum256([]byte(path))
	h := hex.EncodeToString(sum[:])

	return metaDir + "/" + h[0:2] + "/" + h[2:4] + "/" + h + ".json"
}

// ReadMeta reads and decodes the metadata document of path from backend b. A
// missing document surfaces as the backend's not-exist error (satisfying
// errors.Is(err, os.ErrNotExist)), which callers interpret as generation 0
// (legacy file) or "no recorded deletion".
func ReadMeta(ctx context.Context, b backend.Backend, path string) (Sidecar, error) {
	return readMetaDoc(ctx, b, MetaPath(path))
}

// ReadSidecar is an alias of ReadMeta kept for call-site readability on the
// version-lookup path.
func ReadSidecar(ctx context.Context, b backend.Backend, path string) (Sidecar, error) {
	return ReadMeta(ctx, b, path)
}

// readMetaDoc opens, reads and decodes the metadata document at the given key.
func readMetaDoc(ctx context.Context, b backend.Backend, scPath string) (Sidecar, error) {
	var sc Sidecar

	f, err := b.OpenFile(ctx, scPath, os.O_RDONLY, 0)
	if err != nil {
		// Pass the backend's error through unchanged so a missing document
		// still satisfies os.IsNotExist / errors.Is(err, os.ErrNotExist).
		return sc, err
	}
	defer f.Close()

	buf := make([]byte, sidecarBufSize)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && (!errors.Is(err, io.EOF) || n <= 0) {
		return sc, err
	}

	if err := json.Unmarshal(buf[:n], &sc); err != nil {
		return Sidecar{}, fmt.Errorf("metadata %s on %s: %w", scPath, b.GetName(), err)
	}
	if sc.V != sidecarFormatVersion {
		return Sidecar{}, fmt.Errorf("metadata %s on %s: unsupported format version %d", scPath, b.GetName(), sc.V)
	}

	return sc, nil
}

// sidecarGen returns the generation recorded in path's metadata document on
// backend b. A missing document means a legacy (pre-versioning) replica and
// reports generation 0; any other error also degrades to generation 0 (with a
// debug log) so reconciliation can proceed without that backend's version
// knowledge.
func sidecarGen(ctx context.Context, b backend.Backend, path string) int64 {
	sc, err := ReadMeta(ctx, b, path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
			observability.Logger(ctx).Debug("vfs: metadata read failed",
				slog.String("path", path),
				slog.String("backend", b.GetName()),
				slog.Any("error", err),
			)
		}

		return 0
	}

	return sc.Gen
}

// WriteSidecar encodes sc as path's live metadata document on backend b,
// creating parent directories as needed. Deleted is forced false (a write
// session supersedes any prior tombstone) and the format version is set
// internally.
func WriteSidecar(ctx context.Context, b backend.Backend, path string, sc Sidecar) error {
	sc.Deleted = false

	return writeMetaDoc(ctx, b, path, sc)
}

// WriteTombstone encodes sc as path's deletion tombstone on backend b, creating
// parent directories as needed. Deleted is forced true and the format version
// is set internally.
func WriteTombstone(ctx context.Context, b backend.Backend, path string, sc Sidecar) error {
	sc.Deleted = true

	return writeMetaDoc(ctx, b, path, sc)
}

// writeMetaDoc stamps the format version and data path into sc and writes it to
// path's metadata document on backend b.
func writeMetaDoc(ctx context.Context, b backend.Backend, path string, sc Sidecar) error {
	sc.V = sidecarFormatVersion
	sc.Path = path
	data, err := json.Marshal(sc)
	if err != nil {
		return err
	}

	scPath := MetaPath(path)
	if dir := gopath.Dir(scPath); dir != "." {
		if err := b.MkdirAll(ctx, dir, 0755); err != nil {
			return fmt.Errorf("metadata dir %s on %s: %w", dir, b.GetName(), err)
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

// RemoveMeta deletes path's metadata document from backend b. An already-absent
// document counts as success (idempotent remove). Used to reclaim metadata for
// a path that no longer exists anywhere; a deletion that must remain discoverable
// is recorded with WriteTombstone instead.
func RemoveMeta(ctx context.Context, b backend.Backend, path string) error {
	err := b.Remove(ctx, MetaPath(path))
	if err == nil || os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

// RemoveSidecar is an alias of RemoveMeta kept for call-site readability.
func RemoveSidecar(ctx context.Context, b backend.Backend, path string) error {
	return RemoveMeta(ctx, b, path)
}
