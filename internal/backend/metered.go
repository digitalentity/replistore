package backend

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/digitalentity/replistore/internal/observability"
)

// meteredBackend wraps a Backend and records the latency of every remote
// operation as a Prometheus histogram, labelled by backend name, type, and
// operation. The embedded Backend supplies the un-timed accessors and lifecycle
// methods (GetName, GetType, Connect, Ping, Close, ...) unchanged; only the
// round-trip operations are overridden to time them.
type meteredBackend struct {
	Backend
	name  string
	btype string
}

// NewMetered wraps b so its remote operations are timed. Pings keep their own
// dedicated histogram (see RecordBackendPing), so they are not double-counted
// here.
//
//nolint:ireturn // mirrors the Backend interface returned by Create.
func NewMetered(b Backend) Backend {
	return &meteredBackend{Backend: b, name: b.GetName(), btype: b.GetType()}
}

// Unwrap returns the wrapped backend. Call sites that need the concrete backend
// type (e.g. for a type assertion) must unwrap first; see Unwrap.
//
//nolint:ireturn // returns the wrapped Backend interface by design.
func (m *meteredBackend) Unwrap() Backend {
	return m.Backend
}

// Unwrap returns the underlying backend if b is a metered wrapper, otherwise b
// itself. Use it before a type assertion to a concrete backend implementation.
//
//nolint:ireturn // returns the Backend interface by design.
func Unwrap(b Backend) Backend {
	if m, ok := b.(*meteredBackend); ok {
		return m.Backend
	}

	return b
}

func (m *meteredBackend) ReadDir(ctx context.Context, path string) ([]FileInfo, error) {
	start := time.Now()
	res, err := m.Backend.ReadDir(ctx, path)
	observability.RecordBackendOp(m.name, m.btype, "readdir", start, err)

	return res, err
}

func (m *meteredBackend) Stat(ctx context.Context, path string) (FileInfo, error) {
	start := time.Now()
	res, err := m.Backend.Stat(ctx, path)
	observability.RecordBackendOp(m.name, m.btype, "stat", start, err)

	return res, err
}

func (m *meteredBackend) Walk(ctx context.Context, path string, fn func(path string, info FileInfo) error) error {
	start := time.Now()
	err := m.Backend.Walk(ctx, path, fn)
	observability.RecordBackendOp(m.name, m.btype, "walk", start, err)

	return err
}

//nolint:ireturn // backend.File is an interface returned by implementation.
func (m *meteredBackend) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (File, error) {
	start := time.Now()
	f, err := m.Backend.OpenFile(ctx, path, flag, perm)
	observability.RecordBackendOp(m.name, m.btype, "open", start, err)
	if err != nil {
		return f, err
	}

	return &meteredFile{File: f, name: m.name, btype: m.btype}, nil
}

func (m *meteredBackend) Mkdir(ctx context.Context, path string, perm os.FileMode) error {
	start := time.Now()
	err := m.Backend.Mkdir(ctx, path, perm)
	observability.RecordBackendOp(m.name, m.btype, "mkdir", start, err)

	return err
}

func (m *meteredBackend) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	start := time.Now()
	err := m.Backend.MkdirAll(ctx, path, perm)
	observability.RecordBackendOp(m.name, m.btype, "mkdir_all", start, err)

	return err
}

func (m *meteredBackend) Remove(ctx context.Context, path string) error {
	start := time.Now()
	err := m.Backend.Remove(ctx, path)
	observability.RecordBackendOp(m.name, m.btype, "remove", start, err)

	return err
}

func (m *meteredBackend) Rename(ctx context.Context, oldPath, newPath string) error {
	start := time.Now()
	err := m.Backend.Rename(ctx, oldPath, newPath)
	observability.RecordBackendOp(m.name, m.btype, "rename", start, err)

	return err
}

func (m *meteredBackend) Chtimes(ctx context.Context, path string, atime, mtime time.Time) error {
	start := time.Now()
	err := m.Backend.Chtimes(ctx, path, atime, mtime)
	observability.RecordBackendOp(m.name, m.btype, "chtimes", start, err)

	return err
}

func (m *meteredBackend) Truncate(ctx context.Context, path string, size int64) error {
	start := time.Now()
	err := m.Backend.Truncate(ctx, path, size)
	observability.RecordBackendOp(m.name, m.btype, "truncate", start, err)

	return err
}

// meteredFile wraps a File so the per-handle data-path operations (read, write,
// sync) are timed against the owning backend. Close is left to the embedded File
// since it is a lifecycle call, not a data round-trip.
type meteredFile struct {
	File
	name  string
	btype string
}

func (f *meteredFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	start := time.Now()
	n, err := f.File.ReadAt(ctx, b, off)
	// io.EOF is the normal end-of-file signal, not an operation failure.
	recordErr := err
	if errors.Is(recordErr, io.EOF) {
		recordErr = nil
	}
	observability.RecordBackendOp(f.name, f.btype, "read", start, recordErr)
	observability.RecordBackendBytes(f.name, f.btype, "read", n)

	return n, err
}

func (f *meteredFile) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	start := time.Now()
	n, err := f.File.WriteAt(ctx, b, off)
	observability.RecordBackendOp(f.name, f.btype, "write", start, err)
	observability.RecordBackendBytes(f.name, f.btype, "write", n)

	return n, err
}

func (f *meteredFile) Sync(ctx context.Context) error {
	start := time.Now()
	err := f.File.Sync(ctx)
	observability.RecordBackendOp(f.name, f.btype, "sync", start, err)

	return err
}
