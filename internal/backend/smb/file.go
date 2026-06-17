package smb

import (
	"context"

	"github.com/cloudsoda/go-smb2"
)

// smbFile wraps an open *smb2.File. Unlike share-level operations, file I/O does
// not route through execute: a reconnect invalidates the underlying handle, so a
// transparent retry on a fresh share would operate on a dead file descriptor.
// The per-call context is therefore honored as a cancellation pre-check only.
type smbFile struct {
	*smb2.File
	backend *SMBBackend
}

func (f *smbFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	defer f.backend.watchdog(ctx)()

	return f.File.ReadAt(b, off)
}

func (f *smbFile) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	defer f.backend.watchdog(ctx)()

	return f.File.WriteAt(b, off)
}

func (f *smbFile) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	defer f.backend.watchdog(ctx)()

	return f.File.Sync()
}
