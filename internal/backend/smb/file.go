package smb

import (
	"context"

	"go.kvsh.ch/smb2"
)

// smbFile wraps an open *smb2.File. File I/O does not route through execute: a
// reconnect invalidates the underlying handle, so a transparent retry on a fresh
// share would operate on a dead descriptor. Each call binds the handle to the
// caller context via File.WithContext, which honors cancellation and deadlines
// natively without affecting other calls on the same handle.
type smbFile struct {
	*smb2.File
}

func (f *smbFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	return f.File.WithContext(ctx).ReadAt(b, off)
}

func (f *smbFile) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	return f.File.WithContext(ctx).WriteAt(b, off)
}

func (f *smbFile) Sync(ctx context.Context) error {
	return f.File.WithContext(ctx).Sync()
}
