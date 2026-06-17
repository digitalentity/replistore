package smb

import (
	"context"
	"sync"
	"time"

	"go.kvsh.ch/smb2"
)

// openScopedCtx bounds a single OpenFile round-trip by the caller's context
// while leaving the resulting handle governed by the backend lifecycle context.
//
// go-smb2 binds every *smb2.File to the context of the *smb2.Share that opened
// it (file RPCs read f.fs.ctx), so the open and all later reads/writes would
// otherwise share one context. This wrapper delegates to the caller context
// until release is called, then to the lifecycle context, giving the open a
// caller-scoped deadline without that deadline outliving the open and killing
// subsequent I/O on the handle.
type openScopedCtx struct {
	mu sync.Mutex
	//nolint:containedctx // by design: this type phases the open deadline into the lifecycle ctx.
	current context.Context
	//nolint:containedctx // by design: see current.
	lifecycle context.Context
}

func newOpenScopedCtx(call, lifecycle context.Context) *openScopedCtx {
	return &openScopedCtx{current: call, lifecycle: lifecycle}
}

// release detaches from the caller context. Subsequent reads observe the
// lifecycle context only. It must be called once the open has returned.
func (c *openScopedCtx) release() {
	c.mu.Lock()
	c.current = c.lifecycle
	c.mu.Unlock()
}

func (c *openScopedCtx) active() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.current
}

func (c *openScopedCtx) Deadline() (time.Time, bool) { return c.active().Deadline() }
func (c *openScopedCtx) Done() <-chan struct{}       { return c.active().Done() }
func (c *openScopedCtx) Err() error                  { return c.active().Err() }
func (c *openScopedCtx) Value(key any) any           { return c.active().Value(key) }

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
