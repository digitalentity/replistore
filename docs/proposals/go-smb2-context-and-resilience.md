# Proposal: go-smb2 Context Plumbing and Resilience

This document collects defects and gaps found in
[`github.com/cloudsoda/go-smb2`](https://github.com/cloudsoda/go-smb2)
(pinned `v0.0.0-20260609183447-7b96c35f5f4b`) while building the RepliStore SMB
backend, and specifies the changes we intend to make in our fork.

All line references are to the pinned revision in the module cache.

---

## Motivation

The RepliStore SMB backend (`internal/backend/smb`) must cancel a single
in-flight file read/write on a per-request `context.Context` (one FUSE
operation), without disturbing other concurrent operations on the same backend.

The library makes this impossible today, so the backend resorts to a
**watchdog** that force-closes the shared TCP connection when a request's
context is cancelled (`internal/backend/smb/backend.go`). That is a blunt
instrument: closing the shared connection unblocks the stuck call but also tears
down every other in-flight operation on the same backend, and forces a full
reconnect.

The irony is that the library's connection layer **already supports clean,
isolated, per-request cancellation** — it is simply not reachable from the
`*smb2.File` API. The bulk of this proposal is about exposing what already
exists.

---

## 1. `*File` freezes its context at open time (core issue)

### Current State
- `Share` carries a `ctx` and exposes `Share.WithContext(ctx) *Share`
  (`client.go:322`, `client.go:326`).
- A `*File` holds a back-pointer to the `Share` it was opened on:
  `f := &File{fs: fs, ...}` (`client.go:362`).
- Every file operation routes through `File.sendRecv` →
  `f.fs.sendRecv` → `fs.send(fs.ctx, req)` (`client.go:2454`, `client.go:1249`).
- **`*File` exposes no `WithContext`.** The context used for `ReadAt`,
  `WriteAt`, `Sync`, etc. is whatever the `Share` carried *at the moment the
  file was opened*, frozen for the lifetime of the handle.

### Impact
- `ReadAt(ctx, ...)`-style per-call cancellation cannot be expressed. The caller
  cannot supply a deadline for an individual read distinct from the open-time
  context.
- RepliStore therefore force-closes the shared connection to interrupt a stuck
  file call (`watchdog` in `backend.go`), with the blast radius described above.

### Why this is cheap to fix
Cancellation is **already isolated per request** at the connection layer:
- `conn.recv` selects on `rr.ctx.Done()` and `pop`s only that request's
  `msgId` (`conn.go:672`–`684`).
- `conn.send` selects on `ctx.Done()` (`conn.go:365`–`402`).
- A background demux loop (`runReceiver`) routes responses by `msgId`, so
  cancelling one request leaves all others untouched.

So a per-operation context, plumbed to `fs.ctx`, gives clean cancellation with
**no** connection teardown.

### Proposed Design
Add `File.WithContext`, mirroring `Share.WithContext`, returning a shallow copy
bound to a context-scoped share:

```go
// WithContext returns a shallow copy of f whose operations use ctx for
// cancellation and deadlines. The underlying file handle (fd) is shared; only
// the request context changes. Concurrent use of the original and the returned
// *File against the same fd is the caller's responsibility, exactly as with
// Share.WithContext.
func (f *File) WithContext(ctx context.Context) *File {
	if ctx == nil {
		panic("nil context")
	}
	nf := *f
	nf.fs = f.fs.WithContext(ctx)
	return &nf
}
```

Caveats to resolve in the fork:
- `File` carries a `sync.Mutex` (`m`) and mutable `offset`/`dirents` state
  (`client.go:1268`+). A shallow copy duplicates the mutex and offset, which is
  correct for stateless positional ops (`ReadAt`/`WriteAt`/`Sync`/`Stat`/
  `Truncate`) but **not** for stateful streaming (`Read`/`Write`/`Seek`/
  `Readdir`). Document `WithContext` as intended for positional/one-shot ops, or
  share the offset/mutex via pointer indirection so both views stay consistent.
- The `runtime.SetFinalizer(f, (*File).close)` on the original (`client.go:362`)
  must not be duplicated onto the copy (a shallow copy does not inherit the
  finalizer, which is the desired behavior — only the original owns the fd
  lifecycle).

### Downstream effect on RepliStore
`internal/backend/smb/file.go` becomes:

```go
func (f *smbFile) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	return f.File.WithContext(ctx).ReadAt(b, off)
}
```

and the entire `watchdog` / `resetConnLocked` machinery in `backend.go` is
deleted. No more shared-connection blast radius.

### Alternative (if a `*File` copy is undesirable)
Add explicit context-taking variants and keep the old ones as wrappers:

```go
func (f *File) ReadAtContext(ctx context.Context, b []byte, off int64) (int, error)
func (f *File) WriteAtContext(ctx context.Context, b []byte, off int64) (int, error)
func (f *File) SyncContext(ctx context.Context) error
// ...
```

`WithContext` is preferred: one method, no per-op surface growth, matches the
existing `Share`/`Session` pattern.

---

## 2. `File.Close` inherits the (possibly cancelled) open-time context

### Current State
- `File.close` sends an `SMB2_CLOSE` via `f.sendRecv` → `f.fs.ctx`
  (`client.go:1293`–`1320`).
- If the file was opened on a request-scoped context and that context is later
  cancelled, **the close request itself is cancelled** and the server-side file
  handle is never released.

### Impact
- Server-side handle/fd leak on the very common path "open with request ctx,
  request gets cancelled, defer Close()".
- Repeated under load this exhausts server handle limits.

### Proposed Design
- `Close` must use a context independent of the operation context — either
  `context.Background()` or a short bounded `context.WithTimeout` derived from
  `Background`, never the cancelled `fs.ctx`.
- Optionally expose `CloseContext(ctx)` for callers that want to bound the close
  themselves, with `Close()` defaulting to a background-with-timeout context.

---

## 3. Resource cleanup relies on `runtime.SetFinalizer`

### Current State
- `newFile` registers `runtime.SetFinalizer(f, (*File).close)` (`client.go:362`),
  cleared on explicit close (`client.go:1318`).

### Impact
- Non-deterministic cleanup: the server handle is released whenever the GC
  happens to run, not when the logical resource is done.
- The finalizer path calls `close`, which (per §2) uses `f.fs.ctx`; if that
  context is dead, the finalizer close fails silently and the leak is permanent.
- Finalizers are an anti-pattern for network resources with explicit lifecycles.

### Proposed Design
- Keep the finalizer only as a last-resort leak *detector* (log a warning when a
  `*File` is GC'd without an explicit `Close`), not as the primary cleanup path.
- Ensure the finalizer's close uses a background context (ties into §2).

---

## 4. No idle/read deadline or keepalive on the connection

### Current State
- There is no automatic read deadline, idle timeout, or keepalive loop on the
  transport. `Session.Echo` exists (`client.go:170`, `session.go:294`) but must
  be driven manually.
- `conn.recv` blocks until either a response arrives or `rr.ctx` is cancelled
  (`conn.go:672`).

### Impact
- With a context that has **no deadline** (e.g. `context.Background()`) and a
  TCP connection that dies without a FIN/RST (NAS power loss, mid-path firewall
  drop), `recv` blocks **forever**. The application appears hung with no error.
- This is why RepliStore must attach a deadline to every operation context and
  run its own connection monitor — work the library could centralize.

### Proposed Design
- Add an optional `Dialer`/`Session` setting for a background keepalive
  (periodic `Echo`) that closes the connection and fails outstanding requests
  on N consecutive failures.
- Add an optional per-request default deadline at the `Dialer` level so a
  caller that forgets to bound a context still gets a timeout instead of an
  infinite hang.

---

## 5. Session/Share context coupling is a footgun

### Current State
- `Dialer.DialConn(ctx, ...)` stores `ctx` as the session's default context
  (`client.go:87`, `session.go`), and `Mount` derives the share from it
  (`client.go:221`, `fs = fs.WithContext(c.ctx)`).
- Cancelling the context passed to `Dial`/`DialConn` therefore tears down the
  **entire session**, not just the dial.

### Impact
- Easy to accidentally pass a request-scoped context to `DialConn` and have the
  whole session die when that one request ends. RepliStore deliberately dials
  with `context.Background()` and applies `WithContext` per operation
  (`internal/backend/smb/backend.go:125`+) to avoid this, but the trap is
  unmarked.

### Proposed Design
- Treat the dial context strictly as "context for the dial handshake only" and
  do **not** retain it as the session/share default. Default the session to
  `context.Background()` and require explicit `WithContext` for operation
  scoping.
- At minimum, document the coupling loudly on `DialConn`.

---

## 6. Error classification requires string matching

### Current State
- The library surfaces connection-death as a mix of `io.EOF`, `net.OpError`,
  raw `syscall.Errno`, and "use of closed network connection" string errors.
- RepliStore's `isConnectionError` resorts to `strings.Contains` on lowercased
  messages (`internal/backend/smb/backend.go:189`+) to decide whether to
  reconnect.

### Impact
- Fragile, locale/version-sensitive classification of "is the connection dead,
  should I reconnect?".

### Proposed Design
- Export typed sentinel errors (e.g. `ErrConnectionLost`,
  `ErrSessionExpired`) and wrap transport failures so callers can use
  `errors.Is`, eliminating string matching.

---

## Summary of fork changes (priority order)

| # | Change | Effort | RepliStore payoff |
|---|--------|--------|-------------------|
| 1 | `File.WithContext` (or `*Context` op variants) | Small | Deletes the watchdog; isolated per-op cancellation |
| 2 | `Close` uses background context | Small | Stops server handle leaks on cancel |
| 3 | Finalizer demoted to leak detector | Small | Deterministic cleanup |
| 4 | Optional keepalive + default deadline | Medium | Removes infinite-hang failure mode |
| 5 | Decouple dial ctx from session ctx | Small | Removes a silent footgun |
| 6 | Typed sentinel connection errors | Small | Kills `strings.Contains` classification |

Item **1** is the keystone: it alone lets RepliStore drop the connection-killing
watchdog (`internal/backend/smb/backend.go`) and replace it with plain
per-operation contexts, because the connection layer already cancels requests
cleanly and in isolation (`conn.go:672`–`684`).

---

## Fork logistics

- Fork `cloudsoda/go-smb2` into a separate repository under our control.
- Land the changes above behind additive APIs where possible (`WithContext`,
  `*Context` variants, typed errors) so rebasing onto upstream stays cheap.
- Replace the `go-smb2` require in `go.mod` with a `replace` directive (or a
  retagged fork module path) once the fork carries item 1.
- Upstream items 1, 2, 5, 6 as PRs — they are general-purpose correctness fixes,
  not RepliStore-specific, and reducing fork drift is worth the effort.
