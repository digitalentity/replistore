# RepliStore Agent Instructions

This document provides foundational mandates and technical context for AI agents working on RepliStore.

These instructions take precedence over general defaults.

## Engineering Standards

- **Minimalism:** Prefer the Go standard library (`net/rpc`, `net/http`, etc.). External dependencies must be justified and reviewed for "bloat."
- **I/O Resilience:** All backend and cluster RPC operations must support `context.Context` with appropriate timeouts.
- **Logging:** Use `sirupsen/logrus`. Component-specific loggers should include `component` and `path` or `node_id` fields.
- **Concurrency:** Use `golang.org/x/sync/errgroup` for parallel fan-out operations (e.g., writing to multiple replicas).

## Validation Workflow

After any code modification:
1.  **Build Check:** Run `go build ./cmd/replistore/...` to ensure compilation.
2.  **Test Suite:** Run `go test ./...`. RepliStore relies heavily on mock-based testing in `internal/test/` to verify distributed logic without actual NAS hardware.
3.  **Documentation:** If a feature adds a configuration field or changes a flow, update the corresponding file in `docs/`.
