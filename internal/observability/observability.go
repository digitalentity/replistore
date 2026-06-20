// Package observability provides structured logging, correlation ID generation, and context-bound tracing.
package observability

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/snowflake"
	slogmulti "github.com/samber/slog-multi"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

const traceKey contextKey = "trace"

const requestorKey contextKey = "requestor"

// defaultTraceCap bounds the events one trace retains. A single request records
// well under this; the cap only guards against a pathological loop. When full
// the oldest events are dropped (the failure tail is the interesting part).
const defaultTraceCap = 256

// Requestor identifies the process that issued a request (e.g. the userspace
// process behind a FUSE operation). Unlike correlation_id, which is a fresh
// per-operation token, this points at the actual initiator.
type Requestor struct {
	PID uint32
	UID uint32
	GID uint32
}

var sfNode *snowflake.Node

var (
	fsOpsTotal      = make(map[string]*atomic.Uint64)
	fsOpsDurationNs = make(map[string]*atomic.Uint64)
)

func init() {
	ops := []string{"lookup", "read", "write", "create", "mkdir", "remove", "rename", "attr", "setattr", "fsync", "open", "release", "flush", "read_dir_all"}
	for _, op := range ops {
		fsOpsTotal[op] = &atomic.Uint64{}
		fsOpsDurationNs[op] = &atomic.Uint64{}
	}
}

func RecordFSOp(op string, start time.Time) {
	if c, ok := fsOpsTotal[op]; ok {
		c.Add(1)
	}
	if d, ok := fsOpsDurationNs[op]; ok {
		ns := time.Since(start).Nanoseconds()
		if ns >= 0 {
			d.Add(uint64(ns))
		}
	}
}

func GetFSOpMetrics() (map[string]uint64, map[string]uint64) {
	totals := make(map[string]uint64)
	durations := make(map[string]uint64)
	for op, c := range fsOpsTotal {
		totals[op] = c.Load()
	}
	for op, d := range fsOpsDurationNs {
		durations[op] = d.Load()
	}

	return totals, durations
}

// Init initializes the global logger and Snowflake ID generator.
func Init(levelStr, formatStr, nodeID string) error {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	case "info":
		fallthrough
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var baseHandler slog.Handler
	if strings.ToLower(formatStr) == "json" {
		baseHandler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		baseHandler = newConsoleHandler(os.Stdout, opts)
	}

	// Use slogmulti to compose handlers as per requirements
	handler := slogmulti.Pipe().Handler(baseHandler)

	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Initialize Snowflake Node
	workerID := hashNodeID(nodeID)
	node, err := snowflake.NewNode(workerID)
	if err != nil {
		return fmt.Errorf("failed to create snowflake node: %w", err)
	}
	sfNode = node

	return nil
}

func hashNodeID(nodeID string) int64 {
	const maxSnowflakeNodeID = 1024
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeID))

	return int64(h.Sum32() % maxSnowflakeNodeID)
}

// GenerateCorrelationID generates a base36-encoded Snowflake ID.
func GenerateCorrelationID() string {
	if sfNode == nil {
		var b [8]byte
		_, _ = rand.Read(b[:])

		return strconv.FormatUint(binary.BigEndian.Uint64(b[:]), 36)
	}

	return strconv.FormatInt(sfNode.Generate().Int64(), 36)
}

// WithCorrelationID stores a correlation ID in the context and begins a trace
// bound to it. The trace is the lifecycle twin of the correlation ID: minting
// the ID starts recording events (see Event); the trace is dumped on demand
// (see Trace.FlushOnError) and otherwise discarded with the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	ctx = context.WithValue(ctx, correlationIDKey, id)

	return context.WithValue(ctx, traceKey, newTrace(id))
}

// CorrelationID retrieves the correlation ID from the context.
func CorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey).(string); ok {
		return id
	}

	return ""
}

// traceEvent is one breadcrumb recorded along a request's path.
type traceEvent struct {
	at    time.Time
	msg   string
	attrs []slog.Attr
}

// Trace accumulates breadcrumb events for a single correlation ID. Recording is
// cheap (an in-memory append) so callers can annotate the hot path freely; the
// events are emitted to the log only when something goes wrong (FlushOnError),
// keeping the steady-state log quiet. Safe for concurrent recording from the
// goroutines a single request fans out into.
type Trace struct {
	id      string
	start   time.Time
	mu      sync.Mutex
	events  []traceEvent
	dropped int
}

func newTrace(id string) *Trace {
	return &Trace{id: id, start: time.Now()}
}

func (t *Trace) add(msg string, attrs []slog.Attr) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Ring behaviour: once full, drop the oldest so the failure tail survives.
	if len(t.events) >= defaultTraceCap {
		t.events = t.events[1:]
		t.dropped++
	}
	t.events = append(t.events, traceEvent{at: time.Now(), msg: msg, attrs: attrs})
}

// Flush emits the recorded events to logger at the given level: a header line
// then one line per breadcrumb carrying a "+<elapsed>" offset from trace start.
// It is a no-op when nothing was recorded. The trace is left intact (callers may
// flush more than once, though typically they do not).
func (t *Trace) Flush(ctx context.Context, logger *slog.Logger, level slog.Level, reason string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.events) == 0 {
		return
	}

	logger.LogAttrs(ctx, level, "request trace dump",
		slog.String("reason", reason),
		slog.Int("events", len(t.events)),
		slog.Int("dropped", t.dropped),
		slog.Duration("elapsed", time.Since(t.start)),
	)
	for _, e := range t.events {
		attrs := make([]slog.Attr, 0, len(e.attrs)+1)
		attrs = append(attrs, slog.Duration("at", e.at.Sub(t.start)))
		attrs = append(attrs, e.attrs...)
		logger.LogAttrs(ctx, level, "  trace: "+e.msg, attrs...)
	}
}

// FlushOnError dumps the trace only when err is non-nil; otherwise it discards
// the breadcrumbs silently. This is the intended steady-state path: quiet on
// success, full request path on failure.
func (t *Trace) FlushOnError(ctx context.Context, logger *slog.Logger, level slog.Level, err error) {
	if t == nil || err == nil {
		return
	}
	t.Flush(ctx, logger, level, err.Error())
}

// TraceFrom retrieves the trace bound to the context's correlation ID, or nil.
func TraceFrom(ctx context.Context) *Trace {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(traceKey).(*Trace)

	return t
}

// Event records a breadcrumb on the context's trace. A trace is created
// whenever a correlation ID is minted (WithCorrelationID), so every properly
// instrumented request path has one. Calling Event without a trace in context
// means the breadcrumb is lost: that is an observability gap (a code path that
// skipped correlation setup), so it is logged rather than silently dropped.
func Event(ctx context.Context, msg string, attrs ...slog.Attr) {
	t := TraceFrom(ctx)
	if t == nil {
		Logger(ctx).LogAttrs(ctx, slog.LevelWarn,
			"observability gap: trace event recorded without an active trace",
			append([]slog.Attr{slog.String("event", msg)}, attrs...)...)

		return
	}
	t.add(msg, attrs)
}

// WithRequestor stores the issuing process identity in the context.
func WithRequestor(ctx context.Context, r Requestor) context.Context {
	return context.WithValue(ctx, requestorKey, r)
}

// RequestorFrom retrieves the issuing process identity from the context.
func RequestorFrom(ctx context.Context) (Requestor, bool) {
	r, ok := ctx.Value(requestorKey).(Requestor)

	return r, ok
}

// Logger returns a contextual logger pre-populated with the correlation_id and
// the requesting process identity (pid/uid/gid) if present.
func Logger(ctx context.Context) *slog.Logger {
	logger := slog.Default()
	if ctx == nil {
		return logger
	}
	if id := CorrelationID(ctx); id != "" {
		logger = logger.With(slog.String("correlation_id", id))
	}
	if r, ok := RequestorFrom(ctx); ok {
		logger = logger.With(
			slog.Uint64("pid", uint64(r.PID)),
			slog.Uint64("uid", uint64(r.UID)),
			slog.Uint64("gid", uint64(r.GID)),
		)
	}

	return logger
}
