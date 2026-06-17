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
	"sync/atomic"
	"time"

	"github.com/bwmarrin/snowflake"
	slogmulti "github.com/samber/slog-multi"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

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
		baseHandler = slog.NewTextHandler(os.Stdout, opts)
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

// WithCorrelationID stores a correlation ID in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationID retrieves the correlation ID from the context.
func CorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey).(string); ok {
		return id
	}

	return ""
}

// Logger returns a contextual logger pre-populated with the correlation_id if present.
func Logger(ctx context.Context) *slog.Logger {
	logger := slog.Default()
	if ctx == nil {
		return logger
	}
	if id := CorrelationID(ctx); id != "" {
		logger = logger.With(slog.String("correlation_id", id))
	}

	return logger
}
