package observability_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/digitalentity/replistore/internal/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservability(t *testing.T) {
	err := observability.Init("debug", "json", "test-node")
	require.NoError(t, err)

	t.Run("GenerateCorrelationID produces valid base36 values", func(t *testing.T) {
		id1 := observability.GenerateCorrelationID()
		id2 := observability.GenerateCorrelationID()

		assert.NotEmpty(t, id1)
		assert.NotEmpty(t, id2)
		assert.NotEqual(t, id1, id2)

		// Check if it's alphanumeric lowercase (valid base36)
		for _, r := range id1 {
			assert.True(t, (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
		}
	})

	t.Run("Context storage and retrieval", func(t *testing.T) {
		ctx := context.Background()
		assert.Empty(t, observability.CorrelationID(ctx))

		id := "123abc456"
		ctxWithID := observability.WithCorrelationID(ctx, id)
		assert.Equal(t, id, observability.CorrelationID(ctxWithID))

		logger := observability.Logger(ctxWithID)
		assert.NotNil(t, logger)
	})

	t.Run("Requestor storage and retrieval", func(t *testing.T) {
		ctx := context.Background()
		_, ok := observability.RequestorFrom(ctx)
		assert.False(t, ok)

		want := observability.Requestor{PID: 4242, UID: 1000, GID: 1000}
		ctxWithReq := observability.WithRequestor(ctx, want)
		got, ok := observability.RequestorFrom(ctxWithReq)
		require.True(t, ok)
		assert.Equal(t, want, got)

		assert.NotNil(t, observability.Logger(ctxWithReq))
	})
}

func TestTrace(t *testing.T) {
	newLogger := func() (*slog.Logger, *bytes.Buffer) {
		buf := &bytes.Buffer{}
		h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})

		return slog.New(h), buf
	}

	t.Run("WithCorrelationID begins a trace", func(t *testing.T) {
		assert.Nil(t, observability.TraceFrom(context.Background()))

		ctx := observability.WithCorrelationID(context.Background(), "abc")
		require.NotNil(t, observability.TraceFrom(ctx))
	})

	t.Run("FlushOnError dumps events on error, stays quiet on success", func(t *testing.T) {
		ctx := observability.WithCorrelationID(context.Background(), "abc")
		observability.Event(ctx, "step one", slog.Int("n", 1))
		observability.Event(ctx, "step two")
		tr := observability.TraceFrom(ctx)

		logger, buf := newLogger()
		tr.FlushOnError(ctx, logger, slog.LevelWarn, nil)
		assert.Empty(t, buf.String(), "no error -> no dump")

		tr.FlushOnError(ctx, logger, slog.LevelWarn, errors.New("boom"))
		out := buf.String()
		assert.Contains(t, out, "request trace dump")
		assert.Contains(t, out, "reason=boom")
		assert.Contains(t, out, "step one")
		assert.Contains(t, out, "step two")
		assert.Contains(t, out, "events=2")
	})

	t.Run("Event without a trace logs an observability gap", func(t *testing.T) {
		logger, buf := newLogger()
		prev := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(prev)

		observability.Event(context.Background(), "orphan event")
		out := buf.String()
		assert.Contains(t, out, "observability gap")
		assert.Contains(t, out, "orphan event")
	})

	t.Run("nil trace methods are safe", func(t *testing.T) {
		var tr *observability.Trace
		logger, buf := newLogger()
		assert.NotPanics(t, func() {
			tr.FlushOnError(context.Background(), logger, slog.LevelWarn, errors.New("boom"))
			tr.Flush(context.Background(), logger, slog.LevelWarn, "x")
		})
		assert.Empty(t, buf.String())
	})
}
