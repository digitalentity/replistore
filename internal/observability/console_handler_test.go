package observability

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsoleHandler(t *testing.T) {
	var buf bytes.Buffer
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelDebug)

	opts := &slog.HandlerOptions{
		Level: levelVar,
	}
	handler := newConsoleHandler(&buf, opts)
	logger := slog.New(handler)

	fixedTime := time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC)

	t.Run("Logs message with correct level and color", func(t *testing.T) {
		buf.Reset()
		r := slog.NewRecord(fixedTime, slog.LevelInfo, "hello world", 0)
		err := handler.Handle(context.Background(), r)
		require.NoError(t, err)

		output := buf.String()
		// Expect timestamp
		assert.Contains(t, output, "[2026-06-19 15:00:00.000]")
		// Expect green colorized level INFO
		assert.Contains(t, output, "\033[32m[INFO]\033[0m")
		// Expect message
		assert.Contains(t, output, "hello world")
	})

	t.Run("Logs debug level with cyan", func(t *testing.T) {
		buf.Reset()
		r := slog.NewRecord(fixedTime, slog.LevelDebug, "debug msg", 0)
		err := handler.Handle(context.Background(), r)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "\033[36m[DEBU]\033[0m")
		assert.Contains(t, output, "debug msg")
	})

	t.Run("Logs warn level with yellow", func(t *testing.T) {
		buf.Reset()
		r := slog.NewRecord(fixedTime, slog.LevelWarn, "warn msg", 0)
		err := handler.Handle(context.Background(), r)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "\033[33m[WARN]\033[0m")
	})

	t.Run("Logs error level with red", func(t *testing.T) {
		buf.Reset()
		r := slog.NewRecord(fixedTime, slog.LevelError, "error msg", 0)
		err := handler.Handle(context.Background(), r)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "\033[31m[ERRO]\033[0m")
	})

	t.Run("Includes attributes with cyan colorized keys", func(t *testing.T) {
		buf.Reset()
		logger.Info("user event", slog.String("user", "alice"), slog.Int("attempts", 3))

		output := buf.String()
		assert.Contains(t, output, "\033[36muser\033[0m=alice")
		assert.Contains(t, output, "\033[36mattempts\033[0m=3")
	})

	t.Run("Quotes values with special characters", func(t *testing.T) {
		buf.Reset()
		logger.Info("msg", slog.String("phrase", "hello world"))

		output := buf.String()
		assert.Contains(t, output, "\033[36mphrase\033[0m=\"hello world\"")
	})

	t.Run("Supports WithAttrs", func(t *testing.T) {
		buf.Reset()
		subLogger := logger.With(slog.String("component", "auth"))
		subLogger.Info("login success", slog.String("username", "bob"))

		output := buf.String()
		assert.Contains(t, output, "\033[36mcomponent\033[0m=auth")
		assert.Contains(t, output, "\033[36musername\033[0m=bob")
	})

	t.Run("Supports groups and WithGroup", func(t *testing.T) {
		buf.Reset()
		subLogger := logger.WithGroup("request").With(slog.String("method", "GET"))
		subLogger.Info("handled", slog.Int("status", 200))

		output := buf.String()
		assert.Contains(t, output, "\033[36mrequest.method\033[0m=GET")
		assert.Contains(t, output, "\033[36mrequest.status\033[0m=200")
	})
}
