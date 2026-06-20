package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
)

type consoleHandler struct {
	opts   slog.HandlerOptions
	out    io.Writer
	mu     *sync.Mutex
	attrs  []slog.Attr
	groups []string
}

func newConsoleHandler(out io.Writer, opts *slog.HandlerOptions) *consoleHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}

	return &consoleHandler{
		opts: *opts,
		out:  out,
		mu:   &sync.Mutex{},
	}
}

func (h *consoleHandler) Enabled(ctx context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}

	return level >= minLevel
}

func (h *consoleHandler) Handle(ctx context.Context, r slog.Record) error {
	var sb strings.Builder

	// 1. Timestamp
	timestamp := r.Time.Format("2006-01-02 15:04:05.000")
	fmt.Fprintf(&sb, "[%s] ", timestamp)

	// 2. Level (with color)
	var levelStr string
	var colorCode string
	switch r.Level {
	case slog.LevelDebug:
		levelStr = "DEBU"
		colorCode = "36" // Cyan
	case slog.LevelInfo:
		levelStr = "INFO"
		colorCode = "32" // Green
	case slog.LevelWarn:
		levelStr = "WARN"
		colorCode = "33" // Yellow
	case slog.LevelError:
		levelStr = "ERRO"
		colorCode = "31" // Red
	default:
		levelStr = r.Level.String()
		colorCode = "35" // Magenta for custom/unknown levels
	}
	fmt.Fprintf(&sb, "\033[%sm[%s]\033[0m ", colorCode, levelStr)

	// 3. Message
	sb.WriteString(r.Message)

	// Prepend groups if any
	groupPrefix := ""
	if len(h.groups) > 0 {
		groupPrefix = strings.Join(h.groups, ".") + "."
	}

	// 4. Fields (pre-formatted attributes + record attributes)
	for _, a := range h.attrs {
		if formatted := h.formatAttr(groupPrefix, a); formatted != "" {
			sb.WriteString(" ")
			sb.WriteString(formatted)
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		if formatted := h.formatAttr(groupPrefix, a); formatted != "" {
			sb.WriteString(" ")
			sb.WriteString(formatted)
		}

		return true
	})

	sb.WriteString("\n")

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.out, sb.String())

	return err
}

func (h *consoleHandler) formatAttr(prefix string, attr slog.Attr) string {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return ""
	}

	if attr.Value.Kind() == slog.KindGroup {
		var sb strings.Builder
		groupName := attr.Key
		for _, a := range attr.Value.Group() {
			formatted := h.formatAttr(prefix+groupName+".", a)
			if formatted != "" {
				if sb.Len() > 0 {
					sb.WriteString(" ")
				}
				sb.WriteString(formatted)
			}
		}

		return sb.String()
	}

	valStr := attr.Value.String()
	// Quote if value contains whitespace, quotes, or equal signs
	if strings.ContainsAny(valStr, " \t\n\r\"=") {
		valStr = fmt.Sprintf("%q", valStr)
	}

	// Colorize the key name in cyan (36)
	return fmt.Sprintf("\033[36m%s%s\033[0m=%s", prefix, attr.Key, valStr)
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	newAttrs := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	newAttrs = append(newAttrs, h.attrs...)
	newAttrs = append(newAttrs, attrs...)

	return &consoleHandler{
		opts:   h.opts,
		out:    h.out,
		mu:     h.mu,
		attrs:  newAttrs,
		groups: h.groups,
	}
}

func (h *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	newGroups := make([]string, 0, len(h.groups)+1)
	newGroups = append(newGroups, h.groups...)
	newGroups = append(newGroups, name)

	return &consoleHandler{
		opts:   h.opts,
		out:    h.out,
		mu:     h.mu,
		attrs:  h.attrs,
		groups: newGroups,
	}
}
