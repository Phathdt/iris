package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
	colorWhite  = "\033[97m"

	// Bold variants
	colorBoldRed    = "\033[1;31m"
	colorBoldGreen  = "\033[1;32m"
	colorBoldYellow = "\033[1;33m"
	colorBoldBlue   = "\033[1;34m"
	colorBoldCyan   = "\033[1;36m"
)

// ColoredTextHandler is a custom slog handler that outputs colored text
type ColoredTextHandler struct {
	opts   slog.HandlerOptions
	mu     *sync.Mutex
	out    io.Writer
	attrs  []slog.Attr
	groups []string
}

// NewColoredTextHandler creates a new colored text handler
func NewColoredTextHandler(out io.Writer, opts *slog.HandlerOptions) *ColoredTextHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &ColoredTextHandler{
		opts: *opts,
		mu:   &sync.Mutex{},
		out:  out,
	}
}

// Enabled reports whether the handler handles records at the given level
func (h *ColoredTextHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

// Handle formats and writes a log record
func (h *ColoredTextHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Format timestamp
	timeStr := r.Time.Format(time.DateTime)

	// Get level color and text
	levelColor, levelText := h.levelColorAndText(r.Level)

	// Build the log line
	var attrs string
	// Add handler-level attrs first
	for _, a := range h.attrs {
		attrs += h.formatAttr(a)
	}
	// Add record attrs
	r.Attrs(func(a slog.Attr) bool {
		attrs += h.formatAttr(a)
		return true
	})

	// Format: timestamp level message attrs
	line := fmt.Sprintf("%s%s%s %s%-5s%s %s%s%s%s\n",
		colorGray, timeStr, colorReset,
		levelColor, levelText, colorReset,
		colorWhite, r.Message, colorReset,
		attrs,
	)

	_, err := h.out.Write([]byte(line))
	return err
}

// WithAttrs returns a new handler with additional attributes
func (h *ColoredTextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &ColoredTextHandler{
		opts:   h.opts,
		mu:     h.mu,
		out:    h.out,
		attrs:  newAttrs,
		groups: h.groups,
	}
}

// WithGroup returns a new handler with a group prefix
func (h *ColoredTextHandler) WithGroup(name string) slog.Handler {
	newGroups := make([]string, len(h.groups)+1)
	copy(newGroups, h.groups)
	newGroups[len(h.groups)] = name
	return &ColoredTextHandler{
		opts:   h.opts,
		mu:     h.mu,
		out:    h.out,
		attrs:  h.attrs,
		groups: newGroups,
	}
}

// levelColorAndText returns the color and text for a log level
func (h *ColoredTextHandler) levelColorAndText(level slog.Level) (string, string) {
	switch {
	case level < slog.LevelInfo:
		return colorBoldCyan, "DEBUG"
	case level < slog.LevelWarn:
		return colorBoldGreen, "INFO"
	case level < slog.LevelError:
		return colorBoldYellow, "WARN"
	default:
		return colorBoldRed, "ERROR"
	}
}

// formatAttr formats a single attribute with colors
func (h *ColoredTextHandler) formatAttr(a slog.Attr) string {
	if a.Equal(slog.Attr{}) {
		return ""
	}

	key := a.Key
	// Apply group prefix
	for _, g := range h.groups {
		key = g + "." + key
	}

	// Color the key in cyan and value based on type
	value := a.Value.Resolve()
	var valueStr string
	var valueColor string

	switch value.Kind() {
	case slog.KindString:
		valueColor = colorGreen
		valueStr = value.String()
	case slog.KindInt64:
		valueColor = colorPurple
		valueStr = fmt.Sprintf("%d", value.Int64())
	case slog.KindUint64:
		valueColor = colorPurple
		valueStr = fmt.Sprintf("%d", value.Uint64())
	case slog.KindFloat64:
		valueColor = colorPurple
		valueStr = fmt.Sprintf("%g", value.Float64())
	case slog.KindBool:
		valueColor = colorYellow
		valueStr = fmt.Sprintf("%t", value.Bool())
	case slog.KindTime:
		valueColor = colorGray
		valueStr = value.Time().Format(time.RFC3339)
	case slog.KindDuration:
		valueColor = colorPurple
		valueStr = value.Duration().String()
	default:
		valueColor = colorWhite
		valueStr = fmt.Sprintf("%v", value.Any())
	}

	return fmt.Sprintf(" %s%s%s=%s%s%s", colorCyan, key, colorReset, valueColor, valueStr, colorReset)
}

// Logger is an interface for logging that can be swapped to other implementations
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Fatal(msg string, args ...any)
	With(args ...any) Logger
	WithGroup(name string) Logger
	DebugContext(ctx context.Context, msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// SlogLogger is a concrete implementation of Logger using slog
type SlogLogger struct {
	slog *slog.Logger
}

// New creates a new Logger with the specified format and level
// Supported formats: "json", "text" (colored), "plain" (no colors)
// Supported levels: "debug", "info", "warn", "error"
func New(format, level string) Logger {
	var handler slog.Handler

	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
	}

	switch format {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	case "plain":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default: // "text" or any other value uses colored output
		handler = NewColoredTextHandler(os.Stdout, opts)
	}

	return &SlogLogger{
		slog: slog.New(handler),
	}
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// With returns a new Logger with additional attributes
func (l *SlogLogger) With(args ...any) Logger {
	return &SlogLogger{slog: l.slog.With(args...)}
}

// WithGroup returns a new Logger with a group prefix
func (l *SlogLogger) WithGroup(name string) Logger {
	return &SlogLogger{slog: l.slog.WithGroup(name)}
}

// Debug logs at debug level
func (l *SlogLogger) Debug(msg string, args ...any) {
	l.slog.Debug(msg, args...)
}

// Info logs at info level
func (l *SlogLogger) Info(msg string, args ...any) {
	l.slog.Info(msg, args...)
}

// Warn logs at warn level
func (l *SlogLogger) Warn(msg string, args ...any) {
	l.slog.Warn(msg, args...)
}

// Error logs at error level
func (l *SlogLogger) Error(msg string, args ...any) {
	l.slog.Error(msg, args...)
}

// Fatal logs at error level and exits
func (l *SlogLogger) Fatal(msg string, args ...any) {
	l.slog.Error(msg, args...)
	os.Exit(1)
}

// DebugContext logs at debug level with context
func (l *SlogLogger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.slog.DebugContext(ctx, msg, args...)
}

// InfoContext logs at info level with context
func (l *SlogLogger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.slog.InfoContext(ctx, msg, args...)
}

// WarnContext logs at warn level with context
func (l *SlogLogger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.slog.WarnContext(ctx, msg, args...)
}

// ErrorContext logs at error level with context
func (l *SlogLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.slog.ErrorContext(ctx, msg, args...)
}

// Helper functions for structured logging attributes

// String creates a string attribute
func String(key, value string) slog.Attr {
	return slog.String(key, value)
}

// Int creates an integer attribute
func Int(key string, value int) slog.Attr {
	return slog.Int(key, value)
}

// Int64 creates an int64 attribute
func Int64(key string, value int64) slog.Attr {
	return slog.Int64(key, value)
}

// Bool creates a boolean attribute
func Bool(key string, value bool) slog.Attr {
	return slog.Bool(key, value)
}

// Any creates an attribute of any type
func Any(key string, value any) slog.Attr {
	return slog.Any(key, value)
}

// Err creates an error attribute
func Err(err error) slog.Attr {
	return slog.Any("error", err)
}
