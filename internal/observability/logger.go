// Package observability provides structured JSON logging, context-based correlation ID
// propagation, and PII/credential masking helpers.
//
// WHY: Enriching every log line with correlation_id and masking phone numbers, emails, device
// tokens, and database passwords enables incident tracing across API -> Redis -> Worker without
// leaking sensitive customer PII to log aggregators.
//
// Expected output example:
//
//	{"time":"2026-11-20T19:50:00Z","level":"INFO","msg":"request accepted","correlation_id":"7d3c...","recipient":"+90555***4567"}
package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Unexported empty-struct context keys prevent collisions across unrelated packages.
type (
	correlationIDKey struct{}
	loggerKey        struct{}
	fieldsKey        struct{}
)

// NewLogger constructs a structured JSON slog.Logger configured at the requested log level.
//
// WHY: JSON output to stdout satisfies container log collectors while respecting LOG_LEVEL
// (debug, info, warn, error) without relying on package-global mutable loggers.
//
// Expected output example:
//
//	logger := observability.NewLogger("info", os.Stdout)
//	logger.Info("service started", "port", 8080)
func NewLogger(level string, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithCorrelationID stores a correlation ID in ctx for downstream logging and provider propagation.
//
// Expected output example:
//
//	ctx = observability.WithCorrelationID(ctx, "7d3c-corr-1")
//	observability.CorrelationID(ctx) // returns "7d3c-corr-1"
func WithCorrelationID(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, correlationIDKey{}, strings.TrimSpace(id))
}

// CorrelationID extracts the correlation ID from ctx, returning "" if unset.
func CorrelationID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return id
	}
	return ""
}

// WithLogger attaches a base slog.Logger to ctx.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerKey{}, l)
}

// WithFields appends structured slog.Attr fields to ctx so subsequent Logger(ctx) calls carry them.
//
// WHY: Workers can attach notification_id, batch_id, channel, and attempt once per item instead of
// repeating the same keys in every log statement along the delivery lifecycle.
func WithFields(ctx context.Context, attrs ...slog.Attr) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(attrs) == 0 {
		return ctx
	}
	existing, _ := ctx.Value(fieldsKey{}).([]slog.Attr)
	combined := make([]slog.Attr, 0, len(existing)+len(attrs))
	combined = append(combined, existing...)
	combined = append(combined, attrs...)
	return context.WithValue(ctx, fieldsKey{}, combined)
}

// Logger returns the logger from ctx enriched with correlation_id and any context fields.
// If no logger was attached to ctx, a fresh JSON stdout logger at info level is returned.
//
// Expected output example:
//
//	observability.Logger(ctx).Info("dequeued")
//	// Emits JSON with "correlation_id":"..." and any attributes set via WithFields.
func Logger(ctx context.Context) *slog.Logger {
	var base *slog.Logger
	if ctx != nil {
		if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
			base = l
		}
	}
	if base == nil {
		base = NewLogger("info", os.Stdout)
	}
	return LoggerFrom(ctx, base)
}

// LoggerFrom enriches an explicitly injected base logger with correlation_id and fields from ctx.
func LoggerFrom(ctx context.Context, base *slog.Logger) *slog.Logger {
	if base == nil {
		base = NewLogger("info", os.Stdout)
	}
	if ctx == nil {
		return base
	}

	var args []any
	if cid := CorrelationID(ctx); cid != "" {
		args = append(args, slog.String("correlation_id", cid))
	}
	if attrs, ok := ctx.Value(fieldsKey{}).([]slog.Attr); ok {
		for _, a := range attrs {
			args = append(args, a)
		}
	}
	if len(args) == 0 {
		return base
	}
	return base.With(args...)
}

// MaskPhone masks the middle digits of a phone number so logs remain GDPR/PII safe.
//
// Expected output example:
//
//	MaskPhone("+905551234567") // "+90555***4567"
//	MaskPhone("1234")          // "***"
func MaskPhone(phone string) string {
	r := []rune(strings.TrimSpace(phone))
	n := len(r)
	switch {
	case n == 0:
		return ""
	case n <= 4:
		return "***"
	case n <= 9:
		return string(r[:2]) + "***" + string(r[n-2:])
	default:
		return string(r[:6]) + "***" + string(r[n-4:])
	}
}

// MaskEmail masks the local-part of an email address while preserving the first character and domain.
//
// Expected output example:
//
//	MaskEmail("ali@example.com") // "a***@example.com"
func MaskEmail(email string) string {
	trimmed := strings.TrimSpace(email)
	if trimmed == "" {
		return ""
	}
	at := strings.IndexByte(trimmed, '@')
	if at <= 0 || at == len(trimmed)-1 {
		return "***"
	}
	local := []rune(trimmed[:at])
	domain := trimmed[at:]
	return string(local[:1]) + "***" + domain
}

// MaskToken masks push device tokens, keeping only a tiny prefix/suffix for correlation.
//
// Expected output example:
//
//	MaskToken("fcm_abcdef1234567890xyz") // "fcm_***0xyz"
func MaskToken(token string) string {
	r := []rune(strings.TrimSpace(token))
	n := len(r)
	if n == 0 {
		return ""
	}
	if n <= 8 {
		return "***"
	}
	return string(r[:4]) + "***" + string(r[n-4:])
}

// MaskDatabaseURL redacts the password segment of a Postgres DSN for safe startup logging.
//
// Expected output example:
//
//	MaskDatabaseURL("postgres://notif:notif@postgres:5432/notif") // "postgres://notif:***@postgres:5432/notif"
func MaskDatabaseURL(raw string) string {
	schemeIdx := strings.Index(raw, "://")
	if schemeIdx < 0 {
		return "***"
	}
	rest := raw[schemeIdx+3:]
	authEnd := strings.IndexAny(rest, "/?#")
	if authEnd < 0 {
		authEnd = len(rest)
	}
	authority := rest[:authEnd]
	atIdx := strings.LastIndex(authority, "@")
	if atIdx < 0 {
		return raw
	}
	userInfo := authority[:atIdx]
	colonIdx := strings.Index(userInfo, ":")
	if colonIdx < 0 {
		return raw
	}
	return raw[:schemeIdx+3] + userInfo[:colonIdx] + ":***" + rest[atIdx:]
}
