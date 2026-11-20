package middleware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"

	"github.com/TITANBGG/notification-system/internal/observability"
)

type headerTrackingWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *headerTrackingWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *headerTrackingWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (w *headerTrackingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Recoverer catches panics in downstream handlers, logs the stack trace with correlation_id,
// and writes a structured 500 JSON error if headers have not yet been sent.
func Recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	baseLogger := logger
	if baseLogger == nil {
		baseLogger = observability.NewLogger("info", os.Stdout)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tw := &headerTrackingWriter{ResponseWriter: w}
			defer func(ctx context.Context) {
				if rec := recover(); rec != nil {
					if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						// Preserve net/http sentinel abort behavior without treating it as an unhandled crash.
						panic(rec)
					}

					cid := correlationIDFromContext(ctx, w)
					attrs := []any{
						slog.String("panic", fmt.Sprint(rec)),
						slog.String("stack", string(debug.Stack())),
					}
					if cid != "" {
						attrs = append(attrs, slog.String("correlation_id", cid))
					}
					observability.LoggerFrom(ctx, baseLogger).Error("panic recovered in http handler", attrs...)

					if !tw.wroteHeader {
						writeMiddlewareError(ctx, tw, http.StatusInternalServerError, "internal_error", "internal server error")
					}
				}
			}(r.Context())
			next.ServeHTTP(tw, r)
		})
	}
}
