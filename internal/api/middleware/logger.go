package middleware

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/TITANBGG/notification-system/internal/observability"
)

type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// RequestLogger emits one structured access log line per HTTP request after the handler returns,
// logging the matched chi route pattern instead of raw URL paths.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	baseLogger := logger
	if baseLogger == nil {
		baseLogger = observability.NewLogger("info", os.Stdout)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &responseRecorder{
				ResponseWriter: w,
				status:         http.StatusOK,
			}

			next.ServeHTTP(rec, r)

			durationMS := time.Since(start).Milliseconds()
			route := routePattern(r)
			reqLogger := observability.LoggerFrom(r.Context(), baseLogger)

			attrs := []any{
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.Int("status", rec.status),
				slog.Int64("duration_ms", durationMS),
				slog.Int64("bytes", rec.bytes),
			}
			if rec.status >= http.StatusInternalServerError {
				reqLogger.Error("http request completed", attrs...)
				return
			}
			reqLogger.Info("http request completed", attrs...)
		})
	}
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pat := rctx.RoutePattern(); pat != "" {
			return pat
		}
	}
	return "unmatched"
}
