package middleware

import (
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/observability"
)

const correlationIDHeader = "X-Correlation-ID"

var correlationIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// CorrelationID validates X-Correlation-ID or generates a UUIDv7, binds it to the request
// context and logger, and echoes it on the response header.
func CorrelationID(logger *slog.Logger) func(http.Handler) http.Handler {
	baseLogger := logger
	if baseLogger == nil {
		baseLogger = observability.NewLogger("info", os.Stdout)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cid := strings.TrimSpace(r.Header.Get(correlationIDHeader))
			if !correlationIDPattern.MatchString(cid) {
				id, err := uuid.NewV7()
				if err != nil {
					id = uuid.New()
				}
				cid = id.String()
			}

			w.Header().Set(correlationIDHeader, cid)

			ctx := observability.WithCorrelationID(r.Context(), cid)
			ctx = observability.WithLogger(ctx, baseLogger)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
