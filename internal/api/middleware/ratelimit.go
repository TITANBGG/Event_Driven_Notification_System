package middleware

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/TITANBGG/notification-system/internal/observability"
)

// KeyedLimiter grants one request for key, returning how long to wait when it is denied.
type KeyedLimiter interface {
	AllowKey(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}

// anonymousPrincipal is the limiter key when authentication is disabled (all callers share it).
const anonymousPrincipal = "anonymous"

// RateLimitByKey limits each API key (the authenticated principal) to the limiter's rate and answers
// 429 rate_limited with Retry-After beyond it. A nil limiter disables it. A limiter error fails OPEN:
// unlike the provider-side limiter, this one only protects the API, and the API stays usable
// (DB-first) while Redis is down. It must run after Authenticate.
func RateLimitByKey(limiter KeyedLimiter, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if limiter == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := anonymousPrincipal
			if p, ok := PrincipalFrom(r.Context()); ok {
				key = p.Name
			}
			allowed, retryAfter, err := limiter.AllowKey(r.Context(), key)
			if err != nil {
				observability.LoggerFrom(r.Context(), logger).Warn("API rate limiter unavailable; allowing request",
					slog.String("error", err.Error()))
				next.ServeHTTP(w, r)
				return
			}
			if !allowed {
				w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(retryAfter.Seconds())))))
				writeMiddlewareError(r.Context(), w, http.StatusTooManyRequests, "rate_limited",
					"API key exceeded API_RATE_LIMIT_PER_SEC; retry after the Retry-After delay")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
