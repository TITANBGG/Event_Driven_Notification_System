package middleware

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// Timeout applies a per-request context deadline without replacing http.ResponseWriter,
// keeping structured error writers and connection hijacking intact.
// WebSocket upgrades (Upgrade: websocket) are exempted so long-lived streaming connections
// are governed by their own Ping/Pong keepalive deadlines rather than the 10s HTTP timeout.
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if d <= 0 || strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				next.ServeHTTP(w, r)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
