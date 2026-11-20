package middleware

import (
	"net/http"
	"strings"
)

// CORS applies origin allowlist checks and exposes Idempotent-Replayed and X-Correlation-ID headers
// so Swagger UI and browser clients can inspect replay and tracing metadata.
func CORS(allowedOrigins []string) func(http.Handler) http.Handler {
	allowAll := false
	exact := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		trimmed := strings.TrimSpace(o)
		if trimmed == "" {
			continue
		}
		if trimmed == "*" {
			allowAll = true
			continue
		}
		exact[trimmed] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Vary", "Origin")

			origin := strings.TrimSpace(r.Header.Get("Origin"))
			var allowOrigin string
			if origin != "" {
				if _, ok := exact[origin]; ok {
					allowOrigin = origin
				} else if allowAll {
					allowOrigin = "*"
				}
			}

			if allowOrigin != "" {
				w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
				w.Header().Set("Access-Control-Expose-Headers", "Idempotent-Replayed, X-Correlation-ID")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, X-Correlation-ID, Authorization")
				w.Header().Set("Access-Control-Max-Age", "600")
			}

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Add("Vary", "Access-Control-Request-Method")
				w.Header().Add("Vary", "Access-Control-Request-Headers")
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
