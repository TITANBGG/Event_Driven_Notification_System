package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// RequireStaticBearer protects an operational endpoint (the Prometheus scrape) with one shared
// bearer token, compared in constant time. An empty token leaves the endpoint open, which suits a
// scrape that is already confined to a private network.
func RequireStaticBearer(token string) func(http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return func(next http.Handler) http.Handler {
		if token == "" {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, present := bearerToken(r)
			sum := sha256.Sum256([]byte(got))
			if !present || subtle.ConstantTimeCompare(sum[:], want[:]) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="metrics"`)
				writeMiddlewareError(r.Context(), w, http.StatusUnauthorized, "unauthorized", "metrics require the METRICS_BEARER_TOKEN bearer token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
