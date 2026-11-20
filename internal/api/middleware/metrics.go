package middleware

import (
	"net/http"
	"time"
)

// HTTPMetricsObserver records HTTP request counts and durations per chi route pattern.
type HTTPMetricsObserver interface {
	ObserveHTTPRequest(route, method string, status int, duration time.Duration)
}

// Metrics instruments HTTP requests after handler completion using chi's low-cardinality route pattern.
func Metrics(obs HTTPMetricsObserver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if obs == nil {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			rec := &responseRecorder{
				ResponseWriter: w,
				status:         http.StatusOK,
			}
			next.ServeHTTP(rec, r)
			obs.ObserveHTTPRequest(routePattern(r), r.Method, rec.status, time.Since(start))
		})
	}
}
