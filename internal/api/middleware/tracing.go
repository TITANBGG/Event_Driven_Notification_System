package middleware

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// Tracing wraps HTTP requests in an OpenTelemetry server span (using W3C TraceContext propagation)
// and renames the span to "<METHOD> <chi_route_pattern>" after routing completes.
func Tracing(serviceName string) func(http.Handler) http.Handler {
	if serviceName == "" {
		serviceName = "notification-api"
	}
	return func(next http.Handler) http.Handler {
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			if pat := routePattern(r); pat != "" && pat != "unmatched" {
				trace.SpanFromContext(r.Context()).SetName(r.Method + " " + pat)
			}
		})
		return otelhttp.NewHandler(inner, serviceName)
	}
}
