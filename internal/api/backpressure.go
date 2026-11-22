package api

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/TITANBGG/notification-system/internal/observability"
)

// overloadRetryAfter is what an overloaded API tells clients to wait: about one queue-depth sample.
const overloadRetryAfter = 5 * time.Second

// IngressGate turns away new notifications while the ready queues hold more than maxDepth items.
// The send side is capped at 100/s per channel, so without it a long burst only grows Redis memory
// and end-to-end latency. The depth comes from the API's existing 5s queue-depth sampler, so the
// check costs no Redis call on the request path.
type IngressGate struct {
	maxDepth int64
	ready    atomic.Int64
}

// NewIngressGate returns a gate that rejects creates at or above maxDepth ready items (0 disables it).
func NewIngressGate(maxDepth int64) *IngressGate {
	return &IngressGate{maxDepth: maxDepth}
}

// Observe records the latest total of ready queue lengths.
func (g *IngressGate) Observe(readyDepth int64) {
	if g != nil {
		g.ready.Store(readyDepth)
	}
}

// Overloaded reports whether new notifications should be refused.
func (g *IngressGate) Overloaded() bool {
	return g != nil && g.maxDepth > 0 && g.ready.Load() >= g.maxDepth
}

// requireCapacity rejects creates with 503 overloaded + Retry-After while the gate is closed.
// Cancels, reads and retries of existing rows are never gated.
func (h *handler) requireCapacity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.gate.Overloaded() {
			observability.LoggerFrom(r.Context(), h.logger).Warn("rejecting create: queue backlog above INGRESS_MAX_QUEUE_DEPTH")
			w.Header().Set("Retry-After", strconv.Itoa(int(overloadRetryAfter.Seconds())))
			writeError(w, r, h.logger, &apiError{
				status:  http.StatusServiceUnavailable,
				code:    "overloaded",
				message: "queue backlog is above the configured limit; retry later",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}
