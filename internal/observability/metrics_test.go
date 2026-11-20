package observability_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/observability"
)

func TestMetricsCustomRegistryAndExposition(t *testing.T) {
	t.Parallel()

	// Multiple custom registries can be created safely without touching prometheus.DefaultRegisterer.
	m1 := observability.NewAllMetrics()
	m2 := observability.NewAllMetrics()
	require.NotSame(t, m1.Registry(), prometheus.DefaultRegisterer)
	require.NotSame(t, m1.Registry(), m2.Registry())

	code202 := 202
	m1.IncNotificationCreated("sms", "high")
	m1.IncNotificationProcessed("sms", "sent")
	m1.ObserveHTTPRequest("/api/v1/notifications/{id}", "GET", http.StatusOK, 12*time.Millisecond)
	m1.SetQueueDepth("sms", "high", 7)
	m1.SetDelayedDepth(3)
	m1.SetDLQDepth(1)
	m1.SetInflightDepth(4)
	m1.ObserveProviderRequest("sms", &code202, nil, 45*time.Millisecond)
	m1.ObserveEndToEndLatency("sms", 250*time.Millisecond)
	m1.ObserveRateLimit("sms", 15*time.Millisecond)
	m1.SetCircuitBreakerState("sms", "open")
	m1.IncRetryScheduled("sms")
	m1.IncLeaseLost()
	m1.AddReconcilerAction("stale_pending", 2)
	m1.IncWorkerPanic()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m1.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	expectedMetrics := []string{
		"go_goroutines",
		"process_",
		"notifications_created_total",
		"notifications_processed_total",
		"provider_requests_total",
		"delivery_duration_seconds",
		"end_to_end_latency_seconds",
		"queue_depth",
		"delayed_depth",
		"dlq_depth",
		"inflight_depth",
		"rate_limited_total",
		"rate_limit_wait_seconds",
		"circuit_breaker_state",
		"retries_scheduled_total",
		"lease_lost_total",
		"reconciler_actions_total",
		"worker_panics_total",
		"http_requests_total",
		"http_request_duration_seconds",
	}
	for _, metricName := range expectedMetrics {
		assert.Contains(t, body, metricName, "missing metric %s", metricName)
	}
	assert.Contains(t, body, `circuit_breaker_state{channel="sms"} 2`)
}

func TestAPIVsWorkerMetricsSeparation(t *testing.T) {
	t.Parallel()

	apiM := observability.NewAPIMetrics()
	apiM.SetQueueDepth("email", "normal", 5)
	apiM.IncNotificationProcessed("email", "cancelled")
	recAPI := httptest.NewRecorder()
	apiM.Handler().ServeHTTP(recAPI, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	apiBody := recAPI.Body.String()
	assert.Contains(t, apiBody, "queue_depth")
	assert.Contains(t, apiBody, `notifications_processed_total{channel="email",status="cancelled"} 1`, "API counts cancellations")
	assert.NotContains(t, apiBody, "circuit_breaker_state")

	workerM := observability.NewWorkerMetrics()
	workerM.IncRetryScheduled("email")
	recWorker := httptest.NewRecorder()
	workerM.Handler().ServeHTTP(recWorker, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	workerBody := recWorker.Body.String()
	assert.Contains(t, workerBody, "circuit_breaker_state")
	assert.NotContains(t, workerBody, "queue_depth")
}
