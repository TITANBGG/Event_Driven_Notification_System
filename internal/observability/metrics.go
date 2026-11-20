package observability

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsOptions selects which role-specific Prometheus collectors are registered on a Registry.
type MetricsOptions struct {
	IncludeHTTP        bool
	IncludeQueueDepths bool
	IncludeWorker      bool
}

// Metrics holds all application Prometheus instruments isolated on a dedicated registry.
type Metrics struct {
	registry *prometheus.Registry

	notificationsCreated   *prometheus.CounterVec
	notificationsProcessed *prometheus.CounterVec

	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec

	queueDepth    *prometheus.GaugeVec
	delayedDepth  prometheus.Gauge
	dlqDepth      prometheus.Gauge
	inflightDepth prometheus.Gauge

	providerRequests    *prometheus.CounterVec
	deliveryDuration    *prometheus.HistogramVec
	endToEndLatency     *prometheus.HistogramVec
	rateLimitedTotal    *prometheus.CounterVec
	rateLimitWait       *prometheus.HistogramVec
	circuitBreakerState *prometheus.GaugeVec
	retriesScheduled    *prometheus.CounterVec
	leaseLostTotal      prometheus.Counter
	reconcilerActions   *prometheus.CounterVec
	workerPanicsTotal   prometheus.Counter
}

// NewAPIMetrics creates a custom Prometheus registry exposing API HTTP, creation, cancel, and
// sampled queue-depth metrics without worker-only series.
func NewAPIMetrics() *Metrics {
	return NewMetrics(MetricsOptions{
		IncludeHTTP:        true,
		IncludeQueueDepths: true,
	})
}

// NewWorkerMetrics creates a custom Prometheus registry exposing worker delivery, circuit-breaker,
// rate-limiter, latency, and reconciler metrics without API queue-depth gauges.
func NewWorkerMetrics() *Metrics {
	return NewMetrics(MetricsOptions{
		IncludeWorker: true,
	})
}

// NewAllMetrics creates a custom Prometheus registry containing every API and worker metric for tests.
func NewAllMetrics() *Metrics {
	return NewMetrics(MetricsOptions{
		IncludeHTTP:        true,
		IncludeQueueDepths: true,
		IncludeWorker:      true,
	})
}

// NewMetrics registers configured metrics plus Go runtime and process collectors on a private registry.
func NewMetrics(opts MetricsOptions) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{registry: reg}

	m.notificationsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_processed_total",
		Help: "Total notifications transitioned to processed outcomes by channel and status.",
	}, []string{"channel", "status"})
	reg.MustRegister(m.notificationsProcessed)

	if opts.IncludeHTTP {
		m.notificationsCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Total accepted notifications created by channel and priority.",
		}, []string{"channel", "priority"})
		m.httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests handled grouped by matched route pattern, method, and status code.",
		}, []string{"route", "method", "code"})
		m.httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency distribution in seconds by matched route pattern and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"})
		reg.MustRegister(m.notificationsCreated, m.httpRequestsTotal, m.httpRequestDuration)
	}

	if opts.IncludeQueueDepths {
		m.queueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Current ready queue length per channel and priority tier sampled by the API.",
		}, []string{"channel", "priority"})
		m.delayedDepth = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "delayed_depth",
			Help: "Current number of entries scheduled in the Redis delayed ZSET.",
		})
		m.dlqDepth = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dlq_depth",
			Help: "Current number of items in the bounded Redis dead-letter list.",
		})
		m.inflightDepth = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "inflight_depth",
			Help: "Total items currently parked in worker inflight lists.",
		})
		reg.MustRegister(m.queueDepth, m.delayedDepth, m.dlqDepth, m.inflightDepth)
	}

	if opts.IncludeWorker {
		m.providerRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "provider_requests_total",
			Help: "Total external provider HTTP requests grouped by channel and status/error code.",
		}, []string{"channel", "code"})
		m.deliveryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "delivery_duration_seconds",
			Help:    "Provider HTTP call duration in seconds by channel.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"channel"})
		m.endToEndLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "end_to_end_latency_seconds",
			Help:    "End-to-end latency from notification creation to sent completion in seconds.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"channel"})
		m.rateLimitedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rate_limited_total",
			Help: "Total worker requests delayed by the distributed GCRA rate limiter per channel.",
		}, []string{"channel"})
		m.rateLimitWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rate_limit_wait_seconds",
			Help:    "Wait durations spent acquiring a rate-limit token per channel.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"channel"})
		m.circuitBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Per-channel circuit breaker state (0=closed, 1=half_open, 2=open).",
		}, []string{"channel"})
		m.retriesScheduled = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retries_scheduled_total",
			Help: "Total retry attempts scheduled after transient failures by channel.",
		}, []string{"channel"})
		m.leaseLostTotal = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lease_lost_total",
			Help: "Total worker fenced DB updates rejected because row ownership changed.",
		})
		m.reconcilerActions = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconciler_actions_total",
			Help: "Total repair and cleanup actions applied by the distributed reconciler.",
		}, []string{"action"})
		m.workerPanicsTotal = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "worker_panics_total",
			Help: "Total recovered panics across worker goroutines.",
		})

		reg.MustRegister(
			m.providerRequests,
			m.deliveryDuration,
			m.endToEndLatency,
			m.rateLimitedTotal,
			m.rateLimitWait,
			m.circuitBreakerState,
			m.retriesScheduled,
			m.leaseLostTotal,
			m.reconcilerActions,
			m.workerPanicsTotal,
		)

		// Pre-initialize closed state (0) so Prometheus queries return a continuous time series at startup.
		for _, ch := range []string{"sms", "email", "push"} {
			m.circuitBreakerState.WithLabelValues(ch).Set(0)
		}
	}

	return m
}

// Registry returns the private Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// Handler returns the Prometheus HTTP scrape handler backed by the private registry.
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// IncNotificationCreated increments notifications_created_total for channel and priority.
func (m *Metrics) IncNotificationCreated(channel, priority string) {
	if m == nil || m.notificationsCreated == nil {
		return
	}
	m.notificationsCreated.WithLabelValues(channel, priority).Inc()
}

// IncNotificationProcessed increments notifications_processed_total for channel and status.
func (m *Metrics) IncNotificationProcessed(channel, status string) {
	if m == nil || m.notificationsProcessed == nil {
		return
	}
	m.notificationsProcessed.WithLabelValues(channel, status).Inc()
}

// ObserveHTTPRequest records HTTP request count and duration using chi's low-cardinality route pattern.
func (m *Metrics) ObserveHTTPRequest(route, method string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	if route == "" {
		route = "unmatched"
	}
	if m.httpRequestsTotal != nil {
		m.httpRequestsTotal.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	}
	if m.httpRequestDuration != nil {
		m.httpRequestDuration.WithLabelValues(route, method).Observe(duration.Seconds())
	}
}

// SetQueueDepth sets queue_depth for channel and priority.
func (m *Metrics) SetQueueDepth(channel, priority string, depth int64) {
	if m == nil || m.queueDepth == nil {
		return
	}
	m.queueDepth.WithLabelValues(channel, priority).Set(float64(depth))
}

// SetDelayedDepth sets delayed_depth.
func (m *Metrics) SetDelayedDepth(depth int64) {
	if m == nil || m.delayedDepth == nil {
		return
	}
	m.delayedDepth.Set(float64(depth))
}

// SetDLQDepth sets dlq_depth.
func (m *Metrics) SetDLQDepth(depth int64) {
	if m == nil || m.dlqDepth == nil {
		return
	}
	m.dlqDepth.Set(float64(depth))
}

// SetInflightDepth sets inflight_depth.
func (m *Metrics) SetInflightDepth(depth int64) {
	if m == nil || m.inflightDepth == nil {
		return
	}
	m.inflightDepth.Set(float64(depth))
}

// ObserveProviderRequest records external provider call outcome and latency for channel.
func (m *Metrics) ObserveProviderRequest(channel string, httpStatus *int, err error, duration time.Duration) {
	if m == nil {
		return
	}
	if m.providerRequests != nil {
		m.providerRequests.WithLabelValues(channel, ProviderCodeLabel(httpStatus, err)).Inc()
	}
	if m.deliveryDuration != nil {
		m.deliveryDuration.WithLabelValues(channel).Observe(duration.Seconds())
	}
}

// ObserveEndToEndLatency observes notification creation-to-sent duration in seconds.
func (m *Metrics) ObserveEndToEndLatency(channel string, latency time.Duration) {
	if m == nil || m.endToEndLatency == nil {
		return
	}
	if latency < 0 {
		latency = 0
	}
	m.endToEndLatency.WithLabelValues(channel).Observe(latency.Seconds())
}

// ObserveRateLimit increments rate_limited_total and records wait duration in seconds.
func (m *Metrics) ObserveRateLimit(channel string, wait time.Duration) {
	if m == nil {
		return
	}
	if m.rateLimitedTotal != nil {
		m.rateLimitedTotal.WithLabelValues(channel).Inc()
	}
	if m.rateLimitWait != nil {
		if wait < 0 {
			wait = 0
		}
		m.rateLimitWait.WithLabelValues(channel).Observe(wait.Seconds())
	}
}

// SetCircuitBreakerState updates circuit_breaker_state using 0=closed, 1=half_open, 2=open.
func (m *Metrics) SetCircuitBreakerState(channel, state string) {
	if m == nil || m.circuitBreakerState == nil {
		return
	}
	var val float64
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "half_open", "half-open":
		val = 1
	case "open":
		val = 2
	default:
		val = 0
	}
	m.circuitBreakerState.WithLabelValues(channel).Set(val)
}

// IncRetryScheduled increments retries_scheduled_total for channel.
func (m *Metrics) IncRetryScheduled(channel string) {
	if m == nil || m.retriesScheduled == nil {
		return
	}
	m.retriesScheduled.WithLabelValues(channel).Inc()
}

// IncLeaseLost increments lease_lost_total.
func (m *Metrics) IncLeaseLost() {
	if m == nil || m.leaseLostTotal == nil {
		return
	}
	m.leaseLostTotal.Inc()
}

// AddReconcilerAction increments reconciler_actions_total{action} by count.
func (m *Metrics) AddReconcilerAction(action string, count int) {
	if m == nil || m.reconcilerActions == nil || count <= 0 {
		return
	}
	m.reconcilerActions.WithLabelValues(action).Add(float64(count))
}

// IncWorkerPanic increments worker_panics_total.
func (m *Metrics) IncWorkerPanic() {
	if m == nil || m.workerPanicsTotal == nil {
		return
	}
	m.workerPanicsTotal.Inc()
}

// ProviderCodeLabel converts an optional provider HTTP status or error into a bounded Prometheus label.
func ProviderCodeLabel(httpStatus *int, _ error) string {
	if httpStatus != nil {
		return strconv.Itoa(*httpStatus)
	}
	return "transport_error"
}
