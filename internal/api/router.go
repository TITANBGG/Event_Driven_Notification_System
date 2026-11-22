package api

import (
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/TITANBGG/notification-system/internal/api/middleware"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

const (
	batchCreatePath       = "/api/v1/notifications/batch"
	defaultRequestTimeout = 10 * time.Second
)

// Deps supplies all external dependencies needed to wire the HTTP API router.
type Deps struct {
	Config         config.Config
	Logger         *slog.Logger
	Store          Store
	Enqueuer       Enqueuer
	QueueInspector QueueInspector
	Hub            *realtime.Hub
	Publisher      realtime.Publisher
	Metrics        *observability.Metrics
	Now            func() time.Time
}

type handler struct {
	cfg            config.Config
	logger         *slog.Logger
	store          Store
	enqueuer       Enqueuer
	queueInspector QueueInspector
	hub            *realtime.Hub
	publisher      realtime.Publisher
	metrics        *observability.Metrics
	nowFn          func() time.Time

	metricsMu    sync.Mutex
	metricsExp   time.Time
	metricsCache MetricsSnapshotResponse
}

// NewRouter constructs the HTTP API router with the exact middleware chain and routes
// specified in CLAUDE.md Section 7.
func NewRouter(deps Deps) chi.Router {
	logger := deps.Logger
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}
	enq := deps.Enqueuer
	if enq == nil {
		enq = NewMemoryEnqueuer()
	}
	nowFn := deps.Now
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	metrics := deps.Metrics
	if metrics == nil {
		metrics = observability.NewAPIMetrics()
	}
	pub := deps.Publisher
	if pub == nil {
		pub = realtime.NoopPublisher{}
	}

	h := &handler{
		cfg:            deps.Config,
		logger:         logger,
		store:          deps.Store,
		enqueuer:       enq,
		queueInspector: deps.QueueInspector,
		hub:            deps.Hub,
		publisher:      pub,
		metrics:        metrics,
		nowFn:          nowFn,
	}

	r := chi.NewRouter()
	// Order is significant: Recoverer wraps CorrelationID and Tracing so panics and access logs
	// still carry request correlation_id, trace_id, and span_id; BodyLimit runs before decode.
	r.Use(middleware.Recoverer(h.logger))
	r.Use(middleware.CorrelationID(h.logger))
	r.Use(middleware.Tracing("notification-api"))
	r.Use(middleware.RequestLogger(h.logger))
	r.Use(middleware.Metrics(h.metrics))
	r.Use(middleware.CORS(h.cfg.CORSAllowedOrigins))
	r.Use(middleware.BodyLimit(h.cfg.MaxBodyBytes, h.cfg.BatchMaxBodyBytes, batchCreatePath))
	r.Use(middleware.RequireJSON())
	r.Use(middleware.Timeout(defaultRequestTimeout))

	r.NotFound(notFoundHandler(h.logger))
	r.MethodNotAllowed(methodNotAllowedHandler(h.logger))

	r.Get("/health", h.handleHealthReady)
	r.Get("/health/live", h.handleHealthLive)
	r.Get("/health/ready", h.handleHealthReady)
	r.Get("/metrics", h.metrics.Handler().ServeHTTP)
	r.Get("/openapi.yaml", h.handleOpenAPISpec)
	r.Get("/demo", h.handleDemoPage)

	// Probes, the spec, the demo page and the Prometheus scrape stay public (the scrape is protected
	// at the network level); every /api/v1 route needs an API key once API_KEYS is configured.
	auth := middleware.NewAuthenticator(h.cfg.APIKeys, h.metrics)
	read := middleware.RequireScope(auth, config.ScopeRead)
	write := middleware.RequireScope(auth, config.ScopeWrite)
	admin := middleware.RequireScope(auth, config.ScopeAdmin)

	r.Route("/api/v1", func(r chi.Router) {
		r.Use(middleware.Authenticate(auth))

		r.With(write).Post("/notifications", h.handleCreateNotification)
		r.With(write).Post("/notifications/batch", h.handleCreateBatch)
		r.With(read).Get("/notifications", h.handleListNotifications)
		r.With(read).Get("/notifications/{id}", h.handleGetNotification)
		r.With(write).Post("/notifications/{id}/cancel", h.handleCancelNotification)
		// Manual DLQ replay re-sends a message that already failed, so it is an operator action.
		r.With(admin).Post("/notifications/{id}/retry", h.handleRetryNotification)

		r.With(read).Get("/batches/{id}", h.handleGetBatch)
		r.With(write).Post("/batches/{id}/cancel", h.handleCancelBatch)

		r.With(write).Post("/templates", h.handleCreateTemplate)
		r.With(read).Get("/templates", h.handleListTemplates)
		r.With(read).Get("/templates/{id}", h.handleGetTemplate)
		r.With(write).Put("/templates/{id}", h.handleUpdateTemplate)
		r.With(write).Delete("/templates/{id}", h.handleDeleteTemplate)
		// Preview renders without persisting anything, so read is enough.
		r.With(read).Post("/templates/{id}/preview", h.handlePreviewTemplate)

		r.With(read).Get("/ws", h.handleWebSocket)
		r.With(read).Get("/metrics", h.handleMetricsSnapshot)
	})

	return r
}
