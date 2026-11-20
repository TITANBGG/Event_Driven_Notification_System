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
	Metrics        *observability.Metrics
	Now            func() time.Time
}

type handler struct {
	cfg            config.Config
	logger         *slog.Logger
	store          Store
	enqueuer       Enqueuer
	queueInspector QueueInspector
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

	h := &handler{
		cfg:            deps.Config,
		logger:         logger,
		store:          deps.Store,
		enqueuer:       enq,
		queueInspector: deps.QueueInspector,
		metrics:        metrics,
		nowFn:          nowFn,
	}

	r := chi.NewRouter()
	// Order is significant: Recoverer wraps CorrelationID so panics still carry request correlation;
	// BodyLimit runs before Content-Type/handler decode so huge bodies fail fast with 413.
	r.Use(middleware.Recoverer(h.logger))
	r.Use(middleware.CorrelationID(h.logger))
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

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/notifications", h.handleCreateNotification)
		r.Post("/notifications/batch", h.handleCreateBatch)
		r.Get("/notifications", h.handleListNotifications)
		r.Get("/notifications/{id}", h.handleGetNotification)
		r.Post("/notifications/{id}/cancel", h.handleCancelNotification)
		r.Post("/notifications/{id}/retry", h.handleRetryNotification)

		r.Get("/batches/{id}", h.handleGetBatch)
		r.Post("/batches/{id}/cancel", h.handleCancelBatch)

		r.Post("/templates", h.handleCreateTemplate)
		r.Get("/templates", h.handleListTemplates)
		r.Get("/templates/{id}", h.handleGetTemplate)
		r.Put("/templates/{id}", h.handleUpdateTemplate)
		r.Delete("/templates/{id}", h.handleDeleteTemplate)
		r.Post("/templates/{id}/preview", h.handlePreviewTemplate)

		r.Get("/metrics", h.handleMetricsSnapshot)
	})

	return r
}
