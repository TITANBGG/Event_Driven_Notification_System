// Package main implements the cmd/worker background processing entrypoint.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/ratelimit"
	"github.com/TITANBGG/notification-system/internal/realtime"
	"github.com/TITANBGG/notification-system/internal/repository"
	"github.com/TITANBGG/notification-system/internal/worker"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		observability.NewLogger("error", os.Stderr).Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}

	logger := observability.NewLogger(cfg.LogLevel, os.Stdout).With(
		slog.String("service", "notification-worker"),
		slog.String("worker_id", cfg.WorkerID),
	)
	logger.Info("config loaded", slog.Any("config", cfg))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to parse database URL",
			slog.String("database_url", observability.MaskDatabaseURL(cfg.DatabaseURL)),
		)
		return errors.New("invalid DATABASE_URL configuration")
	}
	poolCfg.MaxConns = 35
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		logger.Error("failed to create postgres pool", slog.String("error", err.Error()))
		return err
	}
	defer pool.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	pingErr := pool.Ping(pingCtx)
	cancelPing()
	if pingErr != nil {
		logger.Error("failed to ping postgres at startup", slog.String("error", pingErr.Error()))
		return pingErr
	}

	rdb := queue.NewClient(cfg.RedisAddr, cfg.RedisPoolSize)
	defer func() {
		if closeErr := rdb.Close(); closeErr != nil {
			logger.Warn("failed closing redis client", slog.String("error", closeErr.Error()))
		}
	}()

	store := repository.New(pool)
	q := queue.New(rdb)
	metrics := observability.NewWorkerMetrics()
	limiter := ratelimit.New(
		rdb,
		cfg.RateLimitPerSec,
		cfg.RateLimitBurst,
		ratelimit.WithOnRateLimited(func(ch domain.Channel, wait time.Duration) {
			metrics.ObserveRateLimit(ch.String(), wait)
		}),
	)
	client := provider.NewClient(provider.ClientConfig{
		URL:            cfg.ProviderURL,
		Timeout:        cfg.ProviderTimeout,
		ExtendedFields: cfg.ProviderExtended,
		MaxIdleConns:   max(3*cfg.WorkerConcurrency, 100),
		Logger:         logger,
	})

	hooks := worker.Hooks{
		OnPanic:     metrics.IncWorkerPanic,
		OnLeaseLost: metrics.IncLeaseLost,
		OnProcessed: func(ch domain.Channel, status domain.Status) {
			metrics.IncNotificationProcessed(ch.String(), status.String())
		},
		OnProviderRequest: func(ch domain.Channel, httpStatus *int, sendErr error, duration time.Duration) {
			metrics.ObserveProviderRequest(ch.String(), httpStatus, sendErr, duration)
		},
		OnEndToEndLatency: func(ch domain.Channel, latency time.Duration) {
			metrics.ObserveEndToEndLatency(ch.String(), latency)
		},
		OnRetryScheduled: func(ch domain.Channel) {
			metrics.IncRetryScheduled(ch.String())
		},
		OnBreakerStateChange: func(ch domain.Channel, _, to string) {
			metrics.SetCircuitBreakerState(ch.String(), to)
		},
		OnReconcilerAction: func(step string, count int) {
			metrics.AddReconcilerAction(step, count)
		},
		OnInflightSwept: func(count int) {
			metrics.AddReconcilerAction("inflight_swept", count)
		},
	}
	retryPolicy := worker.NewExponentialRetryPolicy(cfg.RetryBase, cfg.RetryCap, cfg.RetryMin, cfg.RetryAfterMax)
	breaker := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
		Config: cfg,
		Logger: logger,
		OnStateChange: func(ch domain.Channel, from, to string) {
			if hooks.OnBreakerStateChange != nil {
				hooks.OnBreakerStateChange(ch, from, to)
			}
		},
	})
	reconciler, err := worker.NewReconciler(worker.ReconcilerDeps{
		Config: cfg,
		Logger: logger,
		Store:  store,
		Queue:  q,
		OnAction: func(step string, count int) {
			if hooks.OnReconcilerAction != nil {
				hooks.OnReconcilerAction(step, count)
			}
		},
	})
	if err != nil {
		logger.Error("failed to initialize worker reconciler", slog.String("error", err.Error()))
		return err
	}

	w, err := worker.New(worker.Deps{
		Config:      cfg,
		Logger:      logger,
		Store:       store,
		Queue:       q,
		RateLimiter: limiter,
		Sender:      client,
		RetryPolicy: retryPolicy,
		Breaker:     breaker,
		Reconciler:  reconciler,
		Publisher:   realtime.NewRedisPublisher(rdb),
		Hooks:       hooks,
	})
	if err != nil {
		logger.Error("failed to initialize worker engine", slog.String("error", err.Error()))
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeProbeJSON(w, logger, http.StatusOK, map[string]string{"status": "ok"})
	})
	readyHandler := func(w http.ResponseWriter, r *http.Request) {
		checkReady(r.Context(), w, logger, store, q)
	}
	mux.HandleFunc("GET /health/ready", readyHandler)
	mux.HandleFunc("GET /health", readyHandler)
	mux.Handle("GET /metrics", metrics.Handler())

	srv := &http.Server{
		Addr:              cfg.WorkerMetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	httpErrCh := make(chan error, 1)
	go func() {
		logger.Info("worker metrics/health server listening", slog.String("addr", cfg.WorkerMetricsAddr))
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			httpErrCh <- serveErr
			return
		}
		httpErrCh <- nil
	}()

	workerErrCh := make(chan error, 1)
	go func() {
		workerErrCh <- w.Run(ctx)
	}()

	select {
	case serveErr := <-httpErrCh:
		stop()
		if wErr := <-workerErrCh; wErr != nil {
			logger.Error("worker exited with error during http failure drain", slog.String("error", wErr.Error()))
		}
		return serveErr
	case wErr := <-workerErrCh:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		if serveErr := <-httpErrCh; serveErr != nil {
			logger.Error("worker http server error during worker drain", slog.String("error", serveErr.Error()))
		}
		return wErr
	case <-ctx.Done():
		logger.Info("worker shutdown signal received", slog.Duration("timeout", cfg.ShutdownTimeout))
	}

	wErr := <-workerErrCh
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("worker http server shutdown failed", slog.String("error", shutdownErr.Error()))
		return shutdownErr
	}
	if serveErr := <-httpErrCh; serveErr != nil {
		return serveErr
	}
	if wErr != nil {
		return wErr
	}

	logger.Info("worker stopped cleanly")
	return nil
}

func checkReady(ctx context.Context, w http.ResponseWriter, logger *slog.Logger, store *repository.Store, q *queue.Queue) {
	type depStatus struct {
		Status    string `json:"status"`
		LatencyMS int64  `json:"latencyMs"`
	}

	overall := "ok"
	code := http.StatusOK

	pgStart := time.Now()
	pgCtx, cancelPG := context.WithTimeout(ctx, time.Second)
	pgErr := store.Ping(pgCtx)
	cancelPG()
	pgStatus := depStatus{Status: "ok", LatencyMS: time.Since(pgStart).Milliseconds()}
	if pgErr != nil {
		pgStatus.Status = "down"
		overall = "degraded"
		code = http.StatusServiceUnavailable
	}

	rdStart := time.Now()
	rdCtx, cancelRD := context.WithTimeout(ctx, time.Second)
	rdErr := q.Ping(rdCtx)
	cancelRD()
	rdStatus := depStatus{Status: "ok", LatencyMS: time.Since(rdStart).Milliseconds()}
	if rdErr != nil {
		rdStatus.Status = "down"
		overall = "degraded"
		code = http.StatusServiceUnavailable
	}

	writeProbeJSON(w, logger, code, map[string]any{
		"status": overall,
		"checks": map[string]depStatus{
			"postgres": pgStatus,
			"redis":    rdStatus,
		},
	})
}

func writeProbeJSON(w http.ResponseWriter, logger *slog.Logger, status int, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(raw); err != nil {
		logger.Warn("failed writing probe response", slog.String("error", err.Error()))
	}
}
