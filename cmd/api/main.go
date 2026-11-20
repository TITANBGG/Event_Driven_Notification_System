// Package main implements the cmd/api HTTP server entrypoint.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/repository"
)

type redisEnqueuer struct {
	q *queue.Queue
}

func (e redisEnqueuer) Enqueue(ctx context.Context, items []api.QueueItem) error {
	metas := make([]queue.Meta, len(items))
	for i, item := range items {
		metas[i] = queue.Meta{
			ID:            item.ID,
			Channel:       item.Channel,
			Priority:      item.Priority,
			CorrelationID: item.CorrelationID,
			TraceParent:   item.TraceParent,
		}
	}
	return e.q.Enqueue(ctx, metas...)
}

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
		slog.String("service", "notification-api"),
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
	poolCfg.MaxConns = 20
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
	q := queue.New(rdb)

	redisPingCtx, cancelRedisPing := context.WithTimeout(ctx, 2*time.Second)
	if rErr := q.Ping(redisPingCtx); rErr != nil {
		logger.Warn("redis not reachable at api startup; DB-first requests remain durable for reconciler",
			slog.String("error", rErr.Error()),
		)
	}
	cancelRedisPing()

	store := api.NewRepositoryStore(repository.New(pool))

	router := api.NewRouter(api.Deps{
		Config:   cfg,
		Logger:   logger,
		Store:    store,
		Enqueuer: redisEnqueuer{q: q},
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("api server listening", slog.String("addr", cfg.HTTPAddr))
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case serveErr := <-errCh:
		if serveErr != nil {
			logger.Error("api server exited with error", slog.String("error", serveErr.Error()))
		}
		return serveErr
	case <-ctx.Done():
		logger.Info("shutdown signal received", slog.Duration("timeout", cfg.ShutdownTimeout))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("graceful shutdown failed", slog.String("error", shutdownErr.Error()))
		return shutdownErr
	}
	if serveErr := <-errCh; serveErr != nil {
		return serveErr
	}

	logger.Info("api server stopped cleanly")
	return nil
}
