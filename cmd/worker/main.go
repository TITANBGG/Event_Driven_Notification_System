// Package main implements the cmd/worker background processing entrypoint.
//
// WHY: Each worker replica runs isolated from cmd/api, exposes its own liveness/metrics HTTP
// port (:9091), and drains gracefully on SIGTERM so inflight work can be returned cleanly to Redis.
//
// Expected output example (startup log):
//
//	{"level":"INFO","msg":"config loaded","service":"notification-worker","worker_id":"worker-1",...}
//	{"level":"INFO","msg":"worker metrics/health server listening","service":"notification-worker","worker_id":"worker-1","addr":":9091"}
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

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, writeErr := w.Write([]byte(`{"status":"ok"}` + "\n")); writeErr != nil {
			logger.Warn("failed to write worker health response", slog.String("error", writeErr.Error()))
		}
	})

	srv := &http.Server{
		Addr:              cfg.WorkerMetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("worker metrics/health server listening", slog.String("addr", cfg.WorkerMetricsAddr))
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case serveErr := <-errCh:
		if serveErr != nil {
			logger.Error("worker server exited with error", slog.String("error", serveErr.Error()))
		}
		return serveErr
	case <-ctx.Done():
		logger.Info("worker shutdown signal received", slog.Duration("timeout", cfg.ShutdownTimeout))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("worker graceful shutdown failed", slog.String("error", shutdownErr.Error()))
		return shutdownErr
	}
	if serveErr := <-errCh; serveErr != nil {
		return serveErr
	}

	logger.Info("worker stopped cleanly")
	return nil
}
