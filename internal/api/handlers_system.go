package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	apispec "github.com/TITANBGG/notification-system/api"
)

const readyCheckTimeout = time.Second

func (h *handler) handleHealthLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.logger, http.StatusOK, LiveStatusResponse{Status: "ok"})
}

func (h *handler) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	reqCtx := r.Context()
	var (
		pgCheck DependencyCheck
		rdCheck DependencyCheck
		wg      sync.WaitGroup
	)
	wg.Add(2)

	go func(ctx context.Context) {
		defer wg.Done()
		start := time.Now()
		if h.store == nil {
			pgCheck = DependencyCheck{Status: "down", LatencyMS: 0}
			return
		}
		pingCtx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
		defer cancel()
		err := h.store.Ping(pingCtx)
		status := "ok"
		if err != nil {
			status = "down"
		}
		pgCheck = DependencyCheck{Status: status, LatencyMS: time.Since(start).Milliseconds()}
	}(reqCtx)

	go func(ctx context.Context) {
		defer wg.Done()
		start := time.Now()
		if h.queueInspector == nil {
			rdCheck = DependencyCheck{Status: "down", LatencyMS: 0}
			return
		}
		pingCtx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
		defer cancel()
		err := h.queueInspector.Ping(pingCtx)
		status := "ok"
		if err != nil {
			status = "down"
		}
		rdCheck = DependencyCheck{Status: status, LatencyMS: time.Since(start).Milliseconds()}
	}(reqCtx)

	wg.Wait()

	// The API is DB-first: with PostgreSQL up it accepts and stores every notification even while
	// Redis is down (the reconciler enqueues them later), so only PostgreSQL decides readiness. Taking
	// every replica out of a load balancer because Redis blipped would turn a delay into an outage.
	// Redis trouble is still visible as status "degraded" with checks.redis = "down".
	code := http.StatusOK
	overall := "ok"
	switch {
	case pgCheck.Status != "ok":
		code = http.StatusServiceUnavailable
		overall = "unavailable"
	case rdCheck.Status != "ok":
		overall = "degraded"
	}

	writeJSON(w, h.logger, code, ReadyStatusResponse{
		Status: overall,
		Checks: ReadyChecksResponse{
			Postgres: pgCheck,
			Redis:    rdCheck,
		},
	})
}

func (h *handler) handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(apispec.OpenAPISpec); err != nil {
		h.logger.Warn("failed to write OpenAPI spec response", "error", err.Error())
	}
}
