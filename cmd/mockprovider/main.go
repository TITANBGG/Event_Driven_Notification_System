// Package main implements cmd/mockprovider, a deterministic fake external notification provider
// matching the EXT-1 webhook.site HTTP contract (202 Accepted) plus fault/latency injection.
//
// WHY: Free webhook.site endpoints impose request and rate caps; mockprovider lets us run
// 100 msg/s/channel rate-limit proofs, failure/circuit-breaker scenarios, and duplicate detection
// locally with zero external quota risk.
//
// Expected output example (POST /):
//
//	HTTP/1.1 202 Accepted
//	Content-Type: application/json
//	{"messageId":"3f1c2b9e-8a7d-4c1e-9b2a-1d2e3f4a5b6c","status":"accepted","timestamp":"2026-11-20T19:50:00Z"}
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/observability"
)

type mockConfig struct {
	Port            string
	LatencyMS       int
	LatencyJitterMS int
	FailureRate     float64
	Rate429         float64
	PermanentRate   float64
	Seed            int64
}

type acceptedResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

type statsResponse struct {
	Received   int            `json:"received"`
	ByKey      map[string]int `json:"byKey"`
	Timestamps []time.Time    `json:"timestamps"`
}

type mockServer struct {
	cfg    mockConfig
	logger *slog.Logger
	nowFn  func() time.Time

	mu         sync.Mutex
	rng        *rand.Rand
	received   int
	byKey      map[string]int
	timestamps []time.Time
}

func newMockServer(cfg mockConfig, logger *slog.Logger, nowFn func() time.Time) *mockServer {
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	return &mockServer{
		cfg:    cfg,
		logger: logger,
		nowFn:  nowFn,
		rng:    rand.New(rand.NewSource(seed)),
		byKey:  make(map[string]int),
	}
}

func (s *mockServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.handleHealth)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("POST /reset", s.handleReset)
	mux.HandleFunc("POST /", s.handleSend)
	return mux
}

func (s *mockServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.logger, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *mockServer) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	byKeyCopy := make(map[string]int, len(s.byKey))
	for k, v := range s.byKey {
		byKeyCopy[k] = v
	}
	tsCopy := make([]time.Time, len(s.timestamps))
	copy(tsCopy, s.timestamps)
	received := s.received
	s.mu.Unlock()

	writeJSON(w, s.logger, http.StatusOK, statsResponse{
		Received:   received,
		ByKey:      byKeyCopy,
		Timestamps: tsCopy,
	})
}

func (s *mockServer) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.received = 0
	s.byKey = make(map[string]int)
	s.timestamps = nil
	s.mu.Unlock()

	writeJSON(w, s.logger, http.StatusOK, map[string]string{"status": "reset"})
}

func (s *mockServer) handleSend(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, s.logger, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	corrID := r.Header.Get("X-Correlation-ID")
	traceparent := r.Header.Get("traceparent")

	delay, status := s.recordAndDecide(idemKey)

	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}

	s.logger.Info("mockprovider received request",
		slog.Int("status", status),
		slog.String("idempotency_key", idemKey),
		slog.String("correlation_id", corrID),
		slog.String("traceparent", traceparent),
		slog.String("body", string(bodyBytes)),
	)

	switch status {
	case http.StatusBadRequest:
		writeJSON(w, s.logger, http.StatusBadRequest, map[string]string{"error": "simulated permanent 400"})
	case http.StatusTooManyRequests:
		w.Header().Set("Retry-After", "1")
		writeJSON(w, s.logger, http.StatusTooManyRequests, map[string]string{"error": "simulated rate limit 429"})
	case http.StatusInternalServerError:
		writeJSON(w, s.logger, http.StatusInternalServerError, map[string]string{"error": "simulated transient 500"})
	default:
		writeJSON(w, s.logger, http.StatusAccepted, acceptedResponse{
			MessageID: uuid.NewString(),
			Status:    "accepted",
			Timestamp: s.nowFn().Format(time.RFC3339),
		})
	}
}

// recordAndDecide atomically records request statistics and picks latency + fault outcome.
// WHY: Grouping RNG sampling and stat updates under one mutex makes concurrent HTTP handling
// race-free under `go test -race` without spawning per-request RNG instances.
func (s *mockServer) recordAndDecide(idemKey string) (time.Duration, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.received++
	if idemKey != "" {
		s.byKey[idemKey]++
	}
	s.timestamps = append(s.timestamps, s.nowFn())

	delayMS := s.cfg.LatencyMS
	if s.cfg.LatencyJitterMS > 0 {
		delayMS += s.rng.Intn(s.cfg.LatencyJitterMS + 1)
	}
	delay := time.Duration(delayMS) * time.Millisecond

	roll := s.rng.Float64()
	if roll < s.cfg.PermanentRate {
		return delay, http.StatusBadRequest
	}
	roll -= s.cfg.PermanentRate
	if roll < s.cfg.Rate429 {
		return delay, http.StatusTooManyRequests
	}
	roll -= s.cfg.Rate429
	if roll < s.cfg.FailureRate {
		return delay, http.StatusInternalServerError
	}
	return delay, http.StatusAccepted
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		logger.Warn("failed to encode json response", slog.String("error", err.Error()))
	}
}

func loadMockConfig() mockConfig {
	port := strings.TrimSpace(os.Getenv("MOCK_PORT"))
	if port == "" {
		port = "8090"
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	return mockConfig{
		Port:            port,
		LatencyMS:       parseEnvInt("MOCK_LATENCY_MS", 0),
		LatencyJitterMS: parseEnvInt("MOCK_LATENCY_JITTER_MS", 0),
		FailureRate:     parseEnvFloat("MOCK_FAILURE_RATE", 0),
		Rate429:         parseEnvFloat("MOCK_429_RATE", 0),
		PermanentRate:   parseEnvFloat("MOCK_PERMANENT_RATE", 0),
		Seed:            parseEnvInt64("MOCK_SEED", 0),
	}
}

func parseEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return parsed
}

func parseEnvInt64(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return parsed
}

func parseEnvFloat(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return parsed
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg := loadMockConfig()
	logger := observability.NewLogger(os.Getenv("LOG_LEVEL"), os.Stdout).With(
		slog.String("service", "notification-mockprovider"),
	)
	logger.Info("mockprovider configured",
		slog.String("port", cfg.Port),
		slog.Int("latency_ms", cfg.LatencyMS),
		slog.Int("jitter_ms", cfg.LatencyJitterMS),
		slog.Float64("failure_rate", cfg.FailureRate),
		slog.Float64("rate_429", cfg.Rate429),
		slog.Float64("permanent_rate", cfg.PermanentRate),
	)

	ms := newMockServer(cfg, logger, nil)
	srv := &http.Server{
		Addr:              cfg.Port,
		Handler:           ms.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("mockprovider listening", slog.String("addr", cfg.Port))
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case serveErr := <-errCh:
		return serveErr
	case <-ctx.Done():
		logger.Info("mockprovider shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-errCh
}
