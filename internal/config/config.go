// Package config loads and validates runtime configuration from environment variables,
// failing fast on invalid bounds before API or worker replicas begin serving traffic.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/TITANBGG/notification-system/internal/observability"
)

// Config holds the complete typed configuration for API and worker binaries.
// All fields mirror CLAUDE.md Section 8 and carry safe production defaults so
// docker compose works out of the box with zero .env configuration.
type Config struct {
	HTTPAddr           string
	WorkerMetricsAddr  string
	DatabaseURL        string
	RedisAddr          string
	RedisPoolSize      int
	ProviderURL        string
	ProviderTimeout    time.Duration
	ProviderExtended   bool
	RateLimitPerSec    int
	RateLimitBurst     int
	WorkerID           string
	WorkerConcurrency  int
	MaxAttempts        int
	RetryBase          time.Duration
	RetryCap           time.Duration
	RetryMin           time.Duration
	RetryAfterMax      time.Duration
	CBConsecutiveFail  int
	CBMinRequests      int
	CBFailureRatio     float64
	CBInterval         time.Duration
	CBOpenTimeout      time.Duration
	CBHalfOpenMax      int
	MaxCircuitDeferral int
	VisibilityTimeout  time.Duration
	ReconcileInterval  time.Duration
	PendingStaleAfter  time.Duration
	DelayedMoverIntv   time.Duration
	HeartbeatInterval  time.Duration
	HeartbeatTTL       time.Duration
	ClockSkewTolerance time.Duration
	ShutdownTimeout    time.Duration
	MaxBodyBytes       int64
	BatchMaxBodyBytes  int64
	BatchMaxItems      int
	PageLimitDefault   int
	PageLimitMax       int
	IdempotencyTTL     time.Duration
	ScheduleMaxAhead   time.Duration
	CORSAllowedOrigins []string
	OTELEndpoint       string
	OTELSamplerRatio   float64
	LogLevel           string
}

// Load reads environment variables from the current process and validates all invariants
// so services share identical defaults and fail-fast startup bounds.
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom parses environment variables using a caller-provided lookup function instead of
// os.LookupEnv so parallel unit tests never mutate process-global state.
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	var parseErrs []error

	getStr := func(key, def string) string {
		if v, ok := lookup(key); ok {
			trimmed := strings.TrimSpace(v)
			if trimmed != "" {
				return trimmed
			}
		}
		return def
	}

	getStrAllowEmpty := func(key, def string) string {
		if v, ok := lookup(key); ok {
			return strings.TrimSpace(v)
		}
		return def
	}

	getInt := func(key string, def int) int {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("invalid integer for %s: %w", key, err))
			return def
		}
		return parsed
	}

	getInt64 := func(key string, def int64) int64 {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("invalid int64 for %s: %w", key, err))
			return def
		}
		return parsed
	}

	getFloat := func(key string, def float64) float64 {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("invalid float for %s: %w", key, err))
			return def
		}
		return parsed
	}

	getBool := func(key string, def bool) bool {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("invalid bool for %s: %w", key, err))
			return def
		}
		return parsed
	}

	getDuration := func(key string, def time.Duration) time.Duration {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("invalid duration for %s: %w", key, err))
			return def
		}
		return parsed
	}

	workerID := getStr("WORKER_ID", "")
	if workerID == "" {
		host, err := os.Hostname()
		if err != nil || strings.TrimSpace(host) == "" {
			workerID = "worker-unknown"
		} else {
			workerID = host
		}
	}

	cfg := Config{
		HTTPAddr:           getStr("HTTP_ADDR", ":8080"),
		WorkerMetricsAddr:  getStr("WORKER_METRICS_ADDR", ":9091"),
		DatabaseURL:        getStr("DATABASE_URL", "postgres://notif:notif@postgres:5432/notif?sslmode=disable"),
		RedisAddr:          getStr("REDIS_ADDR", "redis:6379"),
		RedisPoolSize:      getInt("REDIS_POOL_SIZE", 0),
		ProviderURL:        getStr("PROVIDER_URL", "http://mockprovider:8090/"),
		ProviderTimeout:    getDuration("PROVIDER_TIMEOUT", 10*time.Second),
		ProviderExtended:   getBool("PROVIDER_EXTENDED_FIELDS", false),
		RateLimitPerSec:    getInt("RATE_LIMIT_PER_SEC", 100),
		RateLimitBurst:     getInt("RATE_LIMIT_BURST", 1),
		WorkerID:           workerID,
		WorkerConcurrency:  getInt("WORKER_CONCURRENCY_PER_CHANNEL", 25),
		MaxAttempts:        getInt("MAX_ATTEMPTS", 5),
		RetryBase:          getDuration("RETRY_BASE", 2*time.Second),
		RetryCap:           getDuration("RETRY_CAP", 5*time.Minute),
		RetryMin:           getDuration("RETRY_MIN", 200*time.Millisecond),
		RetryAfterMax:      getDuration("RETRY_AFTER_MAX", 15*time.Minute),
		CBConsecutiveFail:  getInt("CB_CONSECUTIVE_FAILURES", 10),
		CBMinRequests:      getInt("CB_MIN_REQUESTS", 20),
		CBFailureRatio:     getFloat("CB_FAILURE_RATIO", 0.5),
		CBInterval:         getDuration("CB_INTERVAL", 60*time.Second),
		CBOpenTimeout:      getDuration("CB_OPEN_TIMEOUT", 30*time.Second),
		CBHalfOpenMax:      getInt("CB_HALF_OPEN_MAX", 3),
		MaxCircuitDeferral: getInt("MAX_CIRCUIT_DEFERRALS", 120),
		VisibilityTimeout:  getDuration("VISIBILITY_TIMEOUT", 60*time.Second),
		ReconcileInterval:  getDuration("RECONCILE_INTERVAL", 15*time.Second),
		PendingStaleAfter:  getDuration("PENDING_STALE_AFTER", 30*time.Second),
		DelayedMoverIntv:   getDuration("DELAYED_MOVER_INTERVAL", 500*time.Millisecond),
		HeartbeatInterval:  getDuration("HEARTBEAT_INTERVAL", 5*time.Second),
		HeartbeatTTL:       getDuration("HEARTBEAT_TTL", 15*time.Second),
		ClockSkewTolerance: getDuration("CLOCK_SKEW_TOLERANCE", 2*time.Second),
		ShutdownTimeout:    getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
		MaxBodyBytes:       getInt64("MAX_BODY_BYTES", 524288),
		BatchMaxBodyBytes:  getInt64("BATCH_MAX_BODY_BYTES", 16777216),
		BatchMaxItems:      getInt("BATCH_MAX_ITEMS", 1000),
		PageLimitDefault:   getInt("PAGE_LIMIT_DEFAULT", 50),
		PageLimitMax:       getInt("PAGE_LIMIT_MAX", 200),
		IdempotencyTTL:     getDuration("IDEMPOTENCY_TTL", 24*time.Hour),
		ScheduleMaxAhead:   getDuration("SCHEDULE_MAX_AHEAD", 720*time.Hour),
		CORSAllowedOrigins: parseCSV(getStrAllowEmpty("CORS_ALLOWED_ORIGINS", "http://localhost:8081")),
		OTELEndpoint:       getStrAllowEmpty("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318"),
		OTELSamplerRatio:   getFloat("OTEL_SAMPLER_RATIO", 1.0),
		LogLevel:           strings.ToLower(getStr("LOG_LEVEL", "info")),
	}

	// Auto-size RedisPoolSize (3*concurrency + 20) so all 3 channel worker pools can hold
	// concurrent connections without starving background loops (mover, reconciler, heartbeat, pub/sub).
	if cfg.RedisPoolSize == 0 && cfg.WorkerConcurrency > 0 {
		cfg.RedisPoolSize = 3*cfg.WorkerConcurrency + 20
	}

	if err := cfg.Validate(); err != nil {
		parseErrs = append(parseErrs, err)
	}
	if len(parseErrs) > 0 {
		return Config{}, errors.Join(parseErrs...)
	}
	return cfg, nil
}

// Validate enforces cross-field invariants and bounds at startup to prevent unsafe
// timeout windows or invalid worker/API limits from entering production traffic.
func (c Config) Validate() error {
	var errs []error

	if strings.TrimSpace(c.HTTPAddr) == "" {
		errs = append(errs, errors.New("HTTP_ADDR must not be empty"))
	}
	if strings.TrimSpace(c.WorkerMetricsAddr) == "" {
		errs = append(errs, errors.New("WORKER_METRICS_ADDR must not be empty"))
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		errs = append(errs, errors.New("DATABASE_URL must not be empty"))
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		errs = append(errs, errors.New("REDIS_ADDR must not be empty"))
	}
	if c.RedisPoolSize < 0 {
		errs = append(errs, fmt.Errorf("REDIS_POOL_SIZE (%d) must be >= 0", c.RedisPoolSize))
	}

	parsedURL, urlErr := url.Parse(c.ProviderURL)
	if urlErr != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		errs = append(errs, fmt.Errorf("PROVIDER_URL (%q) must be a valid http or https URL", c.ProviderURL))
	}

	if c.RateLimitPerSec <= 0 {
		errs = append(errs, fmt.Errorf("RATE_LIMIT_PER_SEC (%d) must be > 0", c.RateLimitPerSec))
	}
	if c.RateLimitBurst < 1 {
		errs = append(errs, fmt.Errorf("RATE_LIMIT_BURST (%d) must be >= 1", c.RateLimitBurst))
	}
	if c.WorkerConcurrency <= 0 {
		errs = append(errs, fmt.Errorf("WORKER_CONCURRENCY_PER_CHANNEL (%d) must be > 0", c.WorkerConcurrency))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("MAX_ATTEMPTS (%d) must be >= 1", c.MaxAttempts))
	}

	if c.BatchMaxItems < 1 || c.BatchMaxItems > 1000 {
		errs = append(errs, fmt.Errorf("BATCH_MAX_ITEMS (%d) must be between 1 and 1000", c.BatchMaxItems))
	}
	if c.MaxBodyBytes <= 0 {
		errs = append(errs, fmt.Errorf("MAX_BODY_BYTES (%d) must be > 0", c.MaxBodyBytes))
	}
	if c.BatchMaxBodyBytes < c.MaxBodyBytes {
		errs = append(errs, fmt.Errorf("BATCH_MAX_BODY_BYTES (%d) must be >= MAX_BODY_BYTES (%d)", c.BatchMaxBodyBytes, c.MaxBodyBytes))
	}
	if c.PageLimitDefault < 1 {
		errs = append(errs, fmt.Errorf("PAGE_LIMIT_DEFAULT (%d) must be >= 1", c.PageLimitDefault))
	}
	if c.PageLimitMax < c.PageLimitDefault {
		errs = append(errs, fmt.Errorf("PAGE_LIMIT_MAX (%d) must be >= PAGE_LIMIT_DEFAULT (%d)", c.PageLimitMax, c.PageLimitDefault))
	}

	if c.ProviderTimeout <= 0 {
		errs = append(errs, fmt.Errorf("PROVIDER_TIMEOUT (%s) must be > 0", c.ProviderTimeout))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT (%s) must be > 0", c.ShutdownTimeout))
	}
	if c.RetryMin <= 0 {
		errs = append(errs, fmt.Errorf("RETRY_MIN (%s) must be > 0", c.RetryMin))
	}
	if c.RetryBase < c.RetryMin {
		errs = append(errs, fmt.Errorf("RETRY_BASE (%s) must be >= RETRY_MIN (%s)", c.RetryBase, c.RetryMin))
	}
	if c.RetryCap < c.RetryBase {
		errs = append(errs, fmt.Errorf("RETRY_CAP (%s) must be >= RETRY_BASE (%s)", c.RetryCap, c.RetryBase))
	}
	if c.RetryAfterMax < c.RetryCap {
		errs = append(errs, fmt.Errorf("RETRY_AFTER_MAX (%s) must be >= RETRY_CAP (%s)", c.RetryAfterMax, c.RetryCap))
	}

	// VISIBILITY_TIMEOUT must cover >= 2*PROVIDER_TIMEOUT + 15s headroom so slow-but-alive
	// workers can finish HTTP sends and fenced updates before the reconciler reclaims the row.
	minVisibility := 2*c.ProviderTimeout + 15*time.Second
	if c.VisibilityTimeout < minVisibility {
		errs = append(errs, fmt.Errorf(
			"VISIBILITY_TIMEOUT (%s) must be >= 2*PROVIDER_TIMEOUT + 15s (%s)",
			c.VisibilityTimeout, minVisibility,
		))
	}

	if c.ReconcileInterval <= 0 {
		errs = append(errs, fmt.Errorf("RECONCILE_INTERVAL (%s) must be > 0", c.ReconcileInterval))
	}
	if c.PendingStaleAfter <= 0 {
		errs = append(errs, fmt.Errorf("PENDING_STALE_AFTER (%s) must be > 0", c.PendingStaleAfter))
	}
	if c.DelayedMoverIntv <= 0 {
		errs = append(errs, fmt.Errorf("DELAYED_MOVER_INTERVAL (%s) must be > 0", c.DelayedMoverIntv))
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, fmt.Errorf("HEARTBEAT_INTERVAL (%s) must be > 0", c.HeartbeatInterval))
	}
	if c.HeartbeatTTL <= c.HeartbeatInterval {
		errs = append(errs, fmt.Errorf("HEARTBEAT_TTL (%s) must be > HEARTBEAT_INTERVAL (%s)", c.HeartbeatTTL, c.HeartbeatInterval))
	}
	if c.ClockSkewTolerance < 0 {
		errs = append(errs, fmt.Errorf("CLOCK_SKEW_TOLERANCE (%s) must be >= 0", c.ClockSkewTolerance))
	}
	if c.IdempotencyTTL <= 0 {
		errs = append(errs, fmt.Errorf("IDEMPOTENCY_TTL (%s) must be > 0", c.IdempotencyTTL))
	}
	if c.ScheduleMaxAhead <= 0 {
		errs = append(errs, fmt.Errorf("SCHEDULE_MAX_AHEAD (%s) must be > 0", c.ScheduleMaxAhead))
	}

	if c.CBConsecutiveFail < 1 {
		errs = append(errs, fmt.Errorf("CB_CONSECUTIVE_FAILURES (%d) must be >= 1", c.CBConsecutiveFail))
	}
	if c.CBMinRequests < 1 {
		errs = append(errs, fmt.Errorf("CB_MIN_REQUESTS (%d) must be >= 1", c.CBMinRequests))
	}
	if c.CBFailureRatio <= 0 || c.CBFailureRatio > 1 {
		errs = append(errs, fmt.Errorf("CB_FAILURE_RATIO (%g) must be in (0, 1]", c.CBFailureRatio))
	}
	if c.CBInterval <= 0 {
		errs = append(errs, fmt.Errorf("CB_INTERVAL (%s) must be > 0", c.CBInterval))
	}
	if c.CBOpenTimeout <= 0 {
		errs = append(errs, fmt.Errorf("CB_OPEN_TIMEOUT (%s) must be > 0", c.CBOpenTimeout))
	}
	if c.CBHalfOpenMax < 1 {
		errs = append(errs, fmt.Errorf("CB_HALF_OPEN_MAX (%d) must be >= 1", c.CBHalfOpenMax))
	}
	if c.MaxCircuitDeferral < 1 {
		errs = append(errs, fmt.Errorf("MAX_CIRCUIT_DEFERRALS (%d) must be >= 1", c.MaxCircuitDeferral))
	}

	if c.OTELSamplerRatio < 0 || c.OTELSamplerRatio > 1 {
		errs = append(errs, fmt.Errorf("OTEL_SAMPLER_RATIO (%g) must be in [0, 1]", c.OTELSamplerRatio))
	}

	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("LOG_LEVEL (%q) must be one of debug, info, warn, error", c.LogLevel))
	}

	return errors.Join(errs...)
}

// LogValue implements slog.LogValuer so startup logs redact DATABASE_URL credentials automatically.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("http_addr", c.HTTPAddr),
		slog.String("worker_metrics_addr", c.WorkerMetricsAddr),
		slog.String("database_url", observability.MaskDatabaseURL(c.DatabaseURL)),
		slog.String("redis_addr", c.RedisAddr),
		slog.Int("redis_pool_size", c.RedisPoolSize),
		slog.String("provider_url", c.ProviderURL),
		slog.String("provider_timeout", c.ProviderTimeout.String()),
		slog.Bool("provider_extended_fields", c.ProviderExtended),
		slog.Int("rate_limit_per_sec", c.RateLimitPerSec),
		slog.Int("rate_limit_burst", c.RateLimitBurst),
		slog.String("worker_id", c.WorkerID),
		slog.Int("worker_concurrency_per_channel", c.WorkerConcurrency),
		slog.Int("max_attempts", c.MaxAttempts),
		slog.String("visibility_timeout", c.VisibilityTimeout.String()),
		slog.String("reconcile_interval", c.ReconcileInterval.String()),
		slog.String("shutdown_timeout", c.ShutdownTimeout.String()),
		slog.Int("batch_max_items", c.BatchMaxItems),
		slog.String("log_level", c.LogLevel),
	)
}

func parseCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
