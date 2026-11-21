package config_test

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/config"
)

func mapLookup(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestLoadFromDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(mapLookup(nil))
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, ":9091", cfg.WorkerMetricsAddr)
	assert.Equal(t, "postgres://notif:notif@postgres:5432/notif?sslmode=disable", cfg.DatabaseURL)
	assert.Equal(t, "redis:6379", cfg.RedisAddr)
	assert.Equal(t, 95, cfg.RedisPoolSize, "REDIS_POOL_SIZE=0 must auto-size to 3*25 + 20")
	assert.Equal(t, 0, cfg.DBMaxConns)
	assert.Equal(t, int32(10), cfg.DBPoolMaxConns(10), "DB_MAX_CONNS=0 must keep the binary default")
	assert.Equal(t, int32(20), cfg.DBPoolMaxConns(20))
	assert.Equal(t, "http://mockprovider:8090/", cfg.ProviderURL)
	assert.Equal(t, 10*time.Second, cfg.ProviderTimeout)
	assert.False(t, cfg.ProviderExtended)
	assert.Equal(t, 100, cfg.RateLimitPerSec)
	assert.Equal(t, 1, cfg.RateLimitBurst)
	assert.NotEmpty(t, cfg.WorkerID)
	assert.Equal(t, 25, cfg.WorkerConcurrency)
	assert.Equal(t, 5, cfg.MaxAttempts)
	assert.Equal(t, 2*time.Second, cfg.RetryBase)
	assert.Equal(t, 5*time.Minute, cfg.RetryCap)
	assert.Equal(t, 200*time.Millisecond, cfg.RetryMin)
	assert.Equal(t, 15*time.Minute, cfg.RetryAfterMax)
	assert.Equal(t, 10, cfg.CBConsecutiveFail)
	assert.Equal(t, 20, cfg.CBMinRequests)
	assert.InDelta(t, 0.5, cfg.CBFailureRatio, 1e-9)
	assert.Equal(t, 60*time.Second, cfg.CBInterval)
	assert.Equal(t, 30*time.Second, cfg.CBOpenTimeout)
	assert.Equal(t, 3, cfg.CBHalfOpenMax)
	assert.Equal(t, 120, cfg.MaxCircuitDeferral)
	assert.Equal(t, 60*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, 15*time.Second, cfg.ReconcileInterval)
	assert.Equal(t, 30*time.Second, cfg.PendingStaleAfter)
	assert.Equal(t, 500*time.Millisecond, cfg.DelayedMoverIntv)
	assert.Equal(t, 5*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, 15*time.Second, cfg.HeartbeatTTL)
	assert.Equal(t, 2*time.Second, cfg.ClockSkewTolerance)
	assert.Equal(t, 15*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, int64(524288), cfg.MaxBodyBytes)
	assert.Equal(t, int64(16777216), cfg.BatchMaxBodyBytes)
	assert.Equal(t, 1000, cfg.BatchMaxItems)
	assert.Equal(t, 50, cfg.PageLimitDefault)
	assert.Equal(t, 200, cfg.PageLimitMax)
	assert.Equal(t, 24*time.Hour, cfg.IdempotencyTTL)
	assert.Equal(t, 720*time.Hour, cfg.ScheduleMaxAhead)
	assert.Equal(t, []string{"http://localhost:8081"}, cfg.CORSAllowedOrigins)
	assert.Equal(t, "http://jaeger:4318", cfg.OTELEndpoint)
	assert.InDelta(t, 1.0, cfg.OTELSamplerRatio, 1e-9)
	assert.Equal(t, "info", cfg.LogLevel)
}

func TestLoadFromOverrides(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"HTTP_ADDR":                      ":18080",
		"WORKER_METRICS_ADDR":            ":19091",
		"DATABASE_URL":                   "postgres://user:supersecret@db:5432/app",
		"REDIS_ADDR":                     "localhost:6380",
		"REDIS_POOL_SIZE":                "120",
		"DB_MAX_CONNS":                   "7",
		"PROVIDER_URL":                   "https://webhook.site/abc-123",
		"PROVIDER_TIMEOUT":               "5s",
		"PROVIDER_EXTENDED_FIELDS":       "true",
		"RATE_LIMIT_PER_SEC":             "250",
		"RATE_LIMIT_BURST":               "2",
		"WORKER_ID":                      "worker-custom-1",
		"WORKER_CONCURRENCY_PER_CHANNEL": "40",
		"MAX_ATTEMPTS":                   "7",
		"VISIBILITY_TIMEOUT":             "25s", // exact boundary: 2*5s + 15s = 25s
		"BATCH_MAX_ITEMS":                "500",
		"CORS_ALLOWED_ORIGINS":           "http://localhost:8081, https://example.com",
		"OTEL_EXPORTER_OTLP_ENDPOINT":    "",
		"OTEL_SAMPLER_RATIO":             "0.25",
		"LOG_LEVEL":                      "DEBUG",
	}

	cfg, err := config.LoadFrom(mapLookup(env))
	require.NoError(t, err)

	assert.Equal(t, ":18080", cfg.HTTPAddr)
	assert.Equal(t, 120, cfg.RedisPoolSize)
	assert.Equal(t, int32(7), cfg.DBPoolMaxConns(10), "DB_MAX_CONNS overrides every binary default")
	assert.Equal(t, int32(7), cfg.DBPoolMaxConns(20))
	assert.True(t, cfg.ProviderExtended)
	assert.Equal(t, 250, cfg.RateLimitPerSec)
	assert.Equal(t, "worker-custom-1", cfg.WorkerID)
	assert.Equal(t, 25*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, []string{"http://localhost:8081", "https://example.com"}, cfg.CORSAllowedOrigins)
	assert.Empty(t, cfg.OTELEndpoint)
	assert.InDelta(t, 0.25, cfg.OTELSamplerRatio, 1e-9)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestLoadFromInvalidCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "rate limit zero",
			env:     map[string]string{"RATE_LIMIT_PER_SEC": "0"},
			wantErr: "RATE_LIMIT_PER_SEC",
		},
		{
			name:    "rate limit negative",
			env:     map[string]string{"RATE_LIMIT_PER_SEC": "-5"},
			wantErr: "RATE_LIMIT_PER_SEC",
		},
		{
			name:    "worker concurrency zero",
			env:     map[string]string{"WORKER_CONCURRENCY_PER_CHANNEL": "0"},
			wantErr: "WORKER_CONCURRENCY_PER_CHANNEL",
		},
		{
			name:    "max attempts zero",
			env:     map[string]string{"MAX_ATTEMPTS": "0"},
			wantErr: "MAX_ATTEMPTS",
		},
		{
			name:    "batch max items exceeds 1000",
			env:     map[string]string{"BATCH_MAX_ITEMS": "1001"},
			wantErr: "BATCH_MAX_ITEMS",
		},
		{
			name:    "batch max items zero",
			env:     map[string]string{"BATCH_MAX_ITEMS": "0"},
			wantErr: "BATCH_MAX_ITEMS",
		},
		{
			name:    "redis pool negative",
			env:     map[string]string{"REDIS_POOL_SIZE": "-1"},
			wantErr: "REDIS_POOL_SIZE",
		},
		{
			name:    "db pool negative",
			env:     map[string]string{"DB_MAX_CONNS": "-1"},
			wantErr: "DB_MAX_CONNS",
		},
		{
			name:    "db pool above cap",
			env:     map[string]string{"DB_MAX_CONNS": "501"},
			wantErr: "DB_MAX_CONNS",
		},
		{
			name:    "visibility timeout violates 2x provider timeout + 15s",
			env:     map[string]string{"PROVIDER_TIMEOUT": "10s", "VISIBILITY_TIMEOUT": "34s"},
			wantErr: "VISIBILITY_TIMEOUT",
		},
		{
			name:    "invalid log level",
			env:     map[string]string{"LOG_LEVEL": "trace"},
			wantErr: "LOG_LEVEL",
		},
		{
			name:    "invalid provider url scheme",
			env:     map[string]string{"PROVIDER_URL": "ftp://mockprovider:8090"},
			wantErr: "PROVIDER_URL",
		},
		{
			name:    "invalid duration syntax",
			env:     map[string]string{"PROVIDER_TIMEOUT": "not-a-duration"},
			wantErr: "PROVIDER_TIMEOUT",
		},
		{
			name:    "invalid integer syntax",
			env:     map[string]string{"MAX_ATTEMPTS": "five"},
			wantErr: "MAX_ATTEMPTS",
		},
		{
			name:    "invalid bool syntax",
			env:     map[string]string{"PROVIDER_EXTENDED_FIELDS": "maybe"},
			wantErr: "PROVIDER_EXTENDED_FIELDS",
		},
		{
			name:    "invalid float syntax",
			env:     map[string]string{"CB_FAILURE_RATIO": "half"},
			wantErr: "CB_FAILURE_RATIO",
		},
		{
			name:    "heartbeat ttl not greater than interval",
			env:     map[string]string{"HEARTBEAT_INTERVAL": "5s", "HEARTBEAT_TTL": "5s"},
			wantErr: "HEARTBEAT_TTL",
		},
		{
			name:    "batch max body smaller than single max body",
			env:     map[string]string{"MAX_BODY_BYTES": "1000", "BATCH_MAX_BODY_BYTES": "999"},
			wantErr: "BATCH_MAX_BODY_BYTES",
		},
		{
			name:    "cb failure ratio out of range",
			env:     map[string]string{"CB_FAILURE_RATIO": "1.2"},
			wantErr: "CB_FAILURE_RATIO",
		},
		{
			name:    "otel sampler ratio out of range",
			env:     map[string]string{"OTEL_SAMPLER_RATIO": "-0.1"},
			wantErr: "OTEL_SAMPLER_RATIO",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.LoadFrom(mapLookup(tc.env))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestConfigLogValueMasksDatabasePassword(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(mapLookup(map[string]string{
		"DATABASE_URL": "postgres://notif:topsecretpass@postgres:5432/notif?sslmode=disable",
	}))
	require.NoError(t, err)

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Info("startup", slog.Any("config", cfg))

	logged := buf.String()
	assert.NotContains(t, logged, "topsecretpass", "database password must never appear in logs")
	assert.Contains(t, logged, "postgres://notif:***@postgres:5432/notif?sslmode=disable")
	assert.Contains(t, logged, `"provider_timeout":"10s"`)
	assert.Contains(t, logged, `"visibility_timeout":"1m0s"`)
	assert.Contains(t, logged, `"reconcile_interval":"15s"`)
	assert.Contains(t, logged, `"shutdown_timeout":"15s"`)
}
