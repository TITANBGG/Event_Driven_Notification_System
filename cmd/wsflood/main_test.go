package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

func TestParsePrometheusSampleAndWSURL(t *testing.T) {
	t.Parallel()

	sampleText := `
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 42
process_open_fds 19
go_memstats_alloc_bytes 1.25e+07
`
	s, err := parsePrometheusSample(strings.NewReader(sampleText))
	require.NoError(t, err)
	assert.Equal(t, 42.0, s.Goroutines)
	assert.Equal(t, 19.0, s.OpenFDs)
	assert.Equal(t, 12500000.0, s.HeapBytes)

	assert.Equal(t, "ws://localhost:8080/api/v1/ws?channel=sms", toWSURL("http://localhost:8080/", "/api/v1/ws?channel=sms"))
	assert.Equal(t, "wss://notif.example.com/api/v1/ws?channel=push", toWSURL("https://notif.example.com", "/api/v1/ws?channel=push"))
}

func TestWSCapacityLimitAndReuseAgainstInMemoryRouter(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		HTTPAddr:           ":0",
		MaxBodyBytes:       524288,
		BatchMaxBodyBytes:  16777216,
		BatchMaxItems:      1000,
		PageLimitDefault:   50,
		PageLimitMax:       200,
		IdempotencyTTL:     24 * time.Hour,
		ScheduleMaxAhead:   720 * time.Hour,
		ClockSkewTolerance: 2 * time.Second,
		MaxAttempts:        5,
		MaxWSClients:       8,
		CORSAllowedOrigins: []string{"*"},
	}

	hub := realtime.NewHub(realtime.HubConfig{MaxClients: cfg.MaxWSClients})
	ctx := t.Context()
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(ctx) }()
	t.Cleanup(func() { require.NoError(t, <-hubDone) })

	router := api.NewRouter(api.Deps{
		Config:  cfg,
		Logger:  observability.NewLogger("error", &bytes.Buffer{}),
		Hub:     hub,
		Metrics: observability.NewAPIMetrics(),
	})

	ts := httptest.NewServer(router)
	defer ts.Close()

	// Exercise WS capacity saturation + overflow rejection + slot reuse with tiny MaxClients=8.
	floodCfg := floodConfig{
		baseURL:        ts.URL,
		maxClients:     cfg.MaxWSClients,
		dialWorkers:    4,
		skipBoundaries: true,
	}
	var out bytes.Buffer
	hc := ts.Client()
	require.NoError(t, runWSCapacityAndReuse(ctx, hc, floodCfg, &out))
	assert.Contains(t, out.String(), "rejected with 503 too_many_ws_clients")
	assert.Contains(t, out.String(), "Slot reuse:")

	// Also verify WS invalid requests match expected codes against real router.
	code, body, err := doRequest(ctx, hc, http.MethodGet, ts.URL+"/api/v1/ws", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, string(body), "missing_ws_filter")
}
