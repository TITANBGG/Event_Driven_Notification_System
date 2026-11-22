package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
)

type fakeQueueInspector struct {
	mu      sync.RWMutex
	pingErr error
	depths  queue.MetricsDepths
}

func (f *fakeQueueInspector) Ping(_ context.Context) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pingErr
}

func (f *fakeQueueInspector) Depths(_ context.Context) (queue.MetricsDepths, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.pingErr != nil {
		return queue.MetricsDepths{}, f.pingErr
	}
	return f.depths, nil
}

func TestHealthReadyAndAlias(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)

	store := newFakeStore()
	qi := &fakeQueueInspector{}
	router := api.NewRouter(api.Deps{
		Config:         cfg,
		Logger:         observability.NewLogger("error", &bytes.Buffer{}),
		Store:          store,
		QueueInspector: qi,
	})

	for _, path := range []string{"/health", "/health/ready"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		var body api.ReadyStatusResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "ok", body.Status)
		assert.Equal(t, "ok", body.Checks.Postgres.Status)
		assert.Equal(t, "ok", body.Checks.Redis.Status)
	}

	// Postgres down -> 503 with postgres="down"
	store.SetPingError(errors.New("pg down"))
	recPG := httptest.NewRecorder()
	router.ServeHTTP(recPG, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/ready", nil))
	require.Equal(t, http.StatusServiceUnavailable, recPG.Code)
	var pgBody api.ReadyStatusResponse
	require.NoError(t, json.Unmarshal(recPG.Body.Bytes(), &pgBody))
	assert.Equal(t, "unavailable", pgBody.Status)
	assert.Equal(t, "down", pgBody.Checks.Postgres.Status)
	assert.Equal(t, "ok", pgBody.Checks.Redis.Status)
	store.SetPingError(nil)

	// Redis down -> still ready (200): the DB-first API keeps accepting notifications; the body reports it.
	qi.mu.Lock()
	qi.pingErr = errors.New("redis down")
	qi.mu.Unlock()
	recRD := httptest.NewRecorder()
	router.ServeHTTP(recRD, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/ready", nil))
	require.Equal(t, http.StatusOK, recRD.Code)
	var rdBody api.ReadyStatusResponse
	require.NoError(t, json.Unmarshal(recRD.Body.Bytes(), &rdBody))
	assert.Equal(t, "degraded", rdBody.Status)
	assert.Equal(t, "ok", rdBody.Checks.Postgres.Status)
	assert.Equal(t, "down", rdBody.Checks.Redis.Status)
}

func TestMetricsSnapshotJSONShapeAndTwoSecondCache(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	curNow := now

	store := newFakeStore()
	store.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return curNow
	}

	sentAt := now.Add(-15 * time.Second)
	createdAt := sentAt.Add(-400 * time.Millisecond)
	failedAt := now.Add(-30 * time.Second)
	sentID := uuid.Must(uuid.NewV7())
	store.SeedNotification(domain.Notification{
		ID: sentID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
		Status: domain.StatusSent, CreatedAt: createdAt, SentAt: &sentAt,
	})
	store.SeedNotification(domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Status: domain.StatusFailed, CreatedAt: failedAt.Add(-time.Second), FailedAt: &failedAt,
	})
	store.SeedNotification(domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelEmail, Priority: domain.PriorityNormal,
		Status: domain.StatusRetrying, CreatedAt: now.Add(-10 * time.Second),
	})
	store.SeedAttempt(domain.DeliveryAttempt{
		NotificationID: sentID, AttemptNo: 1, Counted: true, StartedAt: sentAt.Add(-95 * time.Millisecond), DurationMS: 95, Outcome: domain.OutcomeSent,
	})

	require.NoError(t, q.Enqueue(t.Context(),
		queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
		queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelPush, Priority: domain.PriorityNormal},
	))

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	router := api.NewRouter(api.Deps{
		Config:         cfg,
		Logger:         observability.NewLogger("error", &bytes.Buffer{}),
		Store:          store,
		QueueInspector: q,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return curNow
		},
	})

	// First request populates cache.
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/metrics", nil))
	require.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, 1, store.MetricsCalls())

	var snap api.MetricsSnapshotResponse
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &snap))
	assert.Equal(t, now, snap.GeneratedAt)
	assert.Equal(t, int64(1), snap.Queues.SMS.High)
	assert.Equal(t, int64(1), snap.Queues.Push.Normal)
	sms1m := snap.Windows.OneMinute.SMS
	assert.Equal(t, int64(1), sms1m.Sent)
	assert.Equal(t, int64(1), sms1m.Failed)
	require.NotNil(t, sms1m.SuccessRate)
	require.NotNil(t, sms1m.FailureRate)
	assert.Equal(t, 0.5, *sms1m.SuccessRate)
	assert.Equal(t, 0.5, *sms1m.FailureRate)
	assert.Equal(t, int64(1), snap.Windows.OneMinute.Email.Retrying)
	// Nothing sent or failed: the rates are unknown (null), not 0% success.
	assert.Nil(t, snap.Windows.OneMinute.Email.SuccessRate, "retrying only: no outcome to rate")
	assert.Nil(t, snap.Windows.OneMinute.Push.SuccessRate)
	assert.Nil(t, snap.Windows.OneMinute.Push.FailureRate)
	body := rec1.Body.String()
	assert.Contains(t, body, `"successRate":null`)
	// Windows in chronological order and channels in queue order, not alphabetical map order.
	assert.Less(t, strings.Index(body, `"1m":`), strings.Index(body, `"5m":`))
	assert.Less(t, strings.Index(body, `"5m":`), strings.Index(body, `"15m":`))
	win := body[strings.Index(body, `"windows":`):]
	assert.Less(t, strings.Index(win, `"sms":`), strings.Index(win, `"email":`))
	assert.Less(t, strings.Index(win, `"email":`), strings.Index(win, `"push":`))
	assert.InDelta(t, 400.0, snap.LatencyMS.EndToEnd.P50, 0.1)
	assert.InDelta(t, 95.0, snap.LatencyMS.ProviderCall.P50, 0.1)

	// Second request within 2s hits cache without querying DB again.
	mu.Lock()
	curNow = now.Add(1500 * time.Millisecond)
	mu.Unlock()
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/metrics", nil))
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, 1, store.MetricsCalls())

	// After 2s expiry, cache refreshes from live DB/Redis state.
	mu.Lock()
	curNow = now.Add(2500 * time.Millisecond)
	mu.Unlock()
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/metrics", nil))
	require.Equal(t, http.StatusOK, rec3.Code)
	assert.Equal(t, 2, store.MetricsCalls())
}

func TestPrometheusEndpointAndRequestCorrelationLogs(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)

	store := newFakeStore()
	metrics := observability.NewAPIMetrics()
	router := api.NewRouter(api.Deps{
		Config:         cfg,
		Logger:         observability.NewLogger("info", &logBuf),
		Store:          store,
		QueueInspector: &fakeQueueInspector{},
		Metrics:        metrics,
	})

	createReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(`{"recipient":"+905551234567","channel":"sms","content":"metrics probe","priority":"high"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-Correlation-ID", "cid-prom-test-1")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	require.Equal(t, http.StatusAccepted, createRec.Code)

	var created api.NotificationResponse
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	cancelReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/notifications/"+created.ID.String()+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	router.ServeHTTP(cancelRec, cancelReq)
	require.Equal(t, http.StatusOK, cancelRec.Code)

	scrapeReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	scrapeRec := httptest.NewRecorder()
	router.ServeHTTP(scrapeRec, scrapeReq)
	require.Equal(t, http.StatusOK, scrapeRec.Code)

	body := scrapeRec.Body.String()
	assert.Contains(t, body, `notifications_created_total{channel="sms",priority="high"} 1`)
	// The API decides cancellations, so it counts them; sent/retrying/failed come from workers only.
	assert.Contains(t, body, `notifications_processed_total{channel="sms",status="cancelled"} 1`)
	assert.NotContains(t, body, `status="sent"`)
	assert.Contains(t, body, `http_requests_total{code="200",method="POST",route="/api/v1/notifications/{id}/cancel"}`)
	assert.NotContains(t, body, created.ID.String(), "raw notification UUID must never appear in Prometheus labels")
	assert.Contains(t, body, "go_goroutines")

	// Request access log must include correlation_id and never raw recipient/content.
	logText := logBuf.String()
	assert.Contains(t, logText, `"correlation_id":"cid-prom-test-1"`)
	assert.NotContains(t, logText, "+905551234567")
	assert.NotContains(t, logText, "metrics probe")
}
