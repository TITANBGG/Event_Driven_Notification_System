package api_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/ratelimit"
	"github.com/TITANBGG/notification-system/internal/repository"
)

func routerWith(t *testing.T, deps api.Deps) http.Handler {
	t.Helper()
	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	if deps.Store == nil {
		deps.Store = newFakeStore()
	}
	deps.Config = cfg
	deps.Logger = observability.NewLogger("error", &bytes.Buffer{})
	return api.NewRouter(deps)
}

const smsBody = `{"recipient":"+905551234567","channel":"sms","content":"gate"}`

func TestIngressGateRejectsCreatesWhileBacklogIsTooDeep(t *testing.T) {
	t.Parallel()
	gate := api.NewIngressGate(100)
	h := routerWith(t, api.Deps{IngressGate: gate})

	gate.Observe(99)
	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", smsBody, nil)
	require.Equal(t, http.StatusAccepted, rec.Code)

	gate.Observe(100)
	for _, path := range []string{"/api/v1/notifications", "/api/v1/notifications/batch"} {
		body := smsBody
		if path == "/api/v1/notifications/batch" {
			body = `{"notifications":[` + smsBody + `]}`
		}
		rec = doJSONRequest(t, h, http.MethodPost, path, body, nil)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, path)
		assert.Contains(t, rec.Body.String(), `"overloaded"`)
		assert.Equal(t, "5", rec.Header().Get("Retry-After"))
	}
	// Reads are never gated.
	rec = doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications?limit=1", "", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	gate.Observe(10)
	rec = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", smsBody, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code, "the gate reopens when the backlog drains")

	assert.False(t, api.NewIngressGate(0).Overloaded(), "0 disables the gate")
}

func TestAPIRateLimitPerKey(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	withTestAPIKeys(&cfg)
	h := api.NewRouter(api.Deps{
		Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: newFakeStore(),
		APIRateLimiter: ratelimit.NewKeyed(rdb, 3),
	})

	codes := map[int]int{}
	for i := 0; i < 5; i++ {
		rec := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications?limit=1", "", bearer(testReadKey))
		codes[rec.Code]++
		if rec.Code == http.StatusTooManyRequests {
			assert.Contains(t, rec.Body.String(), `"rate_limited"`)
			assert.NotEmpty(t, rec.Header().Get("Retry-After"))
		}
	}
	assert.Equal(t, 3, codes[http.StatusOK], "burst = rate")
	assert.Equal(t, 2, codes[http.StatusTooManyRequests])

	// Another key has its own budget.
	rec := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications?limit=1", "", bearer(testAdminKey))
	assert.Equal(t, http.StatusOK, rec.Code)

}

// failingLimiter reports a Redis error on every call, as a limiter would while Redis is down.
// (Closing a real miniredis instead leaves go-redis' background redial goroutine behind for goleak.)
type failingLimiter struct{}

func (failingLimiter) AllowKey(context.Context, string) (bool, time.Duration, error) {
	return false, 0, errors.New("dial tcp: connection refused")
}

// The API limiter fails open: the DB-first API keeps serving while Redis is down.
func TestAPIRateLimitFailsOpenWhenRedisIsDown(t *testing.T) {
	t.Parallel()
	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	withTestAPIKeys(&cfg)
	h := api.NewRouter(api.Deps{
		Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: newFakeStore(),
		APIRateLimiter: failingLimiter{},
	})
	for i := 0; i < 3; i++ {
		rec := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications?limit=1", "", bearer(testReadKey))
		assert.Equal(t, http.StatusOK, rec.Code)
	}
}

// blockingMetricsStore makes the snapshot's first query block until released, to observe concurrency.
type blockingMetricsStore struct {
	*fakeStore
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockingMetricsStore) MetricsWindowCounts(ctx context.Context) ([]repository.MetricsWindowRow, error) {
	s.calls.Add(1)
	<-s.release
	return s.fakeStore.MetricsWindowCounts(ctx)
}

// A burst of polls after the cache expires costs one set of PostgreSQL queries, not one per request.
func TestMetricsSnapshotIsComputedOnceForConcurrentRequests(t *testing.T) {
	t.Parallel()
	store := &blockingMetricsStore{fakeStore: newFakeStore(), release: make(chan struct{})}
	var mu sync.Mutex
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	h := routerWith(t, api.Deps{Store: store, Now: func() time.Time { mu.Lock(); defer mu.Unlock(); return now }})

	poll := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/metrics", nil))
		return rec.Code
	}

	// First snapshot: 20 concurrent requests, one computation, everyone gets 200.
	var wg sync.WaitGroup
	results := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- poll() }()
	}
	// Every request either waits for this refresh or, arriving after it, hits the fresh cache; in
	// neither case may a second computation start, so no scheduling delay is needed here.
	require.Eventually(t, func() bool { return store.calls.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	close(store.release)
	wg.Wait()
	close(results)
	for code := range results {
		assert.Equal(t, http.StatusOK, code)
	}
	assert.Equal(t, int32(1), store.calls.Load(), "one computation for 20 concurrent first requests")

	// After expiry, concurrent pollers are served while a single refresh runs.
	mu.Lock()
	now = now.Add(3 * time.Second)
	mu.Unlock()
	for i := 0; i < 10; i++ {
		assert.Equal(t, http.StatusOK, poll())
	}
	assert.Equal(t, int32(2), store.calls.Load(), "one refresh after expiry, then the fresh cache")
}

// panickingMetricsStore panics on the first snapshot query only.
type panickingMetricsStore struct {
	*fakeStore
	panicked atomic.Bool
}

func (s *panickingMetricsStore) MetricsWindowCounts(ctx context.Context) ([]repository.MetricsWindowRow, error) {
	if s.panicked.CompareAndSwap(false, true) {
		panic("boom while computing the snapshot")
	}
	return s.fakeStore.MetricsWindowCounts(ctx)
}

// A panic inside the single in-flight refresh must not leave the refresh marked as running:
// the next request recomputes instead of waiting forever.
func TestMetricsSnapshotRecoversFromAPanickingRefresh(t *testing.T) {
	t.Parallel()
	h := routerWith(t, api.Deps{Store: &panickingMetricsStore{fakeStore: newFakeStore()}})

	poll := func() int {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/metrics", nil))
		return rec.Code
	}
	assert.Equal(t, http.StatusInternalServerError, poll(), "the Recoverer answers the panicking request")
	assert.Equal(t, http.StatusOK, poll(), "the next request computes a fresh snapshot")
}
