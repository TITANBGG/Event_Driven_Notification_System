package worker_test

import (
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

func httpStatusServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *provider.Client) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client := provider.NewClient(provider.ClientConfig{
		URL:     srv.URL,
		Timeout: 2 * time.Second,
		Logger:  observability.NewLogger("error", io.Discard),
	})
	return srv, client
}

func TestWorkerRetryAndFailureSequences(t *testing.T) {
	t.Parallel()

	t.Run("sequence_500_500_202_succeeds_on_third_attempt", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 10)
		t.Cleanup(func() { _ = rdb.Close() })
		q := queue.New(rdb)

		var calls atomic.Int32
		_, client := httpStatusServer(t, func(w http.ResponseWriter, _ *http.Request) {
			n := calls.Add(1)
			if n <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"messageId":"ext-retry-ok"}`))
		})

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{
			ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
			Status: domain.StatusPending, Recipient: "+905551234567", Content: "retry ok", MaxAttempts: 5,
		})

		cfg := testWorkerConfig()
		cfg.DelayedMoverIntv = 15 * time.Millisecond
		policy := worker.NewExponentialRetryPolicy(15*time.Millisecond, 40*time.Millisecond, 10*time.Millisecond, time.Second, worker.WithRandSource(rand.NewPCG(1, 2)))

		var retryCount atomic.Int32
		w, err := worker.New(worker.Deps{
			Config:      cfg,
			Logger:      observability.NewLogger("error", io.Discard),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      client,
			RetryPolicy: policy,
			Hooks: worker.Hooks{
				OnRetryScheduled: func(_ domain.Channel) { retryCount.Add(1) },
			},
		})
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))

		runWorkerUntil(t, w, func() bool {
			return store.get(id).Status == domain.StatusSent
		})

		got := store.get(id)
		assert.Equal(t, 3, got.Attempts)
		assert.Equal(t, int32(2), retryCount.Load())
		require.NotNil(t, got.ProviderMessageID)
		assert.Equal(t, "ext-retry-ok", *got.ProviderMessageID)

		attempts := store.attemptList()
		require.Len(t, attempts, 3)
		assert.Equal(t, domain.OutcomeTransient, attempts[0].Outcome)
		assert.Equal(t, domain.OutcomeTransient, attempts[1].Outcome)
		assert.Equal(t, domain.OutcomeSent, attempts[2].Outcome)
		assert.False(t, mr.Exists(queue.DLQListKey))
	})

	t.Run("sequence_500_x5_exhausts_attempts_and_deadletters", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 10)
		t.Cleanup(func() { _ = rdb.Close() })
		q := queue.New(rdb)

		_, client := httpStatusServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{
			ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal,
			Status: domain.StatusPending, Recipient: "a@b.com", Content: "always 500", MaxAttempts: 5,
		})

		cfg := testWorkerConfig()
		cfg.DelayedMoverIntv = 15 * time.Millisecond
		policy := worker.NewExponentialRetryPolicy(10*time.Millisecond, 25*time.Millisecond, 10*time.Millisecond, time.Second, worker.WithRandSource(rand.NewPCG(3, 4)))

		w, err := worker.New(worker.Deps{
			Config:      cfg,
			Logger:      observability.NewLogger("error", io.Discard),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      client,
			RetryPolicy: policy,
		})
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}))

		runWorkerUntil(t, w, func() bool {
			return store.get(id).Status == domain.StatusFailed
		})

		got := store.get(id)
		assert.Equal(t, 5, got.Attempts)
		require.NotNil(t, got.LastError)
		assert.Contains(t, *got.LastError, "max attempts reached:")
		assert.Len(t, store.attemptList(), 5)

		dlq, err := mr.List(queue.DLQListKey)
		require.NoError(t, err)
		assert.Equal(t, []string{id.String()}, dlq)
	})

	t.Run("sequence_400_fails_permanently_without_retry", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 10)
		t.Cleanup(func() { _ = rdb.Close() })
		q := queue.New(rdb)

		var calls atomic.Int32
		_, client := httpStatusServer(t, func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		})

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{
			ID: id, Channel: domain.ChannelPush, Priority: domain.PriorityLow,
			Status: domain.StatusPending, Recipient: "tok", Content: "bad req", MaxAttempts: 5,
		})

		cfg := testWorkerConfig()
		cfg.CBConsecutiveFail = 1
		policy := worker.NewExponentialRetryPolicy(time.Second, time.Minute, time.Second, time.Minute)
		breaker := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
			Config: cfg,
			Logger: observability.NewLogger("error", io.Discard),
		})
		w, err := worker.New(worker.Deps{
			Config:      cfg,
			Logger:      observability.NewLogger("error", io.Discard),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      client,
			RetryPolicy: policy,
			Breaker:     breaker,
		})
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelPush, Priority: domain.PriorityLow}))

		runWorkerUntil(t, w, func() bool {
			return store.get(id).Status == domain.StatusFailed
		})

		assert.Equal(t, 1, store.get(id).Attempts)
		assert.Equal(t, int32(1), calls.Load())
		assert.Equal(t, gobreaker.StateClosed, breaker.State(domain.ChannelPush))
		dlq, err := mr.List(queue.DLQListKey)
		require.NoError(t, err)
		assert.Equal(t, []string{id.String()}, dlq)
	})

	t.Run("sequence_429_retry_after_7_schedules_at_7s", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 10)
		t.Cleanup(func() { _ = rdb.Close() })
		q := queue.New(rdb)

		_, client := httpStatusServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		})

		fixedNow := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{
			ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
			Status: domain.StatusPending, Recipient: "+905551234567", Content: "rate limited", MaxAttempts: 5,
		})

		cfg := testWorkerConfig()
		policy := worker.NewExponentialRetryPolicy(100*time.Millisecond, 500*time.Millisecond, 50*time.Millisecond, 30*time.Second, worker.WithRandSource(rand.NewPCG(5, 6)))
		w, err := worker.New(worker.Deps{
			Config:      cfg,
			Logger:      observability.NewLogger("error", io.Discard),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      client,
			RetryPolicy: policy,
			Now:         func() time.Time { return fixedNow },
		})
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}))

		runWorkerUntil(t, w, func() bool {
			return store.get(id).Status == domain.StatusRetrying
		})

		got := store.get(id)
		require.NotNil(t, got.NextAttemptAt)
		assert.Equal(t, fixedNow.Add(7*time.Second), *got.NextAttemptAt)
		members, err := mr.ZMembers(queue.DelayedZSetKey)
		require.NoError(t, err)
		assert.Contains(t, members, id.String())
	})
}

func TestWorkerCircuitBreakerIntegrationAndMaxDeferrals(t *testing.T) {
	t.Parallel()

	t.Run("trips_after_10_503s_skips_provider_probes_3_half_open_then_closes", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 10)
		t.Cleanup(func() { _ = rdb.Close() })
		q := queue.New(rdb)

		var return202 atomic.Bool
		var providerCalls atomic.Int32
		_, client := httpStatusServer(t, func(w http.ResponseWriter, _ *http.Request) {
			providerCalls.Add(1)
			if return202.Load() {
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"messageId":"recovered"}`))
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		})

		store := newMemWorkerStore()
		cfg := testWorkerConfig()
		cfg.CBConsecutiveFail = 10
		cfg.CBMinRequests = 50
		cfg.CBOpenTimeout = 120 * time.Millisecond
		cfg.CBHalfOpenMax = 3
		cfg.MaxCircuitDeferral = 5
		cfg.DelayedMoverIntv = 15 * time.Millisecond

		breaker := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
			Config: cfg,
			Logger: observability.NewLogger("error", io.Discard),
		})

		// Keep normal 503 retries parked beyond 120ms open timeout while proving open circuit defers.
		tripPolicy := worker.NewExponentialRetryPolicy(5*time.Second, 5*time.Second, 5*time.Second, 10*time.Second)
		wTrip, err := worker.New(worker.Deps{
			Config:      cfg,
			Logger:      observability.NewLogger("error", io.Discard),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      client,
			RetryPolicy: tripPolicy,
			Breaker:     breaker,
			Jitter:      func(time.Duration) time.Duration { return 0 },
		})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			id := uuid.Must(uuid.NewV7())
			store.put(domain.Notification{
				ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
				Status: domain.StatusPending, Recipient: "+905551234567", Content: "trip", MaxAttempts: 5,
			})
			require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- wTrip.Run(ctx) }()

		require.Eventually(t, func() bool {
			return breaker.State(domain.ChannelSMS) == gobreaker.StateOpen
		}, 3*time.Second, 10*time.Millisecond)
		assert.Equal(t, int32(10), providerCalls.Load())

		// While circuit is open, enqueue an item that must be deferred WITHOUT calling provider.
		deferredID := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{
			ID: deferredID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
			Status: domain.StatusPending, Recipient: "+905551234567", Content: "defer me", MaxAttempts: 5,
		})
		require.NoError(t, q.Enqueue(ctx, queue.Meta{ID: deferredID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))

		require.Eventually(t, func() bool {
			return store.get(deferredID).Deferrals >= 1
		}, 2*time.Second, 5*time.Millisecond)
		gotDeferred := store.get(deferredID)
		assert.Equal(t, 0, gotDeferred.Attempts, "circuit_open must not burn counted attempts")
		assert.GreaterOrEqual(t, gotDeferred.Deferrals, 1)

		var foundUncountedCircuitOpen bool
		for _, a := range store.attemptList() {
			if a.NotificationID == deferredID && a.Outcome == domain.OutcomeCircuitOpen && !a.Counted {
				foundUncountedCircuitOpen = true
			}
		}
		assert.True(t, foundUncountedCircuitOpen)

		// Switch provider healthy; 3+ requests after CBOpenTimeout probe half-open and close circuit.
		return202.Store(true)
		for i := 0; i < 4; i++ {
			id := uuid.Must(uuid.NewV7())
			store.put(domain.Notification{
				ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
				Status: domain.StatusPending, Recipient: "+905551234567", Content: "probe", MaxAttempts: 5,
			})
			require.NoError(t, q.Enqueue(ctx, queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
		}

		require.Eventually(t, func() bool {
			return breaker.State(domain.ChannelSMS) == gobreaker.StateClosed && store.get(deferredID).Status == domain.StatusSent
		}, 4*time.Second, 15*time.Millisecond)

		cancel()
		require.NoError(t, <-errCh)
	})

	t.Run("fails_when_deferrals_exceed_max_circuit_deferrals", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 10)
		t.Cleanup(func() { _ = rdb.Close() })
		q := queue.New(rdb)

		cfg := testWorkerConfig()
		cfg.CBConsecutiveFail = 1
		cfg.CBMinRequests = 100
		cfg.CBOpenTimeout = 5 * time.Second
		cfg.MaxCircuitDeferral = 2

		breaker := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
			Config: cfg,
			Logger: observability.NewLogger("error", io.Discard),
		})
		_, err := breaker.Execute(domain.ChannelSMS, func() (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeTransient}, nil
		})
		require.NoError(t, err)
		require.Equal(t, gobreaker.StateOpen, breaker.State(domain.ChannelSMS))

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{
			ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
			Status: domain.StatusPending, Recipient: "+905551234567", Content: "too many deferrals",
			MaxAttempts: 5, Deferrals: 2,
		})

		sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
			t.Fatal("provider must not be invoked when circuit is open")
			return provider.Result{}, nil
		}}

		w, err := worker.New(worker.Deps{
			Config:      cfg,
			Logger:      observability.NewLogger("error", io.Discard),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      sender,
			Breaker:     breaker,
		})
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}))

		runWorkerUntil(t, w, func() bool {
			return store.get(id).Status == domain.StatusFailed
		})

		got := store.get(id)
		require.NotNil(t, got.LastError)
		assert.Equal(t, "circuit open too long", *got.LastError)
		dlq, err := mr.List(queue.DLQListKey)
		require.NoError(t, err)
		assert.Equal(t, []string{id.String()}, dlq)
	})
}
