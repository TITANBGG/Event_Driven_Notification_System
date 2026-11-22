package worker_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

func throttledSender(retryAfter time.Duration) *fakeSender {
	status := 429
	return &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
		return provider.Result{Outcome: provider.OutcomeThrottled, HTTPStatus: &status, RetryAfter: &retryAfter}, nil
	}}
}

// A 429 means "slow down", not "this message is broken": the row is deferred with its attempt given
// back, so a long throttling period cannot exhaust MAX_ATTEMPTS and push healthy messages to the DLQ.
func TestThrottledSendIsDeferredWithoutUsingAnAttempt(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	start := time.Now().UTC()
	w, err := worker.New(worker.Deps{
		Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: throttledSender(7 * time.Second),
		RetryPolicy: worker.NewExponentialRetryPolicy(2*time.Second, 5*time.Minute, 200*time.Millisecond, 15*time.Minute),
	})
	require.NoError(t, err)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusRetrying })

	row := store.get(id)
	assert.Equal(t, 0, row.Attempts, "a 429 must not use an attempt")
	assert.Equal(t, 1, row.Deferrals)
	require.NotNil(t, row.NextAttemptAt)
	assert.GreaterOrEqual(t, row.NextAttemptAt.Sub(start), 7*time.Second, "Retry-After is honored")

	attempts := store.attemptList()
	require.Len(t, attempts, 1, "the provider call is still recorded")
	assert.False(t, attempts[0].Counted)
	require.NotNil(t, attempts[0].HTTPStatus)
	assert.Equal(t, 429, *attempts[0].HTTPStatus)

	members, err := mr.ZMembers(queue.DelayedZSetKey)
	require.NoError(t, err)
	assert.Contains(t, members, id.String())
}

// Throttling is bounded by MAX_CIRCUIT_DEFERRALS so a provider that never stops returning 429 still
// ends in the DLQ instead of deferring forever.
func TestThrottledSendFailsAfterMaxDeferrals(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	row := store.get(id)
	row.Deferrals = 3
	store.put(row)
	cfg := testWorkerConfig()
	cfg.MaxCircuitDeferral = 3
	w, err := worker.New(worker.Deps{
		Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: throttledSender(time.Second),
	})
	require.NoError(t, err)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusFailed })
	require.NotNil(t, store.get(id).LastError)
	assert.Contains(t, *store.get(id).LastError, "throttled too long")
}

// A 429 storm must not open the breaker: the provider is up, it only enforces its own rate.
func TestChannelBreakerTreatsThrottlingAsHealthy(t *testing.T) {
	t.Parallel()
	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cb := worker.NewChannelBreaker(worker.ChannelBreakerConfig{Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{})})
	status := 429
	for i := 0; i < 50; i++ {
		res, err := cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeThrottled, HTTPStatus: &status}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, provider.OutcomeThrottled, res.Outcome)
	}
	assert.Equal(t, gobreaker.StateClosed, cb.State(domain.ChannelSMS))
}

// A provider rejecting our credentials (401/403/407) fails every message on the channel, so it counts
// as a breaker failure: the circuit opens after CB_CONSECUTIVE_FAILURES instead of the whole queue
// draining into the DLQ. Other permanent 4xx (bad payload) still count as successes.
func TestChannelBreakerOpensOnProviderAuthRejections(t *testing.T) {
	t.Parallel()
	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.CBConsecutiveFail = 5

	for _, tc := range []struct {
		status   int
		wantOpen bool
	}{
		{401, true}, {403, true}, {407, true}, {400, false}, {404, false}, {422, false},
	} {
		cb := worker.NewChannelBreaker(worker.ChannelBreakerConfig{Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{})})
		status := tc.status
		for i := 0; i < 5; i++ {
			res, err := cb.Execute(domain.ChannelEmail, func() (provider.Result, error) {
				return provider.Result{Outcome: provider.OutcomePermanent, HTTPStatus: &status}, nil
			})
			require.NoError(t, err, "the message outcome is returned unchanged")
			assert.Equal(t, provider.OutcomePermanent, res.Outcome)
		}
		assert.Equal(t, tc.wantOpen, cb.State(domain.ChannelEmail) == gobreaker.StateOpen, "HTTP %d", tc.status)
	}
}
