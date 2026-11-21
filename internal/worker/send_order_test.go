package worker_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

type funcLimiter func(ctx context.Context, ch domain.Channel) error

func (f funcLimiter) Wait(ctx context.Context, ch domain.Channel) error { return f(ctx, ch) }

func sentSender() *fakeSender {
	status := 202
	msgID := "prov-msg"
	return &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
		return provider.Result{Outcome: provider.OutcomeSent, HTTPStatus: &status, ProviderMessageID: &msgID}, nil
	}}
}

func pendingSMS(store *memWorkerStore) uuid.UUID {
	id := uuid.Must(uuid.NewV7())
	store.put(domain.Notification{
		ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending,
		Recipient: "+905551234567", Content: "hi", MaxAttempts: 5,
	})
	return id
}

func (s *memWorkerStore) markSentCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.markSentHits
}

// The token must be taken after Claim, right before the provider call: a stall between token and send
// (DB pool wait, GC) would otherwise bunch sends together and break the per-channel 100/s window.
func TestRateLimitTokenIsTakenAfterClaimRightBeforeSend(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	var statusAtWait atomic.Value
	limiter := funcLimiter(func(_ context.Context, _ domain.Channel) error {
		statusAtWait.Store(store.get(id).Status)
		return nil
	})
	sender := sentSender()
	w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: limiter, Sender: sender})
	require.NoError(t, err)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })

	assert.Equal(t, domain.StatusProcessing, statusAtWait.Load(), "token must be requested after the row was claimed")
	assert.Equal(t, int32(1), sender.calls.Load())
}

// When no token can be had within the budget (limiter/Redis down), the claimed row is handed back with
// the fenced "not attempted" transition: no provider call, no attempt burned, retried via the delayed set.
func TestLimiterOutageReleasesClaimWithoutBurningAnAttempt(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	limiter := funcLimiter(func(_ context.Context, _ domain.Channel) error { return errors.New("redis: connection refused") })
	sender := sentSender()

	cfg := testWorkerConfig()
	// Budget = VISIBILITY_TIMEOUT - 2*PROVIDER_TIMEOUT - 5s, floored at 1s: keep the test at ~1s.
	cfg.ProviderTimeout = time.Second
	cfg.VisibilityTimeout = 8 * time.Second
	w, err := worker.New(worker.Deps{Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: limiter, Sender: sender})
	require.NoError(t, err)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusRetrying })

	row := store.get(id)
	assert.Equal(t, 0, row.Attempts, "the claim's attempt must be given back")
	assert.Equal(t, 1, row.Deferrals)
	assert.Equal(t, int32(0), sender.calls.Load(), "nothing may be sent without a token")
	assert.Empty(t, store.attemptList())
	members, err := mr.ZMembers(queue.DelayedZSetKey)
	require.NoError(t, err)
	assert.Contains(t, members, id.String(), "the row must be rescheduled, not dropped")
}

// After the provider accepted a message, a short DB failure on MarkSent is retried instead of leaving the
// row in processing for the reconciler, which would re-send it (a duplicate).
func TestMarkSentRetriesTransientDBErrorAfterProviderAccepted(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	store.markSentErrs = 2
	sender := sentSender()
	w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender})
	require.NoError(t, err)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })

	assert.Equal(t, 3, store.markSentCalls(), "two failures, then success")
	assert.Equal(t, int32(1), sender.calls.Load(), "the provider must be called exactly once")
	assert.Len(t, store.attemptList(), 1)
}

// A lost lease is final: another actor owns the row, so retrying MarkSent would be pointless.
func TestMarkSentDoesNotRetryALostLease(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	store.leaseLos = true
	sender := sentSender()
	w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender})
	require.NoError(t, err)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.markSentCalls() == 1 })

	assert.Never(t, func() bool { return store.markSentCalls() > 1 }, 400*time.Millisecond, 20*time.Millisecond)
}
