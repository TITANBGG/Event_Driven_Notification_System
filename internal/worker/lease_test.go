package worker_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

// slowTokenWorker returns a worker whose rate-limit wait advances the injected clock by wait, as a
// long GCRA wait under contention would.
func slowTokenWorker(t *testing.T, store *memWorkerStore, wait time.Duration) (*worker.Worker, *queue.Queue, *fakeSender) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)

	var clock atomic.Int64
	clock.Store(time.Now().UTC().UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	limiter := funcLimiter(func(context.Context, domain.Channel) error {
		clock.Add(int64(wait))
		return nil
	})
	sender := sentSender()
	w, err := worker.New(worker.Deps{
		Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: limiter, Sender: sender, Now: now,
	})
	require.NoError(t, err)
	return w, q, sender
}

func TestLongTokenWaitExtendsTheLeaseBeforeSending(t *testing.T) {
	t.Parallel()
	store := newMemWorkerStore()
	id := pendingSMS(store)
	w, q, sender := slowTokenWorker(t, store, 3*time.Second)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })
	assert.Equal(t, 1, store.leaseExtensions)
	assert.Equal(t, int32(1), sender.calls.Load())
}

// If the reconciler reclaimed the row while the worker waited, the worker must not send: another
// worker now owns the row, and a send here would be a duplicate.
func TestReclaimedDuringTokenWaitIsNotSent(t *testing.T) {
	t.Parallel()
	store := newMemWorkerStore()
	id := pendingSMS(store)
	store.reclaimBeforeExtend = true
	w, q, sender := slowTokenWorker(t, store, 3*time.Second)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.leaseExtensions == 1
	})
	assert.Equal(t, int32(0), sender.calls.Load(), "a reclaimed row must never be sent by the old owner")
	assert.Empty(t, store.attemptList())
}

func TestShortTokenWaitSkipsTheLeaseExtension(t *testing.T) {
	t.Parallel()
	store := newMemWorkerStore()
	id := pendingSMS(store)
	w, q, _ := slowTokenWorker(t, store, 10*time.Millisecond)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })
	assert.Zero(t, store.leaseExtensions, "the normal path costs no extra query")
}

// A send cancelled by SHUTDOWN_TIMEOUT is recorded (counted attempt, outcome unknown) and rescheduled
// at once, instead of sitting in processing with no attempt row until the reconciler resets it.
func TestShutdownAbortedSendIsRecordedAndRescheduled(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	inFlight := make(chan struct{}, 1)
	sender := &fakeSender{fn: func(ctx context.Context, _ domain.Notification) (provider.Result, error) {
		inFlight <- struct{}{}
		<-ctx.Done() // a provider slower than the shutdown window
		return provider.Result{}, ctx.Err()
	}}
	w, err := worker.New(worker.Deps{
		Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender,
	})
	require.NoError(t, err)
	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()
	select {
	case <-inFlight:
	case <-time.After(3 * time.Second):
		t.Fatal("send never started")
	}
	cancel() // SIGTERM; the 500ms SHUTDOWN_TIMEOUT then cancels the send
	require.NoError(t, <-errCh)

	row := store.get(id)
	assert.Equal(t, domain.StatusRetrying, row.Status, "rescheduled, not left in processing")
	assert.Equal(t, 1, row.Attempts, "the request may have reached the provider, so the attempt counts")
	attempts := store.attemptList()
	require.Len(t, attempts, 1)
	assert.True(t, attempts[0].Counted)
	require.NotNil(t, attempts[0].Error)
	assert.Contains(t, *attempts[0].Error, "provider outcome unknown")
}
