package worker_test

import (
	"bytes"
	"context"
	"math"
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

// tokenWorker returns a worker whose limiter withholds the token for the first `blockedRounds`
// wait rounds (each round ends at its context deadline, as a contended GCRA wait would) and then
// grants it. It records how many lease extensions had happened when the token was granted and when
// the provider was called, which proves nothing runs between the token and the send.
func tokenWorker(t *testing.T, store *memWorkerStore, blockedRounds int32) (w *worker.Worker, q *queue.Queue, sender *fakeSender, extAtGrant, extAtSend *atomic.Int32) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q = queue.New(rdb)

	extAtGrant, extAtSend = &atomic.Int32{}, &atomic.Int32{}
	extensions := func() int32 {
		store.mu.Lock()
		defer store.mu.Unlock()
		return int32(min(store.leaseExtensions, math.MaxInt32)) // #nosec G115 -- clamped
	}
	var calls atomic.Int32
	limiter := funcLimiter(func(ctx context.Context, _ domain.Channel) error {
		if calls.Add(1) <= blockedRounds {
			<-ctx.Done() // still no token when the round ends
			return ctx.Err()
		}
		extAtGrant.Store(extensions())
		return nil
	})
	status, msgID := 202, "prov-msg"
	sender = &fakeSender{fn: func(context.Context, domain.Notification) (provider.Result, error) {
		extAtSend.Store(extensions())
		return provider.Result{Outcome: provider.OutcomeSent, HTTPStatus: &status, ProviderMessageID: &msgID}, nil
	}}
	w, err := worker.New(worker.Deps{
		Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: limiter, Sender: sender,
	})
	require.NoError(t, err)
	return w, q, sender, extAtGrant, extAtSend
}

// A wait that outlasts one round re-arms the lease while no token is held; once the token is
// granted the send follows at once, with no database call in between (that call bunched sends).
func TestLongTokenWaitExtendsTheLeaseBeforeTheTokenIsGranted(t *testing.T) {
	t.Parallel()
	store := newMemWorkerStore()
	id := pendingSMS(store)
	w, q, sender, extAtGrant, extAtSend := tokenWorker(t, store, 1)

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })
	assert.Equal(t, int32(1), extAtGrant.Load(), "the lease was re-armed before the token was granted")
	assert.Equal(t, extAtGrant.Load(), extAtSend.Load(), "no lease extension between the token and the send")
	assert.Equal(t, int32(1), sender.calls.Load())
}

// If the reconciler reclaimed the row while the worker waited, the worker must not send: another
// worker now owns the row, and a send here would be a duplicate.
func TestReclaimedDuringTokenWaitIsNotSent(t *testing.T) {
	t.Parallel()
	store := newMemWorkerStore()
	id := pendingSMS(store)
	store.reclaimBeforeExtend = true
	w, q, sender, _, _ := tokenWorker(t, store, 1)

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
	w, q, _, _, _ := tokenWorker(t, store, 0)

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
