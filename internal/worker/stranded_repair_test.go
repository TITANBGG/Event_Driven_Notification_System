package worker_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

// A live worker whose follow-up Redis write failed leaves the ID tracked in {notif}:msg and parked in
// its own inflight list. The reconciler cannot see it (tracked + live heartbeat), so the worker's
// inflight sweeper must move it back to the queue and let the normal claim path deliver it.
func TestWorkerInflightSweeperRecoversStrandedItemOfLiveWorker(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)
	ctx := t.Context()

	cfg := testWorkerConfig()
	cfg.ReconcileInterval = 40 * time.Millisecond

	store := newMemWorkerStore()
	id := uuid.Must(uuid.NewV7())
	store.put(domain.Notification{
		ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Status: domain.StatusPending,
		Recipient: "a@example.com", Content: "hi", MaxAttempts: 5,
	})

	// Strand the ID: dequeued into this worker's inflight list but never processed by this process.
	require.NoError(t, q.Enqueue(ctx, queue.Meta{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}))
	meta, ok, err := q.Dequeue(ctx, domain.ChannelEmail, cfg.WorkerID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, id, meta.ID)

	sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
		return provider.Result{Outcome: provider.OutcomeSent}, nil
	}}
	var swept int
	w, err := worker.New(worker.Deps{
		Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender,
		Hooks: worker.Hooks{OnInflightSwept: func(n int) { swept += n }},
	})
	require.NoError(t, err)

	runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })

	assert.Equal(t, int32(1), sender.calls.Load())
	assert.Equal(t, 1, swept)
	ids, err := q.InflightIDs(ctx, cfg.WorkerID)
	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestQueueRequeueInflightIDsSkipsAckedIDs(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)
	ctx := t.Context()

	stranded := uuid.Must(uuid.NewV7())
	acked := uuid.Must(uuid.NewV7())
	for _, id := range []uuid.UUID{stranded, acked} {
		require.NoError(t, q.Enqueue(ctx, queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
		_, ok, err := q.Dequeue(ctx, domain.ChannelSMS, "w1")
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, q.Ack(ctx, "w1", acked))

	moved, err := q.RequeueInflightIDs(ctx, "w1", []uuid.UUID{stranded, acked})
	require.NoError(t, err)
	assert.Equal(t, int64(1), moved)

	got, err := mr.List(queue.Key(domain.ChannelSMS, domain.PriorityHigh))
	require.NoError(t, err)
	assert.Equal(t, []string{stranded.String()}, got)
}

// With a tracked backlog larger than one cycle's page budget, an untracked row beyond the budget
// must still be repaired on a later cycle because the keyset cursor persists between cycles.
func TestReconcilerCursorPersistsAcrossCycles(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)
	ctx := t.Context()

	store := newMemReconcilerStore()
	old := time.Now().UTC().Add(-time.Minute)
	const batchLimit = 2
	// 5 pages * 2 rows = 10 rows per cycle; the 13th row (highest UUIDv7) is beyond the first cycle.
	for i := 0; i < 12; i++ {
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Status: domain.StatusPending, MaxAttempts: 5, UpdatedAt: old})
		require.NoError(t, q.Enqueue(ctx, queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}))
	}
	lost := uuid.Must(uuid.NewV7())
	store.put(domain.Notification{ID: lost, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Status: domain.StatusPending, MaxAttempts: 5, UpdatedAt: old})

	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.WorkerID = "reconciler-cursor-test"
	rec, err := worker.NewReconciler(worker.ReconcilerDeps{
		Config: cfg, Logger: observability.NewLogger("error", io.Discard),
		Store: store, Queue: q, BatchLimit: batchLimit,
	})
	require.NoError(t, err)

	isTracked := func() bool {
		tr, trErr := q.IsTracked(ctx, []uuid.UUID{lost})
		require.NoError(t, trErr)
		return tr[lost]
	}

	require.NoError(t, rec.RunCycle(ctx))
	assert.False(t, isTracked(), "first cycle exhausts its page budget on the tracked backlog")

	require.NoError(t, rec.RunCycle(ctx))
	assert.True(t, isTracked(), "second cycle resumes from the saved cursor and repairs the lost row")
}
