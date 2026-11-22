package worker_test

import (
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
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

// After a mass Redis loss every pending row is untracked. One cycle must repair up to
// maxPagesPerCycle pages (here 5 x 10 = 50 rows), not stop after the first page of repairs.
func TestReconcilerRepairsSeveralPagesPerCycle(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 10)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)

	store := newMemReconcilerStore()
	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.WorkerID = "worker-cap-1"
	rec, err := worker.NewReconciler(worker.ReconcilerDeps{
		Config: cfg, Logger: observability.NewLogger("error", io.Discard),
		Store: store, Queue: q, BatchLimit: 10,
	})
	require.NoError(t, err)

	stale := time.Now().UTC().Add(-2 * time.Minute)
	for i := 0; i < 60; i++ {
		store.put(domain.Notification{
			ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
			Status: domain.StatusPending, UpdatedAt: stale, MaxAttempts: 5,
		})
	}

	require.NoError(t, rec.RunCycle(t.Context()))
	queued, err := mr.List(queue.Key(domain.ChannelSMS, domain.PriorityNormal))
	require.NoError(t, err)
	assert.Len(t, queued, 50, "five full pages repaired in one cycle")

	require.NoError(t, rec.RunCycle(t.Context()))
	queued, err = mr.List(queue.Key(domain.ChannelSMS, domain.PriorityNormal))
	require.NoError(t, err)
	assert.Len(t, queued, 60, "the persisted cursor picks up the rest next cycle")
}

// lockStealingQueue lets the real queue run but reports the lease as lost on the first renewal,
// as if the cycle had outlived its TTL and another replica had taken the lock.
type lockStealingQueue struct {
	*queue.Queue
	renewals int
}

func (l *lockStealingQueue) ExtendReconcilerLock(context.Context, string, time.Duration) (bool, error) {
	l.renewals++
	return false, nil
}

func TestReconcilerStopsWhenItsLeaseIsLost(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 10)
	t.Cleanup(func() { _ = rdb.Close() })
	q := &lockStealingQueue{Queue: queue.New(rdb)}

	store := newMemReconcilerStore()
	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.WorkerID = "worker-lease-1"
	rec, err := worker.NewReconciler(worker.ReconcilerDeps{
		Config: cfg, Logger: observability.NewLogger("error", io.Discard), Store: store, Queue: q,
	})
	require.NoError(t, err)
	store.put(domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Status: domain.StatusPending, UpdatedAt: time.Now().UTC().Add(-time.Minute), MaxAttempts: 5,
	})

	err = rec.RunCycle(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lease lost before step stale_pending")
	assert.Equal(t, 1, q.renewals)
	queued, lErr := mr.List(queue.Key(domain.ChannelSMS, domain.PriorityNormal))
	if lErr == nil {
		assert.Empty(t, queued, "no repair may run without the lease")
	}
}

func TestExtendReconcilerLockOnlyForOwner(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)
	ctx := t.Context()

	ok, err := q.AcquireReconcilerLock(ctx, "owner-a", time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = q.ExtendReconcilerLock(ctx, "owner-b", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok, "another replica cannot extend the lease")

	ok, err = q.ExtendReconcilerLock(ctx, "owner-a", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Greater(t, mr.TTL(queue.ReconcilerLockKey), 30*time.Second)

	mr.FastForward(2 * time.Minute)
	ok, err = q.ExtendReconcilerLock(ctx, "owner-a", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok, "an expired lease cannot be revived")
}
