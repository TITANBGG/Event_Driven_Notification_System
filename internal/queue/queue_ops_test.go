package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
)

func TestQueueScheduleBatchPlacesItemsAtDueTimesAndMoveDuePromotes(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	now := time.Now().UTC()
	soon := uuid.Must(uuid.NewV7())
	later := uuid.Must(uuid.NewV7())
	metas := []queue.Meta{
		{ID: soon, Channel: domain.ChannelPush, Priority: domain.PriorityHigh},
		// Unknown priority must be normalized rather than producing an unreachable queue key.
		{ID: later, Channel: domain.ChannelPush, Priority: domain.Priority("bogus")},
	}
	require.NoError(t, q.ScheduleBatch(ctx, metas, []time.Time{now.Add(time.Second), now.Add(time.Hour)}))

	score, err := mr.ZScore(queue.DelayedZSetKey, soon.String())
	require.NoError(t, err)
	assert.InDelta(t, float64(now.Add(time.Second).UnixMilli()), score, 1)
	assert.True(t, mr.Exists(queue.MsgHashKey))
	tracked, err := q.IsTracked(ctx, []uuid.UUID{soon, later})
	require.NoError(t, err)
	assert.True(t, tracked[soon])
	assert.True(t, tracked[later])

	moved, err := q.MoveDue(ctx, now.Add(2*time.Second), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), moved, "only the item due within the window is promoted")
	got, err := mr.List(queue.Key(domain.ChannelPush, domain.PriorityHigh))
	require.NoError(t, err)
	assert.Equal(t, []string{soon.String()}, got)

	moved, err = q.MoveDue(ctx, now.Add(2*time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), moved)
	got, err = mr.List(queue.Key(domain.ChannelPush, domain.PriorityNormal))
	require.NoError(t, err)
	assert.Equal(t, []string{later.String()}, got)
}

func TestQueueScheduleBatchRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	_, _, q := newTestQueue(t)
	ctx := t.Context()
	id := uuid.Must(uuid.NewV7())

	require.NoError(t, q.ScheduleBatch(ctx, nil, nil), "empty batch is a no-op")
	require.ErrorContains(t, q.ScheduleBatch(ctx, []queue.Meta{{ID: id, Channel: domain.ChannelSMS}}, nil), "length mismatch")
	require.ErrorContains(t,
		q.ScheduleBatch(ctx, []queue.Meta{{ID: id, Channel: domain.Channel("fax")}}, []time.Time{time.Now()}),
		"invalid channel")
	require.ErrorContains(t, q.Schedule(ctx, queue.Meta{ID: id, Channel: domain.Channel("fax")}, time.Now(), ""), "invalid channel")
	require.ErrorContains(t, q.Enqueue(ctx, queue.Meta{ID: id, Channel: domain.Channel("fax")}), "invalid channel")
	_, _, err := q.Dequeue(ctx, domain.Channel("fax"), "w1")
	require.ErrorContains(t, err, "invalid channel")
}

func TestQueueCancelDelayedUntracksScheduledItem(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	keep := uuid.Must(uuid.NewV7())
	cancelled := uuid.Must(uuid.NewV7())
	due := time.Now().UTC().Add(time.Minute)
	require.NoError(t, q.ScheduleBatch(ctx,
		[]queue.Meta{{ID: keep, Channel: domain.ChannelEmail}, {ID: cancelled, Channel: domain.ChannelEmail}},
		[]time.Time{due, due}))

	require.NoError(t, q.CancelDelayed(ctx))
	require.NoError(t, q.CancelDelayed(ctx, cancelled))

	members, err := mr.ZMembers(queue.DelayedZSetKey)
	require.NoError(t, err)
	assert.Equal(t, []string{keep.String()}, members)
	tracked, err := q.IsTracked(ctx, []uuid.UUID{keep, cancelled})
	require.NoError(t, err)
	assert.True(t, tracked[keep])
	assert.False(t, tracked[cancelled], "cancelled item must be untracked so the reconciler never resurrects it")
}

func TestQueueRemoveFromDLQDropsOnlyReplayedIDs(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	replayed := uuid.Must(uuid.NewV7())
	stillFailed := uuid.Must(uuid.NewV7())
	for _, id := range []uuid.UUID{replayed, stillFailed, replayed} {
		require.NoError(t, q.DeadLetter(ctx, "", id))
	}

	require.NoError(t, q.RemoveFromDLQ(ctx))
	require.NoError(t, q.RemoveFromDLQ(ctx, replayed))

	got, err := mr.List(queue.DLQListKey)
	require.NoError(t, err)
	assert.Equal(t, []string{stillFailed.String()}, got, "every occurrence of the replayed ID is removed")
}

func TestQueueInflightIDsSkipsMalformedEntries(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	id := uuid.Must(uuid.NewV7())
	_, err := mr.Lpush(queue.InflightKey("w1"), id.String())
	require.NoError(t, err)
	_, err = mr.Lpush(queue.InflightKey("w1"), "not-a-uuid")
	require.NoError(t, err)

	ids, err := q.InflightIDs(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{id}, ids)

	n, err := q.RequeueInflightIDs(ctx, "w1", nil)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// Every operation must surface Redis failures as wrapped errors (never panic, never silently succeed),
// because the worker and API decide fail-closed / reconciler-repair behaviour from these errors.
func TestQueueOperationsReturnErrorsWhenRedisUnavailable(t *testing.T) {
	t.Parallel()
	id := uuid.Must(uuid.NewV7())
	meta := queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}

	ops := map[string]func(ctx context.Context, q *queue.Queue) error{
		"Ping":            func(ctx context.Context, q *queue.Queue) error { return q.Ping(ctx) },
		"Enqueue":         func(ctx context.Context, q *queue.Queue) error { return q.Enqueue(ctx, meta) },
		"ForceEnqueue":    func(ctx context.Context, q *queue.Queue) error { return q.ForceEnqueue(ctx, meta) },
		"Ack":             func(ctx context.Context, q *queue.Queue) error { return q.Ack(ctx, "w1", id) },
		"Schedule":        func(ctx context.Context, q *queue.Queue) error { return q.Schedule(ctx, meta, time.Now(), "w1") },
		"DeadLetter":      func(ctx context.Context, q *queue.Queue) error { return q.DeadLetter(ctx, "w1", id) },
		"Heartbeat":       func(ctx context.Context, q *queue.Queue) error { return q.Heartbeat(ctx, "w1", time.Second) },
		"DeleteHeartbeat": func(ctx context.Context, q *queue.Queue) error { return q.DeleteHeartbeat(ctx, "w1") },
		"DeleteInflight":  func(ctx context.Context, q *queue.Queue) error { return q.DeleteInflightKey(ctx, "w1") },
		"CancelDelayed":   func(ctx context.Context, q *queue.Queue) error { return q.CancelDelayed(ctx, id) },
		"RemoveFromDLQ":   func(ctx context.Context, q *queue.Queue) error { return q.RemoveFromDLQ(ctx, id) },
		"ScheduleBatch": func(ctx context.Context, q *queue.Queue) error {
			return q.ScheduleBatch(ctx, []queue.Meta{meta}, []time.Time{time.Now()})
		},
		"Dequeue": func(ctx context.Context, q *queue.Queue) error {
			_, _, err := q.Dequeue(ctx, domain.ChannelSMS, "w1")
			return err
		},
		"RequeueInflight": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.RequeueInflight(ctx, "w1")
			return err
		},
		"InflightIDs": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.InflightIDs(ctx, "w1")
			return err
		},
		"RequeueInflightIDs": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.RequeueInflightIDs(ctx, "w1", []uuid.UUID{id})
			return err
		},
		"MoveDue": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.MoveDue(ctx, time.Now(), 10)
			return err
		},
		"IsTracked": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.IsTracked(ctx, []uuid.UUID{id})
			return err
		},
		"AcquireLock": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.AcquireReconcilerLock(ctx, "owner", time.Second)
			return err
		},
		"ReleaseLock": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.ReleaseReconcilerLock(ctx, "owner")
			return err
		},
		"ScanInflight": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.ScanInflightWorkerIDs(ctx)
			return err
		},
		"HasHeartbeats": func(ctx context.Context, q *queue.Queue) error {
			_, err := q.HasHeartbeats(ctx, []string{"w1"})
			return err
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			assert.Error(t, op(ctx, queue.New(nil)), "nil client")

			mr, _, q := newTestQueue(t)
			mr.Close()
			assert.Error(t, op(ctx, q), "unreachable redis")
		})
	}
}
