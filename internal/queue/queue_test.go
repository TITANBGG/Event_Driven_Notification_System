package queue_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
)

func newTestQueue(t *testing.T) (*miniredis.Miniredis, *redis.Client, *queue.Queue) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 10)
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	return mr, rdb, queue.New(rdb)
}

func TestQueueEnqueueIdempotentDequeueAckAndDeadletter(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	id1 := uuid.Must(uuid.NewV7())
	id2 := uuid.Must(uuid.NewV7())
	m1 := queue.Meta{ID: id1, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, CorrelationID: "c-1"}
	m2 := queue.Meta{ID: id2, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, CorrelationID: "c-2"}

	// Enqueueing m1 twice must leave only 1 entry in the list while returning no error.
	require.NoError(t, q.Enqueue(ctx, m1))
	require.NoError(t, q.Enqueue(ctx, m1, m2))

	listKey := queue.Key(domain.ChannelSMS, domain.PriorityHigh)
	items, err := mr.List(listKey)
	require.NoError(t, err)
	assert.Len(t, items, 2)

	tracked, err := q.IsTracked(ctx, []uuid.UUID{id1, id2, uuid.Must(uuid.NewV7())})
	require.NoError(t, err)
	assert.True(t, tracked[id1])
	assert.True(t, tracked[id2])
	assert.Len(t, tracked, 3)

	// Dequeue pops FIFO (m1 first) and moves to worker-1 inflight list.
	got1, ok, err := q.Dequeue(ctx, domain.ChannelSMS, "worker-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, id1, got1.ID)
	assert.Equal(t, "c-1", got1.CorrelationID)

	inflight, err := mr.List(queue.InflightKey("worker-1"))
	require.NoError(t, err)
	assert.Equal(t, []string{id1.String()}, inflight)

	depths, err := q.Depths(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depths.ByChannel[domain.ChannelSMS][domain.PriorityHigh])
	assert.Equal(t, int64(1), depths.Inflight)

	// Ack removes from inflight and deletes {notif}:msg tracking entry.
	require.NoError(t, q.Ack(ctx, "worker-1", id1))
	assert.False(t, mr.Exists(queue.InflightKey("worker-1")))
	trackedAfterAck, err := q.IsTracked(ctx, []uuid.UUID{id1})
	require.NoError(t, err)
	assert.False(t, trackedAfterAck[id1])

	// Dequeue m2 then deadletter it.
	got2, ok, err := q.Dequeue(ctx, domain.ChannelSMS, "worker-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, id2, got2.ID)
	require.NoError(t, q.DeadLetter(ctx, "worker-1", id2))

	dlq, err := mr.List(queue.DLQListKey)
	require.NoError(t, err)
	assert.Equal(t, []string{id2.String()}, dlq)
	depthsAfterDLQ, err := q.Depths(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depthsAfterDLQ.DLQ)
	assert.Equal(t, int64(0), depthsAfterDLQ.Inflight)
}

func TestQueueRequeueInflightPreservesFIFOOrder(t *testing.T) {
	t.Parallel()
	_, _, q := newTestQueue(t)
	ctx := t.Context()

	ids := []uuid.UUID{uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())}
	var metas []queue.Meta
	for _, id := range ids {
		metas = append(metas, queue.Meta{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityHigh})
	}
	require.NoError(t, q.Enqueue(ctx, metas...))

	// Worker-1 dequeues first two items in FIFO order (ids[0], ids[1]), leaving ids[2] queued.
	d0, ok, err := q.Dequeue(ctx, domain.ChannelEmail, "worker-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, ids[0], d0.ID)

	d1, ok, err := q.Dequeue(ctx, domain.ChannelEmail, "worker-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, ids[1], d1.ID)

	requeued, err := q.RequeueInflight(ctx, "worker-1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), requeued)

	// Requeued items must jump back ahead of ids[2] in their original order: ids[0], ids[1], ids[2].
	for i, want := range ids {
		got, gotOK, dqErr := q.Dequeue(ctx, domain.ChannelEmail, "worker-2")
		require.NoError(t, dqErr)
		require.True(t, gotOK, "expected item %d", i)
		assert.Equal(t, want, got.ID)
	}
}

func TestQueueScheduleAndMoveDueSkipsCancelledDeletedMeta(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	dueID := uuid.Must(uuid.NewV7())
	cancelledDueID := uuid.Must(uuid.NewV7())
	futureID := uuid.Must(uuid.NewV7())

	require.NoError(t, q.Schedule(ctx, queue.Meta{ID: dueID, Channel: domain.ChannelPush, Priority: domain.PriorityNormal}, now.Add(-time.Second), ""))
	require.NoError(t, q.Schedule(ctx, queue.Meta{ID: cancelledDueID, Channel: domain.ChannelPush, Priority: domain.PriorityNormal}, now.Add(-time.Second), ""))
	require.NoError(t, q.Schedule(ctx, queue.Meta{ID: futureID, Channel: domain.ChannelPush, Priority: domain.PriorityNormal}, now.Add(30*time.Second), ""))

	// Simulate cancellation cleanup that removed tracking meta from {notif}:msg while ID remained in ZSET.
	mr.HDel(queue.MsgHashKey, cancelledDueID.String())

	moved, err := q.MoveDue(ctx, now, 500)
	require.NoError(t, err)
	assert.Equal(t, int64(1), moved, "only the due non-cancelled entry should be moved")

	got, ok, err := q.Dequeue(ctx, domain.ChannelPush, "worker-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, dueID, got.ID)

	_, okEmpty, err := q.Dequeue(ctx, domain.ChannelPush, "worker-1")
	require.NoError(t, err)
	assert.False(t, okEmpty, "future item must remain in delayed ZSET until due")

	depths, err := q.Depths(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depths.Delayed)
}

func TestWeightedDequeueDistributionAndNoStarvation(t *testing.T) {
	t.Parallel()

	t.Run("first 10 dequeues from 100 each follow exact 6:3:1 ratio", func(t *testing.T) {
		t.Parallel()
		_, _, q := newTestQueue(t)
		ctx := t.Context()

		var batch []queue.Meta
		for i := 0; i < 100; i++ {
			batch = append(batch,
				queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
				queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal},
				queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityLow},
			)
		}
		require.NoError(t, q.Enqueue(ctx, batch...))

		counts := map[domain.Priority]int{}
		for i := 0; i < 10; i++ {
			m, ok, err := q.Dequeue(ctx, domain.ChannelSMS, "worker-1")
			require.NoError(t, err)
			require.True(t, ok)
			counts[m.Priority]++
		}
		assert.Equal(t, 6, counts[domain.PriorityHigh])
		assert.Equal(t, 3, counts[domain.PriorityNormal])
		assert.Equal(t, 1, counts[domain.PriorityLow])
	})

	t.Run("only low items present uses 100 percent capacity with zero starvation", func(t *testing.T) {
		t.Parallel()
		_, _, q := newTestQueue(t)
		ctx := t.Context()

		for i := 0; i < 20; i++ {
			require.NoError(t, q.Enqueue(ctx, queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityLow}))
		}
		for i := 0; i < 20; i++ {
			m, ok, err := q.Dequeue(ctx, domain.ChannelSMS, "worker-1")
			require.NoError(t, err)
			require.True(t, ok, "item %d should fall through immediately", i)
			assert.Equal(t, domain.PriorityLow, m.Priority)
		}
	})

	t.Run("high and low contention guarantees at least 10 percent low throughput", func(t *testing.T) {
		t.Parallel()
		_, _, q := newTestQueue(t)
		ctx := t.Context()

		for i := 0; i < 200; i++ {
			require.NoError(t, q.Enqueue(ctx,
				queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
				queue.Meta{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityLow},
			))
		}

		counts := map[domain.Priority]int{}
		for i := 0; i < 100; i++ {
			m, ok, err := q.Dequeue(ctx, domain.ChannelSMS, fmt.Sprintf("worker-%d", i%2))
			require.NoError(t, err)
			require.True(t, ok)
			counts[m.Priority]++
		}
		assert.Equal(t, 90, counts[domain.PriorityHigh])
		assert.GreaterOrEqual(t, counts[domain.PriorityLow], 10)
	})
}

func TestHeartbeatLifecycle(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	require.NoError(t, q.Heartbeat(ctx, "worker-1", 15*time.Second))
	assert.True(t, mr.Exists(queue.HeartbeatKey("worker-1")))

	require.NoError(t, q.DeleteHeartbeat(ctx, "worker-1"))
	assert.False(t, mr.Exists(queue.HeartbeatKey("worker-1")))
}

func TestReconcilerLockForceEnqueueAndOrphanHelpers(t *testing.T) {
	t.Parallel()
	mr, _, q := newTestQueue(t)
	ctx := t.Context()

	// Lock acquisition must reject competing owner and owner-checked release must preserve live owner.
	acquired, err := q.AcquireReconcilerLock(ctx, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)

	contended, err := q.AcquireReconcilerLock(ctx, "worker-2", 30*time.Second)
	require.NoError(t, err)
	assert.False(t, contended)

	wrongRelease, err := q.ReleaseReconcilerLock(ctx, "worker-2")
	require.NoError(t, err)
	assert.False(t, wrongRelease)
	assert.True(t, mr.Exists(queue.ReconcilerLockKey))

	rightRelease, err := q.ReleaseReconcilerLock(ctx, "worker-1")
	require.NoError(t, err)
	assert.True(t, rightRelease)
	assert.False(t, mr.Exists(queue.ReconcilerLockKey))

	reacquired, err := q.AcquireReconcilerLock(ctx, "worker-2", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, reacquired)

	// ForceEnqueue pushes onto queue even when {notif}:msg still holds stale metadata.
	id := uuid.Must(uuid.NewV7())
	meta := queue.Meta{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityHigh, CorrelationID: "corr-force"}
	mr.HSet(queue.MsgHashKey, id.String(), `{"id":"stale"}`)
	require.NoError(t, q.ForceEnqueue(ctx, meta))

	items, err := mr.List(queue.Key(domain.ChannelEmail, domain.PriorityHigh))
	require.NoError(t, err)
	assert.Equal(t, []string{id.String()}, items)
	assert.Contains(t, mr.HGet(queue.MsgHashKey, id.String()), "corr-force")

	// Schedule with empty workerID schedules directly into {notif}:delayed without inflight cleanup errors.
	schedID := uuid.Must(uuid.NewV7())
	require.NoError(t, q.Schedule(ctx, queue.Meta{ID: schedID, Channel: domain.ChannelPush, Priority: domain.PriorityNormal}, time.Now().Add(time.Second), ""))
	members, err := mr.ZMembers(queue.DelayedZSetKey)
	require.NoError(t, err)
	assert.Contains(t, members, schedID.String())

	// Inflight scanning + heartbeat existence + inflight key deletion for orphan recovery.
	_, err = mr.Lpush(queue.InflightKey("worker-alive"), uuid.Must(uuid.NewV7()).String())
	require.NoError(t, err)
	_, err = mr.Lpush(queue.InflightKey("worker-dead"), uuid.Must(uuid.NewV7()).String())
	require.NoError(t, err)
	require.NoError(t, q.Heartbeat(ctx, "worker-alive", 15*time.Second))

	workerIDs, err := q.ScanInflightWorkerIDs(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"worker-alive", "worker-dead"}, workerIDs)

	hbs, err := q.HasHeartbeats(ctx, workerIDs)
	require.NoError(t, err)
	assert.True(t, hbs["worker-alive"])
	assert.False(t, hbs["worker-dead"])

	require.NoError(t, q.DeleteInflightKey(ctx, "worker-dead"))
	assert.False(t, mr.Exists(queue.InflightKey("worker-dead")))
	assert.True(t, mr.Exists(queue.InflightKey("worker-alive")))
}
