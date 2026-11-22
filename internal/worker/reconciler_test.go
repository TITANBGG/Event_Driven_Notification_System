package worker_test

import (
	"context"
	"io"
	"slices"
	"sync"
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

type memReconcilerStore struct {
	mu          sync.Mutex
	rows        map[uuid.UUID]domain.Notification
	attempts    []domain.DeliveryAttempt
	expiredKeys int64
	deleteCalls []int
	leaseLostID map[uuid.UUID]bool
	findPending []uuid.UUID
}

func newMemReconcilerStore() *memReconcilerStore {
	return &memReconcilerStore{
		rows:        make(map[uuid.UUID]domain.Notification),
		leaseLostID: make(map[uuid.UUID]bool),
	}
}

func (s *memReconcilerStore) put(n domain.Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[n.ID] = n
}

func (s *memReconcilerStore) get(id uuid.UUID) domain.Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id]
}

func (s *memReconcilerStore) attemptList() []domain.DeliveryAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]domain.DeliveryAttempt, len(s.attempts))
	copy(cp, s.attempts)
	return cp
}

func (s *memReconcilerStore) sortedFiltered(afterID *uuid.UUID, limit int, pred func(domain.Notification) bool) []domain.Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]uuid.UUID, 0, len(s.rows))
	for id := range s.rows {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int {
		return stringCompare(a.String(), b.String())
	})
	var out []domain.Notification
	for _, id := range ids {
		if afterID != nil && stringCompare(id.String(), afterID.String()) <= 0 {
			continue
		}
		n := s.rows[id]
		if pred(n) {
			out = append(out, n)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

func (s *memReconcilerStore) FindStalePending(_ context.Context, staleAfter time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error) {
	cutoff := time.Now().UTC().Add(-staleAfter)
	rows := s.sortedFiltered(afterID, limit, func(n domain.Notification) bool {
		return n.Status == domain.StatusPending && n.UpdatedAt.Before(cutoff)
	})
	s.mu.Lock()
	for _, r := range rows {
		s.findPending = append(s.findPending, r.ID)
	}
	s.mu.Unlock()
	return rows, nil
}

func (s *memReconcilerStore) FindDelayedDueSoon(_ context.Context, staleAfter, lookahead time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error) {
	staleCutoff := time.Now().UTC().Add(-staleAfter)
	dueHorizon := time.Now().UTC().Add(lookahead)
	return s.sortedFiltered(afterID, limit, func(n domain.Notification) bool {
		if n.Status != domain.StatusRetrying && n.Status != domain.StatusScheduled {
			return false
		}
		if !n.UpdatedAt.Before(staleCutoff) {
			return false
		}
		due := n.NextAttemptAt
		if due == nil {
			due = n.ScheduledAt
		}
		return due != nil && !due.After(dueHorizon)
	}), nil
}

func (s *memReconcilerStore) FindAbandonedProcessing(_ context.Context, visibilityTimeout time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error) {
	cutoff := time.Now().UTC().Add(-visibilityTimeout)
	return s.sortedFiltered(afterID, limit, func(n domain.Notification) bool {
		return n.Status == domain.StatusProcessing && n.ProcessingStartedAt != nil && n.ProcessingStartedAt.Before(cutoff)
	}), nil
}

func (s *memReconcilerStore) RequeueAbandoned(_ context.Context, id uuid.UUID, claimedAttempts int) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseLostID[id] {
		return nil, domain.ErrLeaseLost
	}
	n, ok := s.rows[id]
	if !ok || n.Status != domain.StatusProcessing || n.Attempts != claimedAttempts {
		return nil, domain.ErrLeaseLost
	}
	n.Status = domain.StatusPending
	n.ProcessingStartedAt = nil
	n.UpdatedAt = time.Now().UTC()
	s.rows[id] = n
	cp := n
	return &cp, nil
}

func (s *memReconcilerStore) FailAbandoned(_ context.Context, id uuid.UUID, claimedAttempts int, lastError string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseLostID[id] {
		return nil, domain.ErrLeaseLost
	}
	n, ok := s.rows[id]
	if !ok || n.Status != domain.StatusProcessing || n.Attempts != claimedAttempts {
		return nil, domain.ErrLeaseLost
	}
	n.Status = domain.StatusFailed
	n.LastError = &lastError
	now := time.Now().UTC()
	n.FailedAt = &now
	n.UpdatedAt = now
	s.rows[id] = n
	cp := n
	return &cp, nil
}

func (s *memReconcilerStore) InsertAttempt(_ context.Context, a domain.DeliveryAttempt) (*domain.DeliveryAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, a)
	cp := a
	return &cp, nil
}

func (s *memReconcilerStore) DeleteExpiredIdempotencyKeys(_ context.Context, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCalls = append(s.deleteCalls, limit)
	n := min(int64(limit), s.expiredKeys)
	s.expiredKeys -= n
	return n, nil
}

func ptrString(s string) *string {
	return &s
}

func stringCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func TestDistributedReconcilerAllRecoveryStepsAndLock(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 10)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)
	ctx := t.Context()

	store := newMemReconcilerStore()
	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.WorkerID = "worker-rec-1"
	cfg.PendingStaleAfter = 30 * time.Second
	cfg.VisibilityTimeout = 60 * time.Second

	actions := map[string]int{}
	rec, err := worker.NewReconciler(worker.ReconcilerDeps{
		Config:     cfg,
		Logger:     observability.NewLogger("error", io.Discard),
		Store:      store,
		Queue:      q,
		BatchLimit: 2, // force multi-page keyset scan in stale pending step
		OnAction: func(step string, count int) {
			actions[step] += count
		},
	})
	require.NoError(t, err)

	now := time.Now().UTC()
	staleUpdated := now.Add(-2 * time.Minute)
	expiredProc := now.Add(-3 * time.Minute)
	dueSoon := now.Add(2 * time.Minute)

	// (a) Stale pending: 2 untracked (recovered via keyset pages) + 1 already tracked (skipped).
	staleUntracked1 := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
		Status: domain.StatusPending, CorrelationID: ptrString("c-stale-1"), UpdatedAt: staleUpdated, MaxAttempts: 5,
	}
	staleUntracked2 := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelEmail, Priority: domain.PriorityNormal,
		Status: domain.StatusPending, CorrelationID: ptrString("c-stale-2"), UpdatedAt: staleUpdated, MaxAttempts: 5,
	}
	staleTracked := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelPush, Priority: domain.PriorityLow,
		Status: domain.StatusPending, CorrelationID: ptrString("c-stale-tracked"), UpdatedAt: staleUpdated, MaxAttempts: 5,
	}
	store.put(staleUntracked1)
	store.put(staleUntracked2)
	store.put(staleTracked)
	require.NoError(t, q.Enqueue(ctx, queue.Meta{
		ID: staleTracked.ID, Channel: staleTracked.Channel, Priority: staleTracked.Priority, CorrelationID: *staleTracked.CorrelationID,
	}))

	// (b) Delayed due soon (< 5m): 1 retrying missing from Redis + 1 scheduled missing from Redis.
	missingRetry := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Status: domain.StatusRetrying, NextAttemptAt: &dueSoon, UpdatedAt: staleUpdated, CorrelationID: ptrString("c-ret"), MaxAttempts: 5,
	}
	missingSched := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelEmail, Priority: domain.PriorityHigh,
		Status: domain.StatusScheduled, ScheduledAt: &dueSoon, UpdatedAt: staleUpdated, CorrelationID: ptrString("c-sch"), MaxAttempts: 5,
	}
	store.put(missingRetry)
	store.put(missingSched)

	// (c) Abandoned processing: 1 requeued (attempts 2 < 5), 1 failed (attempts 5 >= 5), 1 race-lost skipped.
	abandonedRequeue := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
		Status: domain.StatusProcessing, Attempts: 2, MaxAttempts: 5, ProcessingStartedAt: &expiredProc, CorrelationID: ptrString("c-ab-req"),
	}
	abandonedFail := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelPush, Priority: domain.PriorityNormal,
		Status: domain.StatusProcessing, Attempts: 5, MaxAttempts: 5, ProcessingStartedAt: &expiredProc, CorrelationID: ptrString("c-ab-fail"),
	}
	abandonedRaced := domain.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelEmail, Priority: domain.PriorityNormal,
		Status: domain.StatusProcessing, Attempts: 1, MaxAttempts: 5, ProcessingStartedAt: &expiredProc, CorrelationID: ptrString("c-ab-raced"),
	}
	store.put(abandonedRequeue)
	store.put(abandonedFail)
	store.put(abandonedRaced)
	store.mu.Lock()
	store.leaseLostID[abandonedRaced.ID] = true
	store.mu.Unlock()

	// (d) Orphan inflight: dead worker missing heartbeat gets requeued and deleted; alive worker untouched.
	deadInflightID := uuid.Must(uuid.NewV7())
	liveInflightID := uuid.Must(uuid.NewV7())
	mr.HSet(queue.MsgHashKey, deadInflightID.String(), `{"id":"`+deadInflightID.String()+`","channel":"email","priority":"low"}`)
	mr.HSet(queue.MsgHashKey, liveInflightID.String(), `{"id":"`+liveInflightID.String()+`","channel":"sms","priority":"high"}`)
	_, err = mr.Lpush(queue.InflightKey("worker-dead"), deadInflightID.String())
	require.NoError(t, err)
	_, err = mr.Lpush(queue.InflightKey("worker-live"), liveInflightID.String())
	require.NoError(t, err)
	require.NoError(t, q.Heartbeat(ctx, "worker-live", 15*time.Second))

	// (e) Expired idempotency keys across 2 batches (10000 + 3500 = 13500).
	store.mu.Lock()
	store.expiredKeys = 13500
	store.mu.Unlock()

	// First verify contention lock skips cycle completely when another worker holds {notif}:reconciler:lock.
	held, err := q.AcquireReconcilerLock(ctx, "other-worker", 30*time.Second)
	require.NoError(t, err)
	require.True(t, held)
	require.NoError(t, rec.RunCycle(ctx))
	assert.Empty(t, actions)

	// Release competing lock and run full cycle.
	_, err = q.ReleaseReconcilerLock(ctx, "other-worker")
	require.NoError(t, err)

	require.NoError(t, rec.RunCycle(ctx))
	assert.False(t, mr.Exists(queue.ReconcilerLockKey), "lock must be released after cycle")

	// Verify (a) stale pending untracked items pushed to Redis queues.
	smsHigh, err := mr.List(queue.Key(domain.ChannelSMS, domain.PriorityHigh))
	require.NoError(t, err)
	assert.Contains(t, smsHigh, staleUntracked1.ID.String())
	assert.Contains(t, smsHigh, abandonedRequeue.ID.String())

	emailNormal, err := mr.List(queue.Key(domain.ChannelEmail, domain.PriorityNormal))
	require.NoError(t, err)
	assert.Contains(t, emailNormal, staleUntracked2.ID.String())

	// Verify (b) delayed items scheduled in {notif}:delayed.
	delayedMembers, err := mr.ZMembers(queue.DelayedZSetKey)
	require.NoError(t, err)
	assert.Contains(t, delayedMembers, missingRetry.ID.String())
	assert.Contains(t, delayedMembers, missingSched.ID.String())

	// Verify (c) abandoned rows transitioned + DLQ + abandoned audit rows.
	gotReq := store.get(abandonedRequeue.ID)
	assert.Equal(t, domain.StatusPending, gotReq.Status)
	assert.Nil(t, gotReq.ProcessingStartedAt)

	gotFail := store.get(abandonedFail.ID)
	assert.Equal(t, domain.StatusFailed, gotFail.Status)
	require.NotNil(t, gotFail.LastError)
	assert.Equal(t, "delivery outcome unknown: worker died during the final attempt (visibility timeout)", *gotFail.LastError)
	dlq, err := mr.List(queue.DLQListKey)
	require.NoError(t, err)
	assert.Equal(t, []string{abandonedFail.ID.String()}, dlq)

	attempts := store.attemptList()
	assert.Len(t, attempts, 2)
	for _, a := range attempts {
		assert.Equal(t, domain.OutcomeAbandoned, a.Outcome)
		assert.False(t, a.Counted)
	}

	// Verify (d) orphan inflight requeued & key deleted while alive worker kept inflight item.
	assert.False(t, mr.Exists(queue.InflightKey("worker-dead")))
	assert.True(t, mr.Exists(queue.InflightKey("worker-live")))
	emailLow, err := mr.List(queue.Key(domain.ChannelEmail, domain.PriorityLow))
	require.NoError(t, err)
	assert.Equal(t, []string{deadInflightID.String()}, emailLow)

	// Verify (e) batched idempotency deletion finished all 13500 rows across 2 batches.
	assert.Equal(t, 13500, actions["idempotency_cleanup"])
	store.mu.Lock()
	assert.Equal(t, int64(0), store.expiredKeys)
	assert.Equal(t, []int{10000, 10000}, store.deleteCalls)
	store.mu.Unlock()
}
