package worker_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

type memWorkerStore struct {
	mu       sync.Mutex
	rows     map[uuid.UUID]domain.Notification
	attempts []domain.DeliveryAttempt
	claimErr error
	leaseLos bool
}

func newMemWorkerStore() *memWorkerStore {
	return &memWorkerStore{rows: make(map[uuid.UUID]domain.Notification)}
}

func (s *memWorkerStore) put(n domain.Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[n.ID] = n
}

func (s *memWorkerStore) get(id uuid.UUID) domain.Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id]
}

func (s *memWorkerStore) attemptList() []domain.DeliveryAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.DeliveryAttempt, len(s.attempts))
	copy(out, s.attempts)
	return out
}

func (s *memWorkerStore) Claim(_ context.Context, id uuid.UUID, _ time.Duration) (*domain.Notification, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return nil, false, s.claimErr
	}
	n, ok := s.rows[id]
	if !ok || n.Status != domain.StatusPending {
		return nil, false, nil
	}
	n.Status = domain.StatusProcessing
	n.Attempts++
	s.rows[id] = n
	cp := n
	return &cp, true, nil
}

func (s *memWorkerStore) MarkSent(_ context.Context, id uuid.UUID, claimedAttempts int, providerMsgID *string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseLos {
		return nil, domain.ErrLeaseLost
	}
	n, ok := s.rows[id]
	if !ok || n.Status != domain.StatusProcessing || n.Attempts != claimedAttempts {
		return nil, domain.ErrLeaseLost
	}
	n.Status = domain.StatusSent
	n.ProviderMessageID = providerMsgID
	now := time.Now().UTC()
	n.SentAt = &now
	s.rows[id] = n
	cp := n
	return &cp, nil
}

func (s *memWorkerStore) MarkFailed(_ context.Context, id uuid.UUID, claimedAttempts int, lastError string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseLos {
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
	s.rows[id] = n
	cp := n
	return &cp, nil
}

func (s *memWorkerStore) InsertAttempt(_ context.Context, a domain.DeliveryAttempt) (*domain.DeliveryAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, a)
	cp := a
	return &cp, nil
}

type noopLimiter struct{}

func (noopLimiter) Wait(_ context.Context, _ domain.Channel) error { return nil }

type fakeSender struct {
	fn    func(ctx context.Context, n domain.Notification) (provider.Result, error)
	calls atomic.Int32
}

func (f *fakeSender) Send(ctx context.Context, n domain.Notification) (provider.Result, error) {
	f.calls.Add(1)
	return f.fn(ctx, n)
}

func testWorkerConfig() config.Config {
	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.WorkerConcurrency = 1
	cfg.WorkerID = "worker-test-1"
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.HeartbeatTTL = 5 * time.Second
	cfg.DelayedMoverIntv = 30 * time.Millisecond
	cfg.ShutdownTimeout = 500 * time.Millisecond
	return cfg
}

func runWorkerUntil(t *testing.T, w *worker.Worker, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()

	require.Eventually(t, check, 3*time.Second, 15*time.Millisecond)
	cancel()
	require.NoError(t, <-errCh)
}

func TestWorkerHappyPathAndCancelledBeforeClaimAndPermanentFailure(t *testing.T) {
	t.Parallel()

	t.Run("happy path marks sent and acks", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 4)
		defer func() { _ = rdb.Close() }()
		q := queue.New(rdb)

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending, Recipient: "+905551234567", Content: "hi", MaxAttempts: 5})

		msgID := "prov-msg-1"
		status := 202
		sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeSent, HTTPStatus: &status, ProviderMessageID: &msgID, Duration: 10 * time.Millisecond}, nil
		}}

		var logBuf bytes.Buffer
		w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("info", &logBuf), Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender})
		require.NoError(t, err)

		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, CorrelationID: "corr-happy"}))
		runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusSent })

		assert.Equal(t, int32(1), sender.calls.Load())
		require.Len(t, store.attemptList(), 1)
		assert.Contains(t, logBuf.String(), "corr-happy")
		assert.False(t, mr.Exists(queue.HeartbeatKey("worker-test-1")), "heartbeat must be deleted on clean shutdown")
	})

	t.Run("cancelled before claim skips provider call and acks redis", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 4)
		defer func() { _ = rdb.Close() }()
		q := queue.New(rdb)

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Status: domain.StatusCancelled})

		sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeSent}, nil
		}}
		w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender})
		require.NoError(t, err)

		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}))
		runWorkerUntil(t, w, func() bool {
			tracked, _ := q.IsTracked(t.Context(), []uuid.UUID{id})
			return !tracked[id]
		})
		assert.Equal(t, int32(0), sender.calls.Load())
	})

	t.Run("permanent failure transitions failed and pushes to DLQ", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 4)
		defer func() { _ = rdb.Close() }()
		q := queue.New(rdb)

		store := newMemWorkerStore()
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{ID: id, Channel: domain.ChannelPush, Priority: domain.PriorityLow, Status: domain.StatusPending, MaxAttempts: 5})

		http400 := 400
		sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomePermanent, HTTPStatus: &http400, Err: errors.New("bad device token")}, nil
		}}
		w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sender})
		require.NoError(t, err)

		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelPush, Priority: domain.PriorityLow}))
		runWorkerUntil(t, w, func() bool { return store.get(id).Status == domain.StatusFailed })

		dlq, err := mr.List(queue.DLQListKey)
		require.NoError(t, err)
		assert.Equal(t, []string{id.String()}, dlq)
	})
}

func TestWorkerLeaseLostPanicRecoveryAndShutdownRequeue(t *testing.T) {
	t.Parallel()

	t.Run("fenced stale update triggers ErrLeaseLost hook and acks", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 4)
		defer func() { _ = rdb.Close() }()
		q := queue.New(rdb)

		store := newMemWorkerStore()
		store.leaseLos = true
		id := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Status: domain.StatusPending, MaxAttempts: 5})

		var leaseLostCalls atomic.Int32
		sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeSent}, nil
		}}
		w, err := worker.New(worker.Deps{
			Config:      testWorkerConfig(),
			Logger:      observability.NewLogger("warn", &bytes.Buffer{}),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      sender,
			Hooks:       worker.Hooks{OnLeaseLost: func() { leaseLostCalls.Add(1) }},
		})
		require.NoError(t, err)

		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}))
		runWorkerUntil(t, w, func() bool { return leaseLostCalls.Load() == 1 })
		tracked, err := q.IsTracked(t.Context(), []uuid.UUID{id})
		require.NoError(t, err)
		assert.False(t, tracked[id])
	})

	t.Run("panic inside provider fake is recovered and goroutine processes next item", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 4)
		defer func() { _ = rdb.Close() }()
		q := queue.New(rdb)

		store := newMemWorkerStore()
		idPanic := uuid.Must(uuid.NewV7())
		idNext := uuid.Must(uuid.NewV7())
		store.put(domain.Notification{ID: idPanic, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending, MaxAttempts: 5})
		store.put(domain.Notification{ID: idNext, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending, MaxAttempts: 5})

		var panics atomic.Int32
		sender := &fakeSender{fn: func(_ context.Context, n domain.Notification) (provider.Result, error) {
			if n.ID == idPanic {
				panic("simulated provider crash")
			}
			return provider.Result{Outcome: provider.OutcomeSent}, nil
		}}

		w, err := worker.New(worker.Deps{
			Config:      testWorkerConfig(),
			Logger:      observability.NewLogger("error", &bytes.Buffer{}),
			Store:       store,
			Queue:       q,
			RateLimiter: noopLimiter{},
			Sender:      sender,
			Hooks:       worker.Hooks{OnPanic: func() { panics.Add(1) }},
		})
		require.NoError(t, err)

		require.NoError(t, q.Enqueue(t.Context(),
			queue.Meta{ID: idPanic, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
			queue.Meta{ID: idNext, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
		))
		runWorkerUntil(t, w, func() bool { return panics.Load() == 1 && store.get(idNext).Status == domain.StatusSent })
	})

	t.Run("shutdown requeues remaining inflight items", func(t *testing.T) {
		t.Parallel()
		mr := miniredis.RunT(t)
		rdb := queue.NewClient(mr.Addr(), 4)
		defer func() { _ = rdb.Close() }()
		q := queue.New(rdb)

		// Pre-populate worker-test-1 inflight list directly so shutdown drains and requeues it.
		unstartedID := uuid.Must(uuid.NewV7())
		require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: unstartedID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
		_, ok, err := q.Dequeue(t.Context(), domain.ChannelSMS, "worker-test-1")
		require.NoError(t, err)
		require.True(t, ok)

		// Limiter blocks until ctx cancels so the inflight item stays unstarted during shutdown.
		blockingLimiter := blockUntilCancelLimiter{}
		w, err := worker.New(worker.Deps{
			Config:      testWorkerConfig(),
			Logger:      observability.NewLogger("error", &bytes.Buffer{}),
			Store:       newMemWorkerStore(),
			Queue:       q,
			RateLimiter: blockingLimiter,
			Sender:      &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) { return provider.Result{}, nil }},
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() { errCh <- w.Run(ctx) }()
		require.Eventually(t, func() bool { return mr.Exists(queue.HeartbeatKey("worker-test-1")) }, time.Second, 10*time.Millisecond)
		cancel()
		require.NoError(t, <-errCh)

		list, err := mr.List(queue.Key(domain.ChannelSMS, domain.PriorityHigh))
		require.NoError(t, err)
		assert.Equal(t, []string{unstartedID.String()}, list)
	})
}

type blockUntilCancelLimiter struct{}

func (blockUntilCancelLimiter) Wait(ctx context.Context, _ domain.Channel) error {
	<-ctx.Done()
	return ctx.Err()
}
