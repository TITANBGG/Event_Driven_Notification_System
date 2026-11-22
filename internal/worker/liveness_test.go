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
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

// A consumer stuck inside one item (here: blocked waiting for a rate-limit token) stops ticking, so
// Live reports it once the stall exceeds 2*VISIBILITY_TIMEOUT, while idle consumers keep ticking.
func TestLiveReportsAStalledConsumer(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)

	store := newMemWorkerStore()
	id := pendingSMS(store)
	entered := make(chan struct{}, 1)
	limiter := funcLimiter(func(ctx context.Context, _ domain.Channel) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})

	var nowNanos atomic.Int64
	nowNanos.Store(time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, nowNanos.Load()).UTC() }

	cfg := testWorkerConfig()
	w, err := worker.New(worker.Deps{
		Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store: store, Queue: q, RateLimiter: limiter, Sender: sentSender(), Now: now,
	})
	require.NoError(t, err)
	assert.NoError(t, w.Live(now()), "not started yet: nothing to report")

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()

	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}))
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer never reached the limiter")
	}
	assert.NoError(t, w.Live(now()), "a fresh in-flight item is not a stall")

	// Two visibility timeouts later the SMS consumer still has not finished its item.
	err = w.Live(now().Add(2*cfg.VisibilityTimeout + time.Second))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sms")

	cancel()
	require.NoError(t, <-errCh)
	assert.NoError(t, w.Live(now().Add(time.Hour)), "a stopped worker reports no stalled consumer")
}
