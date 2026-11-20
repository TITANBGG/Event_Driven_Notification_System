package worker_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"sync"
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

func TestWorkerEmitsObservabilityHooksAndMaskedLogs(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)

	var logBuf bytes.Buffer
	baseLogger := observability.NewLogger("info", &logBuf).With(slog.String("worker_id", "worker-obs-1"))

	now := time.Date(2026, 11, 21, 10, 0, 5, 0, time.UTC)
	created := now.Add(-1500 * time.Millisecond)
	batchID := uuid.Must(uuid.NewV7())
	id := uuid.Must(uuid.NewV7())

	store := newMemWorkerStore()
	store.put(domain.Notification{
		ID:          id,
		BatchID:     &batchID,
		Channel:     domain.ChannelSMS,
		Priority:    domain.PriorityHigh,
		Status:      domain.StatusPending,
		Recipient:   "+905551234567",
		Content:     "secret-otp-9911",
		MaxAttempts: 5,
		CreatedAt:   created,
	})

	msgID := "ext-obs-1"
	code202 := http.StatusAccepted
	sender := &fakeSender{fn: func(_ context.Context, _ domain.Notification) (provider.Result, error) {
		return provider.Result{
			Outcome:           provider.OutcomeSent,
			HTTPStatus:        &code202,
			ProviderMessageID: &msgID,
			Duration:          42 * time.Millisecond,
		}, nil
	}}

	var (
		mu             sync.Mutex
		providerCalled bool
		e2eLatency     time.Duration
		processedSt    domain.Status
	)
	cfg := testWorkerConfig()
	cfg.WorkerID = "worker-obs-1"

	w, err := worker.New(worker.Deps{
		Config:      cfg,
		Logger:      baseLogger,
		Store:       store,
		Queue:       q,
		RateLimiter: noopLimiter{},
		Sender:      sender,
		Now:         func() time.Time { return now },
		Hooks: worker.Hooks{
			OnProviderRequest: func(ch domain.Channel, httpStatus *int, sendErr error, dur time.Duration) {
				mu.Lock()
				defer mu.Unlock()
				providerCalled = true
				assert.Equal(t, domain.ChannelSMS, ch)
				require.NotNil(t, httpStatus)
				assert.Equal(t, 202, *httpStatus)
				assert.NoError(t, sendErr)
				assert.Equal(t, 42*time.Millisecond, dur)
			},
			OnEndToEndLatency: func(ch domain.Channel, latency time.Duration) {
				mu.Lock()
				defer mu.Unlock()
				assert.Equal(t, domain.ChannelSMS, ch)
				e2eLatency = latency
			},
			OnProcessed: func(_ domain.Channel, st domain.Status) {
				mu.Lock()
				defer mu.Unlock()
				processedSt = st
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, q.Enqueue(t.Context(), queue.Meta{
		ID:            id,
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityHigh,
		CorrelationID: "cid-worker-obs-1",
	}))

	runWorkerUntil(t, w, func() bool {
		return store.get(id).Status == domain.StatusSent
	})

	mu.Lock()
	assert.True(t, providerCalled)
	assert.Equal(t, 1500*time.Millisecond, e2eLatency)
	assert.Equal(t, domain.StatusSent, processedSt)
	mu.Unlock()

	logs := logBuf.String()
	assert.Contains(t, logs, `"correlation_id":"cid-worker-obs-1"`)
	assert.Contains(t, logs, `"notification_id":"`+id.String()+`"`)
	assert.Contains(t, logs, `"batch_id":"`+batchID.String()+`"`)
	assert.Contains(t, logs, `"worker_id":"worker-obs-1"`)
	assert.Contains(t, logs, `"outcome":"sent"`)
	assert.Contains(t, logs, `"duration_ms":42`)
	assert.Contains(t, logs, `"recipient":"+90555***4567"`)
	assert.NotContains(t, logs, "+905551234567")
	assert.NotContains(t, logs, "secret-otp-9911")
}
