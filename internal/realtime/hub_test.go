package realtime_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

func TestHubFilteringSlowClientDropMaxClientsAndNoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseGoroutines := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(t.Context())
	hub := realtime.NewHub(realtime.HubConfig{
		MaxClients: 3,
		BufferSize: 2,
	})
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- hub.Run(ctx)
	}()

	notifA := uuid.Must(uuid.NewV7())
	notifB := uuid.Must(uuid.NewV7())
	batchA := uuid.Must(uuid.NewV7())
	batchB := uuid.Must(uuid.NewV7())
	chSMS := domain.ChannelSMS
	chEmail := domain.ChannelEmail

	// 1. Subscribe by notificationId, by batchId, and by AND-combined (batchId + channel).
	cNotif, unsubNotif, err := hub.Subscribe(ctx, realtime.Filter{NotificationID: &notifA})
	require.NoError(t, err)
	cBatch, unsubBatch, err := hub.Subscribe(ctx, realtime.Filter{BatchID: &batchA})
	require.NoError(t, err)
	cBatchSMS, unsubBatchSMS, err := hub.Subscribe(ctx, realtime.Filter{BatchID: &batchA, Channel: &chSMS})
	require.NoError(t, err)

	assert.Equal(t, 3, hub.ClientCount(ctx))

	// 2. Exceeding MaxClients returns ErrTooManyClients.
	_, _, errTooMany := hub.Subscribe(ctx, realtime.Filter{Channel: &chEmail})
	require.ErrorIs(t, errTooMany, realtime.ErrTooManyClients)

	// 3. Broadcast events and verify AND-combined filtering.
	ev1 := realtime.Event{
		NotificationID: notifA,
		BatchID:        &batchA,
		Channel:        domain.ChannelSMS,
		Status:         domain.StatusProcessing,
		Attempt:        1,
		OccurredAt:     time.Now().UTC(),
	}
	ev2 := realtime.Event{
		NotificationID: notifB,
		BatchID:        &batchA,
		Channel:        domain.ChannelEmail,
		Status:         domain.StatusSent,
		Attempt:        1,
		OccurredAt:     time.Now().UTC(),
	}
	evOtherBatch := realtime.Event{
		NotificationID: uuid.Must(uuid.NewV7()),
		BatchID:        &batchB,
		Channel:        domain.ChannelSMS,
		Status:         domain.StatusSent,
		Attempt:        1,
		OccurredAt:     time.Now().UTC(),
	}

	require.NoError(t, hub.Broadcast(ctx, ev1))
	require.NoError(t, hub.Broadcast(ctx, ev2))
	require.NoError(t, hub.Broadcast(ctx, evOtherBatch))

	// cNotif receives only ev1.
	select {
	case got := <-cNotif.Events():
		assert.Equal(t, notifA, got.NotificationID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cNotif event")
	}
	select {
	case unexpected := <-cNotif.Events():
		t.Fatalf("unexpected event for cNotif: %+v", unexpected)
	default:
	}

	// cBatch receives ev1 and ev2 (both in batchA).
	gotB1 := <-cBatch.Events()
	gotB2 := <-cBatch.Events()
	assert.Equal(t, notifA, gotB1.NotificationID)
	assert.Equal(t, notifB, gotB2.NotificationID)

	// cBatchSMS receives only ev1 (batchA AND channel=sms).
	gotBS := <-cBatchSMS.Events()
	assert.Equal(t, notifA, gotBS.NotificationID)
	select {
	case unexpected := <-cBatchSMS.Events():
		t.Fatalf("unexpected non-SMS event for cBatchSMS: %+v", unexpected)
	default:
	}

	// 4. Slow client backpressure: fill cNotif buffer (capacity=2) and send a 3rd event matching only notifA
	// (batchId=nil so cBatch and cBatchSMS are not targeted) -> cNotif dropped without blocking hub.
	evSlowOnlyNotifA := realtime.Event{
		NotificationID: notifA,
		Channel:        domain.ChannelSMS,
		Status:         domain.StatusProcessing,
		Attempt:        2,
		OccurredAt:     time.Now().UTC(),
	}
	require.NoError(t, hub.Broadcast(ctx, evSlowOnlyNotifA))
	require.NoError(t, hub.Broadcast(ctx, evSlowOnlyNotifA))
	require.NoError(t, hub.Broadcast(ctx, evSlowOnlyNotifA))

	require.Eventually(t, func() bool {
		select {
		case <-cNotif.Dropped():
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, 2, hub.ClientCount(ctx))

	// 5. Unsubscribe remaining clients -> ClientCount drops to 0.
	unsubNotif()
	unsubBatch()
	unsubBatchSMS()
	require.Eventually(t, func() bool {
		return hub.ClientCount(ctx) == 0
	}, time.Second, 10*time.Millisecond)

	// 6. Stop hub and verify no goroutine leak.
	cancel()
	require.NoError(t, <-doneCh)
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseGoroutines+2
	}, time.Second, 15*time.Millisecond)
}

func TestRedisPublisherAndHubEndToEndPubSub(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 10)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	hub := realtime.NewHub(realtime.HubConfig{
		Redis:      rdb,
		MaxClients: 10,
	})
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- hub.Run(ctx)
	}()

	notifID := uuid.Must(uuid.NewV7())
	client, unsub, err := hub.Subscribe(ctx, realtime.Filter{NotificationID: &notifID})
	require.NoError(t, err)
	defer unsub()

	pub := realtime.NewRedisPublisher(rdb)
	// Publish until the subscriber receives the event (handles initial subscribe handshake).
	require.Eventually(t, func() bool {
		_ = pub.Publish(ctx, realtime.Event{
			NotificationID: notifID,
			Channel:        domain.ChannelSMS,
			Status:         domain.StatusSent,
			Attempt:        1,
			OccurredAt:     time.Now().UTC(),
			CorrelationID:  "corr-pubsub-1",
		})
		select {
		case ev := <-client.Events():
			return ev.NotificationID == notifID && ev.Status == domain.StatusSent && ev.CorrelationID == "corr-pubsub-1"
		default:
			return false
		}
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-doneCh)
}
