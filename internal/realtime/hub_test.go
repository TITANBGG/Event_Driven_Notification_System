package realtime_test

import (
	"context"
	"encoding/json"
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
		MaxClients:     3,
		QueueSoftLimit: 2,
		QueueHardLimit: 2,
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

	// Each client receives exactly the matching events, in order (the broadcast of the 3rd event proves
	// the first two have been fanned out, since the Hub handles broadcasts one at a time).
	require.Eventually(t, func() bool { return hub.ClientCount(ctx) == 3 }, time.Second, 10*time.Millisecond)
	gotNotif := drainEvents(t, cNotif, 1)
	assert.Equal(t, notifA, gotNotif[0].NotificationID)
	gotBatch := drainEvents(t, cBatch, 2)
	assert.Equal(t, notifA, gotBatch[0].NotificationID)
	assert.Equal(t, notifB, gotBatch[1].NotificationID)
	gotBatchSMS := drainEvents(t, cBatchSMS, 1)
	assert.Equal(t, notifA, gotBatchSMS[0].NotificationID, "batchA AND channel=sms excludes the email event")

	// 4. Backpressure: with a hard queue limit of 2, a 3rd undelivered event for cNotif drops it without
	// blocking the Hub (batchId=nil, so cBatch and cBatchSMS are not targeted).
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
		for _, frame := range client.Drain() {
			var ev realtime.Event
			if json.Unmarshal(frame, &ev) == nil && ev.NotificationID == notifID &&
				ev.Status == domain.StatusSent && ev.CorrelationID == "corr-pubsub-1" && ev.Type == "event" {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-doneCh)
}

// drainEvents waits for exactly want frames on c, decodes them, and fails on more.
func drainEvents(t *testing.T, c *realtime.Client, want int) []realtime.Event {
	t.Helper()
	var got []realtime.Event
	deadline := time.After(time.Second)
	for len(got) < want {
		select {
		case <-c.Ready():
			for _, frame := range c.Drain() {
				var ev realtime.Event
				require.NoError(t, json.Unmarshal(frame, &ev))
				got = append(got, ev)
			}
		case <-deadline:
			t.Fatalf("got %d of %d events", len(got), want)
		}
	}
	require.Len(t, got, want, "no extra events")
	return got
}

func startHub(t *testing.T, cfg realtime.HubConfig) (*realtime.Hub, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	hub := realtime.NewHub(cfg)
	done := make(chan error, 1)
	go func() { done <- hub.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return hub, ctx
}

func burst(ctx context.Context, t *testing.T, hub *realtime.Hub, ch domain.Channel, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, hub.Broadcast(ctx, realtime.Event{NotificationID: uuid.Must(uuid.NewV7()), Channel: ch, Status: domain.StatusPending}))
	}
}

// A burst far above the soft limit must not drop a client that is still making progress: one 1,000-item
// batch publishes 1,000 events at once, and fixed 64/1024-slot buffers dropped readers that kept up.
func TestHubBurstAboveSoftLimitKeepsAProgressingClient(t *testing.T) {
	t.Parallel()
	hub, ctx := startHub(t, realtime.HubConfig{QueueSoftLimit: 4, QueueHardLimit: 1000, SlowClientAfter: time.Hour})
	ch := domain.ChannelSMS
	c, unsub, err := hub.Subscribe(ctx, realtime.Filter{Channel: &ch})
	require.NoError(t, err)
	defer unsub()

	burst(ctx, t, hub, ch, 500)
	require.Equal(t, 1, hub.ClientCount(ctx))
	assert.Len(t, drainEvents(t, c, 500), 500)
	select {
	case <-c.Dropped():
		t.Fatal("a progressing client must not be dropped for a burst")
	default:
	}
}

// Above the soft limit, a client that has not delivered a frame for SlowClientAfter is dropped.
func TestHubDropsAStalledClientAboveSoftLimit(t *testing.T) {
	t.Parallel()
	hub, ctx := startHub(t, realtime.HubConfig{QueueSoftLimit: 4, QueueHardLimit: 1000, SlowClientAfter: 50 * time.Millisecond})
	ch := domain.ChannelSMS
	c, unsub, err := hub.Subscribe(ctx, realtime.Filter{Channel: &ch})
	require.NoError(t, err)
	defer unsub()

	burst(ctx, t, hub, ch, 4) // fills the queue to the soft limit while the client is still "fresh"
	require.Equal(t, 1, hub.ClientCount(ctx))
	time.Sleep(100 * time.Millisecond) // no MarkProgress: the client stalls
	burst(ctx, t, hub, ch, 1)
	require.Eventually(t, func() bool { return hub.ClientCount(ctx) == 0 }, time.Second, 10*time.Millisecond)
	select {
	case <-c.Dropped():
	default:
		t.Fatal("stalled client must be reported as Dropped (1008)")
	}
	select {
	case <-c.Closed():
	default:
		t.Fatal("dropped client must also be Closed")
	}
}

// The hard limit bounds memory even for a client that looks healthy.
func TestHubHardLimitDropsRegardlessOfProgress(t *testing.T) {
	t.Parallel()
	hub, ctx := startHub(t, realtime.HubConfig{QueueSoftLimit: 4, QueueHardLimit: 10, SlowClientAfter: time.Hour})
	ch := domain.ChannelSMS
	c, unsub, err := hub.Subscribe(ctx, realtime.Filter{Channel: &ch})
	require.NoError(t, err)
	defer unsub()

	burst(ctx, t, hub, ch, 11)
	require.Eventually(t, func() bool { return hub.ClientCount(ctx) == 0 }, time.Second, 10*time.Millisecond)
	select {
	case <-c.Dropped():
	default:
		t.Fatal("client above the hard limit must be dropped")
	}
	assert.Zero(t, c.QueueLen(), "a dropped client releases its queue")
}
