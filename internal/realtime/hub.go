package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
)

const (
	// DefaultMaxClients is the default cap on concurrent WebSocket connections per API replica.
	DefaultMaxClients = 1000

	// DefaultQueueSoftLimit is the queue length above which a client that has not written a frame for
	// DefaultSlowClientAfter is dropped as slow. A fixed-size buffer cannot tell a burst from a stalled
	// reader: one 1,000-item batch publishes 1,000 "pending" events at once, and a 64- or even
	// 1024-slot buffer dropped clients that were reading as fast as the server could write.
	DefaultQueueSoftLimit = 1024
	// DefaultQueueHardLimit drops a client regardless of progress. Queues grow on demand, so idle
	// clients hold no memory; this bounds a progressing client to ~2 MB of queued frames.
	DefaultQueueHardLimit = 8192
	// DefaultSlowClientAfter is how long a client above the soft limit may go without delivering a frame.
	DefaultSlowClientAfter = 5 * time.Second
)

// ErrTooManyClients is returned by Hub.Subscribe when MAX_WS_CLIENTS is reached.
var ErrTooManyClients = errors.New("maximum websocket clients reached")

// Event is the canonical JSON message published to Redis pub/sub channel `notif:events`
// after every notification state transition and forwarded to matching WebSocket clients.
type Event struct {
	Type           string         `json:"type,omitempty"`
	NotificationID uuid.UUID      `json:"notificationId"`
	BatchID        *uuid.UUID     `json:"batchId"`
	Channel        domain.Channel `json:"channel"`
	Status         domain.Status  `json:"status"`
	Attempt        int            `json:"attempt"`
	Error          *string        `json:"error"`
	OccurredAt     time.Time      `json:"occurredAt"`
	CorrelationID  string         `json:"correlationId,omitempty"`
}

// NewEvent constructs a status transition Event from a domain.Notification after a DB update succeeds.
func NewEvent(n domain.Notification, occurredAt time.Time, correlationID string) Event {
	cid := correlationID
	if cid == "" && n.CorrelationID != nil {
		cid = *n.CorrelationID
	}
	var errPtr *string
	if n.LastError != nil && *n.LastError != "" {
		cp := *n.LastError
		errPtr = &cp
	}
	return Event{
		Type:           "event",
		NotificationID: n.ID,
		BatchID:        n.BatchID,
		Channel:        n.Channel,
		Status:         n.Status,
		Attempt:        n.Attempts,
		Error:          errPtr,
		OccurredAt:     occurredAt.UTC(),
		CorrelationID:  cid,
	}
}

// Filter specifies the AND-combined subscription criteria for a WebSocket client.
// At least one filter field must be non-nil.
type Filter struct {
	NotificationID *uuid.UUID
	BatchID        *uuid.UUID
	Channel        *domain.Channel
}

// Valid reports whether at least one filter criterion is set.
func (f Filter) Valid() bool {
	return f.NotificationID != nil || f.BatchID != nil || f.Channel != nil
}

// Matches reports whether ev satisfies all non-nil fields in f (AND-combined).
func (f Filter) Matches(ev Event) bool {
	if !f.Valid() {
		return false
	}
	if f.NotificationID != nil && ev.NotificationID != *f.NotificationID {
		return false
	}
	if f.BatchID != nil {
		if ev.BatchID == nil || *ev.BatchID != *f.BatchID {
			return false
		}
	}
	if f.Channel != nil && ev.Channel != *f.Channel {
		return false
	}
	return true
}

// Publisher broadcasts notification status events (best-effort).
type Publisher interface {
	Publish(ctx context.Context, events ...Event) error
}

// NoopPublisher discards events when real-time publishing is not configured.
type NoopPublisher struct{}

// Publish is a no-op returning nil.
func (NoopPublisher) Publish(_ context.Context, _ ...Event) error { return nil }

// RedisPublisher publishes JSON-encoded Event messages to Redis pub/sub `notif:events`.
type RedisPublisher struct {
	rdb redis.UniversalClient
}

// NewRedisPublisher constructs a RedisPublisher backed by rdb.
func NewRedisPublisher(rdb redis.UniversalClient) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

// Publish serializes and publishes one or more events to `notif:events` via a Redis pipeline.
func (p *RedisPublisher) Publish(ctx context.Context, events ...Event) error {
	if len(events) == 0 || p == nil || p.rdb == nil {
		return nil
	}
	pipe := p.rdb.Pipeline()
	for _, ev := range events {
		if ev.OccurredAt.IsZero() {
			ev.OccurredAt = time.Now().UTC()
		}
		raw, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("marshaling realtime event: %w", err)
		}
		pipe.Publish(ctx, queue.EventsPubSubChannel, raw)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("publishing realtime events: %w", err)
	}
	return nil
}

// Client represents a single subscribed WebSocket consumer managed by Hub. The Hub appends
// pre-encoded JSON frames to the client's queue; the connection's writer drains them.
type Client struct {
	filter Filter

	mu     sync.Mutex
	queue  [][]byte
	closed bool

	readyCh      chan struct{} // capacity 1: "frames are waiting"
	slowCh       chan struct{} // closed when the Hub drops the client for backpressure
	closedCh     chan struct{} // closed when the client is removed for any reason
	lastProgress atomic.Int64  // unix nanos of the last frame the writer delivered (or of subscribe)
}

// Ready signals that frames are waiting; call Drain to take them.
func (c *Client) Ready() <-chan struct{} { return c.readyCh }

// Drain takes every queued frame (oldest first). Each frame is one complete JSON event.
func (c *Client) Drain() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	frames := c.queue
	c.queue = nil
	return frames
}

// MarkProgress records that the writer delivered a frame. A client that keeps making progress is never
// dropped for a burst below the hard queue limit, however large the burst.
func (c *Client) MarkProgress() { c.lastProgress.Store(time.Now().UnixNano()) }

// Dropped returns a channel that is closed if the Hub drops this client for backpressure.
func (c *Client) Dropped() <-chan struct{} { return c.slowCh }

// Closed returns a channel that is closed when the client is removed (unsubscribe, drop or Hub stop).
func (c *Client) Closed() <-chan struct{} { return c.closedCh }

// QueueLen reports how many frames are waiting (tests and diagnostics).
func (c *Client) QueueLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

type enqueueResult int

const (
	enqueued enqueueResult = iota
	droppedSlow
	alreadyClosed
)

// enqueue is called only from the Hub loop and never blocks.
func (c *Client) enqueue(frame []byte, now time.Time, softLimit, hardLimit int, slowAfter time.Duration) enqueueResult {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return alreadyClosed
	}
	n := len(c.queue)
	stalled := now.Sub(time.Unix(0, c.lastProgress.Load())) > slowAfter
	if n >= hardLimit || (n >= softLimit && stalled) {
		c.mu.Unlock()
		return droppedSlow
	}
	c.queue = append(c.queue, frame)
	c.mu.Unlock()
	select {
	case c.readyCh <- struct{}{}:
	default: // a wake-up is already pending
	}
	return enqueued
}

// close marks the client removed; slow additionally signals Dropped. Called only from the Hub loop.
func (c *Client) close(slow bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.queue = nil
	c.mu.Unlock()
	if slow {
		close(c.slowCh)
	}
	close(c.closedCh)
}

type registerReq struct {
	filter Filter
	reply  chan registerResp
}

type registerResp struct {
	client *Client
	err    error
}

// HubConfig configures a Hub instance. Zero values select the Default* constants.
type HubConfig struct {
	Redis           redis.UniversalClient
	Logger          *slog.Logger
	MaxClients      int
	QueueSoftLimit  int
	QueueHardLimit  int
	SlowClientAfter time.Duration
}

// Hub subscribes ONCE per API process to Redis pub/sub `notif:events` and fans out matching
// events to connected WebSocket clients using non-blocking buffered channels.
type Hub struct {
	rdb        redis.UniversalClient
	logger     *slog.Logger
	maxClients int
	softLimit  int
	hardLimit  int
	slowAfter  time.Duration

	regCh       chan registerReq
	unregCh     chan *Client
	broadcastCh chan encodedEvent
	countCh     chan chan int
	doneCh      chan struct{}
}

// NewHub constructs a Hub ready to be started via Run(ctx).
func NewHub(cfg HubConfig) *Hub {
	logger := cfg.Logger
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}
	maxClients := cfg.MaxClients
	if maxClients <= 0 {
		maxClients = DefaultMaxClients
	}
	soft := cfg.QueueSoftLimit
	if soft <= 0 {
		soft = DefaultQueueSoftLimit
	}
	hard := cfg.QueueHardLimit
	if hard <= 0 {
		hard = DefaultQueueHardLimit
	}
	slowAfter := cfg.SlowClientAfter
	if slowAfter <= 0 {
		slowAfter = DefaultSlowClientAfter
	}
	return &Hub{
		rdb:         cfg.Redis,
		logger:      logger,
		maxClients:  maxClients,
		softLimit:   soft,
		hardLimit:   hard,
		slowAfter:   slowAfter,
		regCh:       make(chan registerReq),
		unregCh:     make(chan *Client),
		broadcastCh: make(chan encodedEvent, 2048),
		countCh:     make(chan chan int),
		doneCh:      make(chan struct{}),
	}
}

// Run executes the single-owner Hub event loop and optional Redis Pub/Sub subscriber until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) error {
	defer close(h.doneCh)

	var subWg sync.WaitGroup
	if h.rdb != nil {
		pubsub := h.rdb.Subscribe(ctx, queue.EventsPubSubChannel)
		// Wait briefly for subscription confirmation on initial startup (best-effort).
		_, _ = pubsub.ReceiveTimeout(ctx, 2*time.Second)
		msgCh := pubsub.Channel()

		subWg.Add(1)
		go func() {
			defer subWg.Done()
			defer func() { _ = pubsub.Close() }()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-msgCh:
					if !ok {
						return
					}
					var ev Event
					if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
						h.logger.Warn("ignoring malformed pub/sub event", slog.String("error", err.Error()))
						continue
					}
					// The payload already is the client frame; re-encode only to fill a missing type.
					frame := []byte(msg.Payload)
					if ev.Type == "" {
						var encErr error
						if frame, encErr = encodeEvent(&ev); encErr != nil {
							continue
						}
					}
					select {
					case h.broadcastCh <- encodedEvent{ev: ev, frame: frame}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	clients := make(map[*Client]struct{})
	removeClient := func(c *Client, slow bool) {
		if _, exists := clients[c]; exists {
			delete(clients, c)
			c.close(slow)
		}
	}

	for {
		select {
		case <-ctx.Done():
			for c := range clients {
				removeClient(c, false)
			}
			subWg.Wait()
			return nil

		case req := <-h.regCh:
			if len(clients) >= h.maxClients {
				req.reply <- registerResp{err: ErrTooManyClients}
				continue
			}
			c := &Client{
				filter:   req.filter,
				readyCh:  make(chan struct{}, 1),
				slowCh:   make(chan struct{}),
				closedCh: make(chan struct{}),
			}
			c.MarkProgress()
			clients[c] = struct{}{}
			req.reply <- registerResp{client: c}

		case c := <-h.unregCh:
			removeClient(c, false)

		case item := <-h.broadcastCh:
			now := time.Now()
			for c := range clients {
				if !c.filter.Matches(item.ev) {
					continue
				}
				// Every matching client shares the same encoded frame; the Hub never blocks on a client.
				if c.enqueue(item.frame, now, h.softLimit, h.hardLimit, h.slowAfter) == droppedSlow {
					removeClient(c, true)
				}
			}

		case replyCh := <-h.countCh:
			replyCh <- len(clients)
		}
	}
}

// Subscribe registers a new client with filter, returning an unsub function that is safe to call multiple times.
// Returns ErrTooManyClients when MAX_WS_CLIENTS is exceeded.
func (h *Hub) Subscribe(ctx context.Context, filter Filter) (*Client, func(), error) {
	reply := make(chan registerResp, 1)
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-h.doneCh:
		return nil, nil, errors.New("realtime hub is stopped")
	case h.regCh <- registerReq{filter: filter, reply: reply}:
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-h.doneCh:
		return nil, nil, errors.New("realtime hub is stopped")
	case resp := <-reply:
		if resp.err != nil {
			return nil, nil, resp.err
		}
		var once sync.Once
		unsub := func() {
			once.Do(func() {
				select {
				case h.unregCh <- resp.client:
				case <-h.doneCh:
				}
			})
		}
		return resp.client, unsub, nil
	}
}

// Broadcast injects an event directly into the Hub (used by unit tests and in-process fan-out).
func (h *Hub) Broadcast(ctx context.Context, ev Event) error {
	frame, err := encodeEvent(&ev)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.doneCh:
		return errors.New("realtime hub is stopped")
	case h.broadcastCh <- encodedEvent{ev: ev, frame: frame}:
		return nil
	}
}

// encodedEvent pairs an event (used for filtering) with its JSON frame (sent unchanged to every match).
type encodedEvent struct {
	ev    Event
	frame []byte
}

func encodeEvent(ev *Event) ([]byte, error) {
	if ev.Type == "" {
		ev.Type = "event"
	}
	frame, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("encoding realtime event: %w", err)
	}
	return frame, nil
}

// ClientCount returns the current number of active subscribed clients.
func (h *Hub) ClientCount(ctx context.Context) int {
	reply := make(chan int, 1)
	select {
	case <-ctx.Done():
		return 0
	case <-h.doneCh:
		return 0
	case h.countCh <- reply:
	}
	select {
	case <-ctx.Done():
		return 0
	case <-h.doneCh:
		return 0
	case n := <-reply:
		return n
	}
}
