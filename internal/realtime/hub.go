package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
)

const (
	// ClientSendBufferSize is the per-client outbound event channel capacity.
	// Clients whose buffer fills up are disconnected with WebSocket close code 1008 (Policy Violation)
	// so a slow consumer never blocks the Hub or other subscribers.
	ClientSendBufferSize = 64

	// DefaultMaxClients is the default cap on concurrent WebSocket connections per API replica.
	DefaultMaxClients = 1000
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

// Client represents a single subscribed WebSocket consumer managed by Hub.
type Client struct {
	filter   Filter
	sendCh   chan Event
	slowCh   chan struct{}
	slowOnce sync.Once
}

// Events returns the read-only channel of filtered events for this client.
func (c *Client) Events() <-chan Event {
	return c.sendCh
}

// Dropped returns a channel that is closed if the Hub drops this client due to a full send buffer (backpressure).
func (c *Client) Dropped() <-chan struct{} {
	return c.slowCh
}

func (c *Client) markSlow() {
	c.slowOnce.Do(func() {
		close(c.slowCh)
	})
}

type registerReq struct {
	filter Filter
	reply  chan registerResp
}

type registerResp struct {
	client *Client
	err    error
}

// HubConfig configures a Hub instance.
type HubConfig struct {
	Redis      redis.UniversalClient
	Logger     *slog.Logger
	MaxClients int
	BufferSize int
}

// Hub subscribes ONCE per API process to Redis pub/sub `notif:events` and fans out matching
// events to connected WebSocket clients using non-blocking buffered channels.
type Hub struct {
	rdb        redis.UniversalClient
	logger     *slog.Logger
	maxClients int
	bufferSize int

	regCh       chan registerReq
	unregCh     chan *Client
	broadcastCh chan Event
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
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = ClientSendBufferSize
	}
	return &Hub{
		rdb:         cfg.Redis,
		logger:      logger,
		maxClients:  maxClients,
		bufferSize:  bufSize,
		regCh:       make(chan registerReq),
		unregCh:     make(chan *Client),
		broadcastCh: make(chan Event, 256),
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
					if ev.Type == "" {
						ev.Type = "event"
					}
					select {
					case h.broadcastCh <- ev:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	clients := make(map[*Client]struct{})
	removeClient := func(c *Client) {
		if _, exists := clients[c]; exists {
			delete(clients, c)
			close(c.sendCh)
		}
	}

	for {
		select {
		case <-ctx.Done():
			for c := range clients {
				removeClient(c)
			}
			subWg.Wait()
			return nil

		case req := <-h.regCh:
			if len(clients) >= h.maxClients {
				req.reply <- registerResp{err: ErrTooManyClients}
				continue
			}
			c := &Client{
				filter: req.filter,
				sendCh: make(chan Event, h.bufferSize),
				slowCh: make(chan struct{}),
			}
			clients[c] = struct{}{}
			req.reply <- registerResp{client: c}

		case c := <-h.unregCh:
			removeClient(c)

		case ev := <-h.broadcastCh:
			for c := range clients {
				if !c.filter.Matches(ev) {
					continue
				}
				select {
				case c.sendCh <- ev:
				default:
					// Slow client backpressure: drop and disconnect with 1008 instead of blocking the hub.
					c.markSlow()
					removeClient(c)
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
	if ev.Type == "" {
		ev.Type = "event"
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.doneCh:
		return errors.New("realtime hub is stopped")
	case h.broadcastCh <- ev:
		return nil
	}
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
