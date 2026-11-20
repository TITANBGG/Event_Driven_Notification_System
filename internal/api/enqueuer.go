package api

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
)

// QueueItem carries the minimal scheduling metadata pushed to Redis after Postgres commit.
type QueueItem struct {
	ID            uuid.UUID
	Channel       domain.Channel
	Priority      domain.Priority
	CorrelationID string
	TraceParent   string
}

// Enqueuer abstracts asynchronous queue dispatch so the API package remains decoupled from Redis until Phase 4.
type Enqueuer interface {
	Enqueue(ctx context.Context, items []QueueItem) error
}

// MemoryEnqueuer is a thread-safe in-memory Enqueuer for unit tests and pre-Redis Phase 3 startup.
type MemoryEnqueuer struct {
	mu    sync.Mutex
	items []QueueItem
	err   error
}

// NewMemoryEnqueuer constructs an empty MemoryEnqueuer.
func NewMemoryEnqueuer() *MemoryEnqueuer {
	return &MemoryEnqueuer{}
}

// Enqueue appends items in order unless a simulated error or cancelled context prevents dispatch.
func (m *MemoryEnqueuer) Enqueue(ctx context.Context, items []QueueItem) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return m.err
	}
	if len(items) == 0 {
		return nil
	}
	m.items = append(m.items, items...)
	return nil
}

// Items returns a snapshot copy of all enqueued items in submission order.
func (m *MemoryEnqueuer) Items() []QueueItem {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]QueueItem, len(m.items))
	copy(out, m.items)
	return out
}

// Reset clears recorded items and any configured error.
func (m *MemoryEnqueuer) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items = nil
	m.err = nil
}

// SetError configures an error returned by subsequent Enqueue calls (nil clears it).
func (m *MemoryEnqueuer) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.err = err
}
