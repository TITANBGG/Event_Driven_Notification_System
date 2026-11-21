package api

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
)

// QueueInspector exposes Redis health and live queue depth snapshots for API probes and metrics.
type QueueInspector interface {
	Ping(ctx context.Context) error
	Depths(ctx context.Context) (queue.MetricsDepths, error)
}

// QueueItem carries the minimal scheduling metadata pushed to Redis after Postgres commit.
// When ScheduledAt is non-nil, the item is routed to {notif}:delayed instead of a ready list.
type QueueItem struct {
	ID            uuid.UUID
	Channel       domain.Channel
	Priority      domain.Priority
	CorrelationID string
	TraceParent   string
	// TraceLink: the worker links its trace to TraceParent instead of joining it (batch items).
	TraceLink   bool
	ScheduledAt *time.Time
}

// Enqueuer abstracts asynchronous queue dispatch plus best-effort Redis cleanup after
// PostgreSQL transitions (cancellation and manual DLQ replay).
type Enqueuer interface {
	Enqueue(ctx context.Context, items []QueueItem) error
	CancelDelayed(ctx context.Context, ids ...uuid.UUID) error
	ReleaseFromDLQ(ctx context.Context, ids ...uuid.UUID) error
}

// MemoryEnqueuer is a thread-safe in-memory Enqueuer for unit tests and pre-Redis Phase 3 startup.
type MemoryEnqueuer struct {
	mu           sync.Mutex
	items        []QueueItem
	cancelledIDs []uuid.UUID
	releasedIDs  []uuid.UUID
	err          error
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

// CancelDelayed records best-effort delayed cancellation IDs in memory.
func (m *MemoryEnqueuer) CancelDelayed(ctx context.Context, ids ...uuid.UUID) error {
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
	m.cancelledIDs = append(m.cancelledIDs, ids...)
	return nil
}

// ReleaseFromDLQ records IDs removed from the DLQ list after a manual retry.
func (m *MemoryEnqueuer) ReleaseFromDLQ(ctx context.Context, ids ...uuid.UUID) error {
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
	m.releasedIDs = append(m.releasedIDs, ids...)
	return nil
}

// ReleasedIDs returns a snapshot copy of all IDs passed to ReleaseFromDLQ.
func (m *MemoryEnqueuer) ReleasedIDs() []uuid.UUID {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]uuid.UUID, len(m.releasedIDs))
	copy(out, m.releasedIDs)
	return out
}

// CancelledIDs returns a snapshot copy of all IDs passed to CancelDelayed.
func (m *MemoryEnqueuer) CancelledIDs() []uuid.UUID {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]uuid.UUID, len(m.cancelledIDs))
	copy(out, m.cancelledIDs)
	return out
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
	m.cancelledIDs = nil
	m.releasedIDs = nil
	m.err = nil
}

// SetError configures an error returned by subsequent Enqueue calls (nil clears it).
func (m *MemoryEnqueuer) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.err = err
}
