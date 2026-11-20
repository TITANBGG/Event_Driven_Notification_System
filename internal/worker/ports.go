package worker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
)

// ErrCircuitOpen signals that a channel's circuit breaker rejected a call without invoking the provider.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Store defines the PostgreSQL operations consumed by the worker loop.
type Store interface {
	Claim(ctx context.Context, id uuid.UUID, skew time.Duration) (*domain.Notification, bool, error)
	MarkSent(ctx context.Context, id uuid.UUID, claimedAttempts int, providerMessageID *string) (*domain.Notification, error)
	ScheduleRetry(ctx context.Context, id uuid.UUID, claimedAttempts int, nextAttemptAt time.Time, lastError string) (*domain.Notification, error)
	DeferCircuitOpen(ctx context.Context, id uuid.UUID, claimedAttempts int, nextAttemptAt time.Time, lastError string) (*domain.Notification, error)
	MarkFailed(ctx context.Context, id uuid.UUID, claimedAttempts int, lastError string) (*domain.Notification, error)
	InsertAttempt(ctx context.Context, a domain.DeliveryAttempt) (*domain.DeliveryAttempt, error)
}

// Queue defines the Redis queue operations consumed by worker consumers, heartbeat, and delayed mover.
type Queue interface {
	Dequeue(ctx context.Context, ch domain.Channel, workerID string) (*queue.Meta, bool, error)
	Ack(ctx context.Context, workerID string, id uuid.UUID) error
	Schedule(ctx context.Context, meta queue.Meta, dueAt time.Time, workerID string) error
	DeadLetter(ctx context.Context, workerID string, id uuid.UUID) error
	RequeueInflight(ctx context.Context, workerID string) (int64, error)
	InflightIDs(ctx context.Context, workerID string) ([]uuid.UUID, error)
	RequeueInflightIDs(ctx context.Context, workerID string, ids []uuid.UUID) (int64, error)
	MoveDue(ctx context.Context, now time.Time, limit int) (int64, error)
	Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error
	DeleteHeartbeat(ctx context.Context, workerID string) error
}

// RateLimiter throttles provider calls per channel across worker replicas.
type RateLimiter interface {
	Wait(ctx context.Context, ch domain.Channel) error
}

// Sender dispatches a claimed notification to the external delivery provider.
type Sender interface {
	Send(ctx context.Context, n domain.Notification) (provider.Result, error)
}

// RetryPolicy computes backoff durations for transient failures (implemented in Phase 5).
type RetryPolicy interface {
	NextDelay(attempt int, retryAfter *time.Duration) time.Duration
}

// NoopRetryPolicy satisfies RetryPolicy during Phase 4 before exponential backoff is enabled.
type NoopRetryPolicy struct{}

// NextDelay returns zero delay in Phase 4.
func (NoopRetryPolicy) NextDelay(_ int, _ *time.Duration) time.Duration {
	return 0
}

// Breaker shields the worker pool from unhealthy provider endpoints (implemented with gobreaker in Phase 5).
type Breaker interface {
	Execute(ch domain.Channel, fn func() (provider.Result, error)) (provider.Result, error)
}

// NoopBreaker passes every send straight through until Phase 5 attaches sony/gobreaker.
type NoopBreaker struct{}

// Execute invokes fn directly without circuit breaking.
func (NoopBreaker) Execute(_ domain.Channel, fn func() (provider.Result, error)) (provider.Result, error) {
	return fn()
}

// Reconciler repairs divergences between PostgreSQL and Redis and expires stale leases/keys.
type Reconciler interface {
	RunCycle(ctx context.Context) error
}

// Hooks exposes optional instrumentation callbacks wired to Prometheus in Phase 6.
type Hooks struct {
	OnPanic              func()
	OnLeaseLost          func()
	OnProcessed          func(ch domain.Channel, status domain.Status)
	OnRetryScheduled     func(ch domain.Channel)
	OnBreakerStateChange func(ch domain.Channel, from, to string)
	OnReconcilerAction   func(step string, count int)
	OnInflightSwept      func(count int)
}
