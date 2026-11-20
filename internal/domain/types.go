package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Channel represents a supported notification delivery medium.
type Channel string

// Supported notification delivery channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// ParseChannel validates and converts a raw string into a typed Channel.
//
// Expected output example:
//
//	ParseChannel("SMS") -> ChannelSMS ("sms"), nil
//	ParseChannel("fax") -> "", error("unsupported channel \"fax\"")
func ParseChannel(raw string) (Channel, error) {
	c := Channel(strings.ToLower(strings.TrimSpace(raw)))
	if !c.Valid() {
		return "", fmt.Errorf("unsupported channel %q", raw)
	}
	return c, nil
}

// Valid reports whether c is one of sms, email, or push.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	default:
		return false
	}
}

// String returns the canonical lowercase channel name.
func (c Channel) String() string {
	return string(c)
}

// Priority represents the dispatch scheduling tier for a notification.
type Priority string

// Supported dispatch priorities (weighted 6:3:1 by queue consumers).
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// ParsePriority validates a non-empty priority string.
func ParsePriority(raw string) (Priority, error) {
	p := Priority(strings.ToLower(strings.TrimSpace(raw)))
	if !p.Valid() {
		return "", fmt.Errorf("unsupported priority %q", raw)
	}
	return p, nil
}

// ParsePriorityOrDefault returns PriorityNormal when raw is blank, or validates raw otherwise.
//
// Expected output example:
//
//	ParsePriorityOrDefault("") -> PriorityNormal ("normal"), nil
func ParsePriorityOrDefault(raw string) (Priority, error) {
	if strings.TrimSpace(raw) == "" {
		return PriorityNormal, nil
	}
	return ParsePriority(raw)
}

// Valid reports whether p is high, normal, or low.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}

// String returns the canonical lowercase priority name.
func (p Priority) String() string {
	return string(p)
}

// Status represents the authoritative lifecycle state of a notification in PostgreSQL.
type Status string

// Notification lifecycle statuses matching PostgreSQL CHECK constraints.
const (
	StatusScheduled  Status = "scheduled"
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// AllStatuses returns every defined Status in canonical lifecycle order.
func AllStatuses() []Status {
	return []Status{
		StatusScheduled,
		StatusPending,
		StatusProcessing,
		StatusRetrying,
		StatusSent,
		StatusFailed,
		StatusCancelled,
	}
}

// ParseStatus validates and converts a raw string into a typed Status.
func ParseStatus(raw string) (Status, error) {
	s := Status(strings.ToLower(strings.TrimSpace(raw)))
	if !s.Valid() {
		return "", fmt.Errorf("unsupported status %q", raw)
	}
	return s, nil
}

// Valid reports whether s is a defined notification Status.
func (s Status) Valid() bool {
	switch s {
	case StatusScheduled, StatusPending, StatusProcessing, StatusRetrying, StatusSent, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// String returns the canonical lowercase status name.
func (s Status) String() string {
	return string(s)
}

// AttemptOutcome represents the recorded result of a single delivery attempt or deferral.
type AttemptOutcome string

// Supported delivery_attempts outcome values matching PostgreSQL CHECK constraints.
const (
	OutcomeSent        AttemptOutcome = "sent"
	OutcomeTransient   AttemptOutcome = "transient"
	OutcomePermanent   AttemptOutcome = "permanent"
	OutcomeCircuitOpen AttemptOutcome = "circuit_open"
	OutcomeAbandoned   AttemptOutcome = "abandoned"
)

// Valid reports whether o is a supported delivery attempt outcome.
func (o AttemptOutcome) Valid() bool {
	switch o {
	case OutcomeSent, OutcomeTransient, OutcomePermanent, OutcomeCircuitOpen, OutcomeAbandoned:
		return true
	default:
		return false
	}
}

// String returns the canonical lowercase outcome name.
func (o AttemptOutcome) String() string {
	return string(o)
}

// ResourceType identifies the entity backed by an idempotency key record.
type ResourceType string

// Supported idempotency resource types.
const (
	ResourceNotification ResourceType = "notification"
	ResourceBatch        ResourceType = "batch"
)

// Notification is the core domain aggregate persisted in the notifications table.
type Notification struct {
	ID                  uuid.UUID  `json:"id"`
	BatchID             *uuid.UUID `json:"batchId"`
	Channel             Channel    `json:"channel"`
	Priority            Priority   `json:"priority"`
	Status              Status     `json:"status"`
	Recipient           string     `json:"recipient"`
	Subject             *string    `json:"subject,omitempty"`
	Title               *string    `json:"title,omitempty"`
	Content             string     `json:"content"`
	IdempotencyKey      *string    `json:"idempotencyKey,omitempty"`
	Attempts            int        `json:"attempts"`
	MaxAttempts         int        `json:"maxAttempts"`
	Deferrals           int        `json:"deferrals"`
	ScheduledAt         *time.Time `json:"scheduledAt"`
	NextAttemptAt       *time.Time `json:"nextAttemptAt"`
	ProcessingStartedAt *time.Time `json:"processingStartedAt,omitempty"`
	LastError           *string    `json:"lastError"`
	ProviderMessageID   *string    `json:"providerMessageId"`
	CorrelationID       *string    `json:"correlationId,omitempty"`
	TraceParent         *string    `json:"traceparent,omitempty"`
	CreatedAt           time.Time  `json:"createdAt"`
	UpdatedAt           time.Time  `json:"updatedAt"`
	SentAt              *time.Time `json:"sentAt"`
	FailedAt            *time.Time `json:"failedAt,omitempty"`
	CancelledAt         *time.Time `json:"cancelledAt"`
}

// Batch represents a grouped submission of up to 1000 notifications.
type Batch struct {
	ID            uuid.UUID `json:"id"`
	Total         int       `json:"total"`
	CorrelationID *string   `json:"correlationId,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
}

// DeliveryAttempt represents a single immutable attempt row in delivery_attempts.
type DeliveryAttempt struct {
	ID                int64          `json:"id"`
	NotificationID    uuid.UUID      `json:"notificationId"`
	AttemptNo         int            `json:"attemptNo"`
	Counted           bool           `json:"counted"`
	StartedAt         time.Time      `json:"startedAt"`
	DurationMS        int            `json:"durationMs"`
	HTTPStatus        *int           `json:"httpStatus"`
	Outcome           AttemptOutcome `json:"outcome"`
	Error             *string        `json:"error"`
	ProviderMessageID *string        `json:"providerMessageId"`
	WorkerID          *string        `json:"workerId"`
}

// IdempotencyKey represents an API-level deduplication record with TTL expiration.
type IdempotencyKey struct {
	Key            string       `json:"key"`
	RequestHash    string       `json:"requestHash"`
	ResourceType   ResourceType `json:"resourceType"`
	ResourceID     uuid.UUID    `json:"resourceId"`
	ResponseStatus int          `json:"responseStatus"`
	CreatedAt      time.Time    `json:"createdAt"`
	ExpiresAt      time.Time    `json:"expiresAt"`
}

// CreateNotificationInput holds raw creation parameters validated before persistence.
type CreateNotificationInput struct {
	Recipient      string         `json:"recipient"`
	Channel        string         `json:"channel"`
	Content        string         `json:"content"`
	Priority       string         `json:"priority,omitempty"`
	Subject        *string        `json:"subject,omitempty"`
	Title          *string        `json:"title,omitempty"`
	ScheduledAt    *time.Time     `json:"scheduledAt,omitempty"`
	IdempotencyKey *string        `json:"idempotencyKey,omitempty"`
	TemplateID     *uuid.UUID     `json:"templateId,omitempty"`
	TemplateVars   map[string]any `json:"templateVariables,omitempty"`
}

// ValidationOptions controls channel and scheduling validation behavior.
type ValidationOptions struct {
	AllowEmptyContent      bool
	CheckScheduledAt       bool
	ClockSkewTolerance     time.Duration
	ScheduleMaxAhead       time.Duration
	ExtendedProviderFields bool
}
