package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Channel represents a supported notification delivery medium.
type Channel string

// Supported delivery channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// ParseChannel normalizes and validates a raw channel string.
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

// Supported dispatch priority levels.
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

// Supported notification lifecycle states matching PostgreSQL CHECK constraints.
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

// ParseStatus normalizes and validates a raw status string.
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

// Supported idempotency resource categories.
const (
	ResourceNotification ResourceType = "notification"
	ResourceBatch        ResourceType = "batch"
)

// Notification is the persistence aggregate stored in PostgreSQL.
// Intentionally omits JSON tags so HTTP handlers must map through explicit public DTOs
// instead of exposing internal fields such as processingStartedAt, traceparent, or deferrals.
type Notification struct {
	ID                  uuid.UUID
	BatchID             *uuid.UUID
	Channel             Channel
	Priority            Priority
	Status              Status
	Recipient           string
	Subject             *string
	Title               *string
	Content             string
	IdempotencyKey      *string
	Attempts            int
	MaxAttempts         int
	Deferrals           int
	ScheduledAt         *time.Time
	NextAttemptAt       *time.Time
	ProcessingStartedAt *time.Time
	LastError           *string
	ProviderMessageID   *string
	CorrelationID       *string
	TraceParent         *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	SentAt              *time.Time
	FailedAt            *time.Time
	CancelledAt         *time.Time
}

// Batch represents a grouped submission of up to 1000 notifications.
type Batch struct {
	ID            uuid.UUID
	Total         int
	CorrelationID *string
	CreatedAt     time.Time
}

// DeliveryAttempt represents an immutable attempt audit row in delivery_attempts.
type DeliveryAttempt struct {
	ID                int64
	NotificationID    uuid.UUID
	AttemptNo         int
	Counted           bool
	StartedAt         time.Time
	DurationMS        int
	HTTPStatus        *int
	Outcome           AttemptOutcome
	Error             *string
	ProviderMessageID *string
	WorkerID          *string
}

// IdempotencyKey represents an API-level deduplication record with TTL expiration.
type IdempotencyKey struct {
	Key            string
	RequestHash    string
	ResourceType   ResourceType
	ResourceID     uuid.UUID
	ResponseStatus int
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

// CreateNotificationInput holds creation parameters validated before persistence.
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

// Normalize returns a copy with lowercase trimmed channel/priority (defaulting priority to normal)
// and UTC-normalized scheduledAt so DB writes and idempotency hashing observe identical canonical values.
// Recipient whitespace is preserved intentionally so channel validators enforce strict formatting.
func (in CreateNotificationInput) Normalize() CreateNotificationInput {
	out := in
	out.Channel = strings.ToLower(strings.TrimSpace(in.Channel))
	out.Priority = strings.ToLower(strings.TrimSpace(in.Priority))
	if out.Priority == "" {
		out.Priority = string(PriorityNormal)
	}
	if in.ScheduledAt != nil {
		utc := in.ScheduledAt.UTC()
		out.ScheduledAt = &utc
	}
	if in.IdempotencyKey != nil {
		trimmed := strings.TrimSpace(*in.IdempotencyKey)
		if trimmed == "" {
			out.IdempotencyKey = nil
		} else {
			out.IdempotencyKey = &trimmed
		}
	}
	return out
}

func (in CreateNotificationInput) canonicalize() any {
	return in.Normalize()
}

// BatchCreateInput represents the POST /api/v1/notifications/batch request payload.
type BatchCreateInput struct {
	Notifications []CreateNotificationInput `json:"notifications"`
}

// Normalize returns a copy with every item normalized for canonical batch hashing and persistence.
func (b BatchCreateInput) Normalize() BatchCreateInput {
	items := make([]CreateNotificationInput, len(b.Notifications))
	for i, item := range b.Notifications {
		items[i] = item.Normalize()
	}
	return BatchCreateInput{Notifications: items}
}

func (b BatchCreateInput) canonicalize() any {
	return b.Normalize()
}

// ValidationOptions controls channel and scheduling validation behavior.
type ValidationOptions struct {
	AllowEmptyContent      bool
	CheckScheduledAt       bool
	ClockSkewTolerance     time.Duration
	ScheduleMaxAhead       time.Duration
	ExtendedProviderFields bool
}
