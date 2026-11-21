package api

import (
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
)

type createNotificationRequest struct {
	Recipient         string         `json:"recipient"`
	Channel           string         `json:"channel"`
	Content           string         `json:"content"`
	Priority          string         `json:"priority,omitempty"`
	Subject           *string        `json:"subject,omitempty"`
	Title             *string        `json:"title,omitempty"`
	ScheduledAt       *time.Time     `json:"scheduledAt,omitempty"`
	IdempotencyKey    *string        `json:"idempotencyKey,omitempty"`
	TemplateID        *uuid.UUID     `json:"templateId,omitempty"`
	TemplateVariables map[string]any `json:"templateVariables,omitempty"`
}

func (r createNotificationRequest) toDomainInput() domain.CreateNotificationInput {
	return domain.CreateNotificationInput{
		Recipient:      r.Recipient,
		Channel:        r.Channel,
		Content:        r.Content,
		Priority:       r.Priority,
		Subject:        r.Subject,
		Title:          r.Title,
		ScheduledAt:    r.ScheduledAt,
		IdempotencyKey: r.IdempotencyKey,
		TemplateID:     r.TemplateID,
		TemplateVars:   r.TemplateVariables,
	}
}

type batchCreateRequest struct {
	Notifications []createNotificationRequest `json:"notifications"`
}

// LiveStatusResponse represents the liveness probe response payload.
type LiveStatusResponse struct {
	Status string `json:"status"`
}

// DependencyCheck describes the health status and round-trip latency of one external dependency.
type DependencyCheck struct {
	Status    string `json:"status"`
	LatencyMS int64  `json:"latencyMs"`
}

// ReadyChecksResponse holds individual Postgres and Redis readiness check results.
type ReadyChecksResponse struct {
	Postgres DependencyCheck `json:"postgres"`
	Redis    DependencyCheck `json:"redis"`
}

// ReadyStatusResponse is returned by GET /health and GET /health/ready.
type ReadyStatusResponse struct {
	Status string              `json:"status"`
	Checks ReadyChecksResponse `json:"checks"`
}

// ChannelQueueDepthsResponse holds per-priority ready list lengths for one notification channel.
type ChannelQueueDepthsResponse struct {
	High   int64 `json:"high"`
	Normal int64 `json:"normal"`
	Low    int64 `json:"low"`
}

// QueuesSnapshotResponse holds live Redis queue, delayed, DLQ, and inflight list depths.
type QueuesSnapshotResponse struct {
	SMS      ChannelQueueDepthsResponse `json:"sms"`
	Email    ChannelQueueDepthsResponse `json:"email"`
	Push     ChannelQueueDepthsResponse `json:"push"`
	Delayed  int64                      `json:"delayed"`
	DLQ      int64                      `json:"dlq"`
	Inflight int64                      `json:"inflight"`
}

// WindowChannelStatsResponse holds outcome counts and rates for a channel in a trailing time window.
// The rates are null when the window has no sent or failed notification: 0 would read as "every
// delivery failed" on a dashboard, while the truth is "nothing to measure".
type WindowChannelStatsResponse struct {
	Sent        int64    `json:"sent"`
	Failed      int64    `json:"failed"`
	Retrying    int64    `json:"retrying"`
	SuccessRate *float64 `json:"successRate"`
	FailureRate *float64 `json:"failureRate"`
}

// WindowChannelsResponse holds one window's stats per channel, in the same order as the queues.
type WindowChannelsResponse struct {
	SMS   WindowChannelStatsResponse `json:"sms"`
	Email WindowChannelStatsResponse `json:"email"`
	Push  WindowChannelStatsResponse `json:"push"`
}

// WindowsResponse holds the trailing windows in chronological order (a map would serialize as
// 15m, 1m, 5m).
type WindowsResponse struct {
	OneMinute      WindowChannelsResponse `json:"1m"`
	FiveMinutes    WindowChannelsResponse `json:"5m"`
	FifteenMinutes WindowChannelsResponse `json:"15m"`
}

// PercentilesResponse holds p50/p95/p99 latency percentiles in milliseconds.
type PercentilesResponse struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// LatencyMetricsResponse groups trailing 15-minute end-to-end and provider-call latency percentiles.
type LatencyMetricsResponse struct {
	EndToEnd     PercentilesResponse `json:"endToEnd"`
	ProviderCall PercentilesResponse `json:"providerCall"`
}

// MetricsSnapshotResponse is the JSON body returned by GET /api/v1/metrics.
type MetricsSnapshotResponse struct {
	GeneratedAt time.Time              `json:"generatedAt"`
	Queues      QueuesSnapshotResponse `json:"queues"`
	Windows     WindowsResponse        `json:"windows"`
	LatencyMS   LatencyMetricsResponse `json:"latencyMs"`
}

// NotificationResponse is the public HTTP representation of a notification.
// It intentionally excludes internal persistence fields such as processingStartedAt,
// traceparent, idempotencyKey, and deferrals.
type NotificationResponse struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batchId"`
	TemplateID        *uuid.UUID `json:"templateId,omitempty"`
	Recipient         string     `json:"recipient"`
	Channel           string     `json:"channel"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	Subject           *string    `json:"subject,omitempty"`
	Title             *string    `json:"title,omitempty"`
	Content           string     `json:"content"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"maxAttempts"`
	ScheduledAt       *time.Time `json:"scheduledAt"`
	NextAttemptAt     *time.Time `json:"nextAttemptAt"`
	ProviderMessageID *string    `json:"providerMessageId"`
	LastError         *string    `json:"lastError"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	SentAt            *time.Time `json:"sentAt"`
	FailedAt          *time.Time `json:"failedAt"`
	CancelledAt       *time.Time `json:"cancelledAt"`
}

type upsertTemplateRequest struct {
	Name      string   `json:"name"`
	Channel   string   `json:"channel"`
	Subject   *string  `json:"subject,omitempty"`
	Title     *string  `json:"title,omitempty"`
	Body      string   `json:"body"`
	Variables []string `json:"variables"`
}

type previewTemplateRequest struct {
	Variables map[string]any `json:"variables"`
}

// TemplateResponse is the public HTTP representation of a message template.
type TemplateResponse struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Subject   *string   `json:"subject,omitempty"`
	Title     *string   `json:"title,omitempty"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// TemplateListResponse is returned by GET /api/v1/templates.
type TemplateListResponse struct {
	Data       []TemplateResponse `json:"data"`
	NextCursor *string            `json:"nextCursor"`
}

// SMSPreviewInfo describes SMS encoding, code units, and segment count for a rendered SMS preview.
type SMSPreviewInfo struct {
	Encoding string `json:"encoding"`
	Units    int    `json:"units"`
	Segments int    `json:"segments"`
}

// TemplatePreviewResponse is returned by POST /api/v1/templates/{id}/preview.
type TemplatePreviewResponse struct {
	Subject         *string         `json:"subject"`
	Title           *string         `json:"title"`
	Content         string          `json:"content"`
	SMS             *SMSPreviewInfo `json:"sms,omitempty"`
	UnusedVariables []string        `json:"unusedVariables"`
}

// DeliveryAttemptResponse is the public representation of one recorded delivery attempt.
type DeliveryAttemptResponse struct {
	ID                int64     `json:"id"`
	AttemptNo         int       `json:"attemptNo"`
	Counted           bool      `json:"counted"`
	StartedAt         time.Time `json:"startedAt"`
	DurationMS        int       `json:"durationMs"`
	HTTPStatus        *int      `json:"httpStatus"`
	Outcome           string    `json:"outcome"`
	Error             *string   `json:"error"`
	ProviderMessageID *string   `json:"providerMessageId"`
	WorkerID          *string   `json:"workerId,omitempty"`
}

// NotificationDetailResponse combines NotificationResponse with its delivery attempt history.
type NotificationDetailResponse struct {
	NotificationResponse
	DeliveryAttempts []DeliveryAttemptResponse `json:"deliveryAttempts"`
}

// BatchCreateResponse is returned by POST /api/v1/notifications/batch.
type BatchCreateResponse struct {
	BatchID         uuid.UUID   `json:"batchId"`
	Count           int         `json:"count"`
	NotificationIDs []uuid.UUID `json:"notificationIds"`
}

// NotificationListResponse is returned by GET /api/v1/notifications.
type NotificationListResponse struct {
	Data       []NotificationResponse `json:"data"`
	NextCursor *string                `json:"nextCursor"`
}

// BatchDetailResponse is returned by GET /api/v1/batches/{id}.
type BatchDetailResponse struct {
	BatchID        uuid.UUID              `json:"batchId"`
	Total          int                    `json:"total"`
	CountsByStatus map[string]int64       `json:"countsByStatus"`
	CreatedAt      time.Time              `json:"createdAt"`
	Items          []NotificationResponse `json:"items"`
	NextCursor     *string                `json:"nextCursor"`
}

// BatchCancelResponse is returned by POST /api/v1/batches/{id}/cancel.
type BatchCancelResponse struct {
	BatchID        uuid.UUID `json:"batchId"`
	Cancelled      int       `json:"cancelled"`
	NotCancellable int       `json:"notCancellable"`
}

func toNotificationResponse(n domain.Notification) NotificationResponse {
	return NotificationResponse{
		ID:                n.ID,
		BatchID:           n.BatchID,
		TemplateID:        n.TemplateID,
		Recipient:         n.Recipient,
		Channel:           n.Channel.String(),
		Priority:          n.Priority.String(),
		Status:            n.Status.String(),
		Subject:           n.Subject,
		Title:             n.Title,
		Content:           n.Content,
		Attempts:          n.Attempts,
		MaxAttempts:       n.MaxAttempts,
		ScheduledAt:       utcTimePtr(n.ScheduledAt),
		NextAttemptAt:     utcTimePtr(n.NextAttemptAt),
		ProviderMessageID: n.ProviderMessageID,
		LastError:         n.LastError,
		CreatedAt:         n.CreatedAt.UTC(),
		UpdatedAt:         n.UpdatedAt.UTC(),
		SentAt:            utcTimePtr(n.SentAt),
		FailedAt:          utcTimePtr(n.FailedAt),
		CancelledAt:       utcTimePtr(n.CancelledAt),
	}
}

func toTemplateResponse(t domain.Template) TemplateResponse {
	vars := t.Variables
	if vars == nil {
		vars = []string{}
	}
	return TemplateResponse{
		ID:        t.ID,
		Name:      t.Name,
		Channel:   t.Channel.String(),
		Subject:   t.Subject,
		Title:     t.Title,
		Body:      t.Body,
		Variables: vars,
		CreatedAt: t.CreatedAt.UTC(),
		UpdatedAt: t.UpdatedAt.UTC(),
	}
}

func toTemplateResponses(items []domain.Template) []TemplateResponse {
	out := make([]TemplateResponse, 0, len(items))
	for _, t := range items {
		out = append(out, toTemplateResponse(t))
	}
	return out
}

func toNotificationResponses(items []domain.Notification) []NotificationResponse {
	out := make([]NotificationResponse, 0, len(items))
	for _, n := range items {
		out = append(out, toNotificationResponse(n))
	}
	return out
}

func toAttemptResponses(attempts []domain.DeliveryAttempt) []DeliveryAttemptResponse {
	out := make([]DeliveryAttemptResponse, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, DeliveryAttemptResponse{
			ID:                a.ID,
			AttemptNo:         a.AttemptNo,
			Counted:           a.Counted,
			StartedAt:         a.StartedAt.UTC(),
			DurationMS:        a.DurationMS,
			HTTPStatus:        a.HTTPStatus,
			Outcome:           a.Outcome.String(),
			Error:             a.Error,
			ProviderMessageID: a.ProviderMessageID,
			WorkerID:          a.WorkerID,
		})
	}
	return out
}

func utcTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
