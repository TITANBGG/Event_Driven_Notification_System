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

// NotificationResponse is the public HTTP representation of a notification.
// It intentionally excludes internal persistence fields such as processingStartedAt,
// traceparent, idempotencyKey, and deferrals.
type NotificationResponse struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batchId"`
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
