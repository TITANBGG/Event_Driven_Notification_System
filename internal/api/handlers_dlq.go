package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/repository"
)

const (
	bulkRetryDefaultLimit = 100
	bulkRetryMaxLimit     = 1000
)

// BulkRetryRequest selects failed notifications for a bulk DLQ replay. Every field is optional.
type BulkRetryRequest struct {
	Channel    *string `json:"channel,omitempty"`
	FailedFrom *string `json:"failedFrom,omitempty"`
	FailedTo   *string `json:"failedTo,omitempty"`
	Limit      *int    `json:"limit,omitempty"`
}

// BulkRetryResponse lists the notifications moved from failed back to pending.
type BulkRetryResponse struct {
	Retried         int         `json:"retried"`
	NotificationIDs []uuid.UUID `json:"notificationIds"`
}

// handleBulkRetry replays up to `limit` failed notifications (oldest failure first) in one fenced
// UPDATE. It exists for the case a single /retry cannot cover: after a provider credential or URL
// fix, the messages that failed before the breaker opened can be replayed in a few calls.
func (h *handler) handleBulkRetry(w http.ResponseWriter, r *http.Request) {
	var req BulkRetryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	filter := repository.RetryFailedFilter{Limit: bulkRetryDefaultLimit}
	ve := &domain.ValidationError{}
	if req.Channel != nil {
		ch, chErr := domain.ParseChannel(*req.Channel)
		if chErr != nil {
			ve.Add("channel", "invalid_channel", "must be one of sms, email, push")
		} else {
			filter.Channel = &ch
		}
	}
	parseTime := func(field string, raw *string) *time.Time {
		if raw == nil {
			return nil
		}
		t, err := time.Parse(time.RFC3339, *raw)
		if err != nil {
			ve.Add(field, "invalid_timestamp", "must be an RFC3339 timestamp")
			return nil
		}
		t = t.UTC()
		return &t
	}
	filter.FailedFrom = parseTime("failedFrom", req.FailedFrom)
	filter.FailedTo = parseTime("failedTo", req.FailedTo)
	if filter.FailedFrom != nil && filter.FailedTo != nil && !filter.FailedFrom.Before(*filter.FailedTo) {
		ve.Add("failedTo", "invalid_range", "must be after failedFrom")
	}
	if req.Limit != nil {
		if *req.Limit < 1 || *req.Limit > bulkRetryMaxLimit {
			ve.Add("limit", "out_of_range", "must be between 1 and 1000")
		} else {
			filter.Limit = *req.Limit
		}
	}
	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	rows, err := h.store.ManualRetryFailedBulk(r.Context(), filter)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	resp := BulkRetryResponse{Retried: len(rows), NotificationIDs: make([]uuid.UUID, len(rows))}
	if len(rows) > 0 {
		cid := observability.CorrelationID(r.Context())
		tp := observability.InjectTraceParent(r.Context())
		items := make([]QueueItem, len(rows))
		ids := make([]uuid.UUID, len(rows))
		for i, n := range rows {
			resp.NotificationIDs[i], ids[i] = n.ID, n.ID
			items[i] = QueueItem{ID: n.ID, Channel: n.Channel, Priority: n.Priority, CorrelationID: cid, TraceParent: tp}
		}
		// PostgreSQL first, then Redis: a failed enqueue leaves pending rows the reconciler re-enqueues.
		followCtx, cancelFollow := redisFollowUpCtx(r.Context())
		defer cancelFollow()
		if enqErr := h.enqueueWithSpan(followCtx, items); enqErr != nil {
			observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue bulk retry; reconciler will recover",
				slog.Int("count", len(rows)), slog.String("error", enqErr.Error()))
		}
		if dlqErr := h.enqueuer.ReleaseFromDLQ(followCtx, ids...); dlqErr != nil {
			observability.LoggerFrom(r.Context(), h.logger).Warn("failed to remove bulk-retried notifications from DLQ list",
				slog.Int("count", len(rows)), slog.String("error", dlqErr.Error()))
		}
		h.publishStatusEvents(followCtx, cid, rows...)
	}
	observability.LoggerFrom(r.Context(), h.logger).Info("bulk DLQ replay", slog.Int("retried", len(rows)))
	writeJSON(w, h.logger, http.StatusOK, resp)
}
