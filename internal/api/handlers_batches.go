package api

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/realtime"
	"github.com/TITANBGG/notification-system/internal/repository"
)

func (h *handler) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	q := r.URL.Query()
	ve := &domain.ValidationError{}
	limit, _ := parsePageLimit(q.Get("limit"), h.cfg.PageLimitDefault, h.cfg.PageLimitMax, ve)
	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	cursor, err := DecodeCursor(q.Get("cursor"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	batch, err := h.store.GetBatch(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	summary, err := h.store.GetBatchSummary(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	countsByStatus := make(map[string]int64, len(summary))
	for st, cnt := range summary {
		countsByStatus[st.String()] = cnt
	}

	rows, err := h.store.ListBatchItems(r.Context(), id, cursor, limit+1)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	var nextCursor *string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		enc, encErr := EncodeCursor(repository.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if encErr != nil {
			writeError(w, r, h.logger, fmt.Errorf("encoding batch cursor: %w", encErr))
			return
		}
		nextCursor = &enc
	}

	writeJSON(w, h.logger, http.StatusOK, BatchDetailResponse{
		BatchID:        batch.ID,
		Total:          batch.Total,
		CountsByStatus: countsByStatus,
		CreatedAt:      batch.CreatedAt.UTC(),
		Items:          toNotificationResponses(rows),
		NextCursor:     nextCursor,
	})
}

func (h *handler) handleCancelBatch(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	batch, err := h.store.GetBatch(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	cancelledIDs, err := h.store.CancelBatch(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	if len(cancelledIDs) > 0 {
		if cancelErr := h.enqueuer.CancelDelayed(r.Context(), cancelledIDs...); cancelErr != nil {
			observability.LoggerFrom(r.Context(), h.logger).Warn("failed best-effort delayed cleanup on batch cancel",
				slog.String("batch_id", id.String()),
				slog.Int("count", len(cancelledIDs)),
				slog.String("error", cancelErr.Error()),
			)
		}
		cid := observability.CorrelationID(r.Context())
		now := h.nowFn().UTC()
		cancelledItems, listErr := h.listAllBatchItemsOrdered(r.Context(), id, batch.Total)
		if listErr == nil {
			cancelSet := make(map[string]struct{}, len(cancelledIDs))
			for _, cidUUID := range cancelledIDs {
				cancelSet[cidUUID.String()] = struct{}{}
			}
			var events []realtime.Event
			for _, item := range cancelledItems {
				if _, ok := cancelSet[item.ID.String()]; ok {
					events = append(events, realtime.NewEvent(item, now, cid))
					// Cancellation is a terminal outcome decided by the API, so the API (not a worker) counts it.
					h.metrics.IncNotificationProcessed(item.Channel.String(), domain.StatusCancelled.String())
				}
			}
			if len(events) > 0 && h.publisher != nil {
				if pubErr := h.publisher.Publish(r.Context(), events...); pubErr != nil {
					observability.LoggerFrom(r.Context(), h.logger).Warn("failed publishing batch cancel events",
						slog.String("batch_id", id.String()),
						slog.String("error", pubErr.Error()),
					)
				}
			}
		}
	}

	cancelled := len(cancelledIDs)
	notCancellable := batch.Total - cancelled
	if notCancellable < 0 {
		notCancellable = 0
	}

	writeJSON(w, h.logger, http.StatusOK, BatchCancelResponse{
		BatchID:        batch.ID,
		Cancelled:      cancelled,
		NotCancellable: notCancellable,
	})
}
