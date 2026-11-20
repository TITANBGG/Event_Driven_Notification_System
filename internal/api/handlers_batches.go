package api

import (
	"fmt"
	"net/http"

	"github.com/TITANBGG/notification-system/internal/domain"
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
	limit, limitOK := parsePageLimit(q.Get("limit"), h.cfg.PageLimitDefault, h.cfg.PageLimitMax, ve)
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

	fetchLimit := limit
	if limitOK {
		fetchLimit = limit + 1
	}
	rows, err := h.store.ListBatchItems(r.Context(), id, cursor, fetchLimit)
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
