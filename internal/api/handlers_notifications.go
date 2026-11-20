package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/repository"
)

var (
	errIdempotencyKeyConflictSentinel = errors.New("idempotency key exists")
	errBatchIdempotencyExistsSentinel = errors.New("batch idempotency key exists")
)

func (h *handler) handleCreateNotification(w http.ResponseWriter, r *http.Request) {
	var req createNotificationRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	now := h.nowFn().UTC()
	in := req.toDomainInput()

	ve := &domain.ValidationError{}
	idemKey, idemValid := parseIdempotencyHeader(r)
	if !idemValid {
		ve.Add("Idempotency-Key", "invalid_idempotency_key", "must be 1..255 printable ASCII characters")
	}

	addPhase3UnsupportedErrors("", in, ve)
	if in.IdempotencyKey != nil {
		ve.Add("idempotencyKey", "unsupported_field", "use the Idempotency-Key header for single notifications; body idempotencyKey is only supported for batch items")
	}
	opts := domain.ValidationOptions{
		AllowEmptyContent:      in.TemplateID != nil,
		CheckScheduledAt:       false,
		ClockSkewTolerance:     h.cfg.ClockSkewTolerance,
		ScheduleMaxAhead:       h.cfg.ScheduleMaxAhead,
		ExtendedProviderFields: h.cfg.ProviderExtended,
	}
	if domErr := domain.ValidateNotification(in, now, opts); domErr != nil {
		ve.Fields = append(ve.Fields, domErr.Fields...)
	}
	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	norm := in.Normalize()
	cid := observability.CorrelationID(r.Context())
	reqHash, err := domain.RequestHash(norm)
	if err != nil {
		writeError(w, r, h.logger, fmt.Errorf("computing request hash: %w", err))
		return
	}

	notifID, err := newUUIDv7()
	if err != nil {
		writeError(w, r, h.logger, fmt.Errorf("generating notification id: %w", err))
		return
	}

	storedIdemKey := norm.IdempotencyKey
	if idemKey != "" {
		kCopy := idemKey
		storedIdemKey = &kCopy
	}

	candidate := domain.Notification{
		ID:             notifID,
		Channel:        domain.Channel(norm.Channel),
		Priority:       domain.Priority(norm.Priority),
		Status:         domain.StatusPending,
		Recipient:      norm.Recipient,
		Subject:        norm.Subject,
		Title:          norm.Title,
		Content:        norm.Content,
		IdempotencyKey: storedIdemKey,
		MaxAttempts:    h.cfg.MaxAttempts,
		CorrelationID:  optionalNonEmpty(cid),
	}

	created, replayed, replayStatus, err := h.persistSingleNotification(r.Context(), candidate, idemKey, reqHash, now)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, h.logger, replayStatus, toNotificationResponse(*created))
		return
	}

	h.metrics.IncNotificationCreated(created.Channel.String(), created.Priority.String())

	// Commit to PostgreSQL completed before pushing queue hint to Redis so reconciler repairs enqueue failures.
	if enqErr := h.enqueuer.Enqueue(r.Context(), []QueueItem{{
		ID:            created.ID,
		Channel:       created.Channel,
		Priority:      created.Priority,
		CorrelationID: cid,
	}}); enqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue notification; reconciler will recover",
			slog.String("notification_id", created.ID.String()),
			slog.String("error", enqErr.Error()),
		)
	}

	writeJSON(w, h.logger, http.StatusAccepted, toNotificationResponse(*created))
}

func (h *handler) persistSingleNotification(
	ctx context.Context,
	candidate domain.Notification,
	idemKey string,
	reqHash string,
	now time.Time,
) (*domain.Notification, bool, int, error) {
	// At most 2 attempts handles the narrow window where a conflicting key expires between insert and lookup.
	for attempt := 0; attempt < 2; attempt++ {
		var inserted *domain.Notification
		txErr := h.store.WithTx(ctx, func(txCtx context.Context, tx Store) error {
			if idemKey != "" {
				_, ok, err := tx.InsertIdempotencyKey(txCtx, domain.IdempotencyKey{
					Key:            idemKey,
					RequestHash:    reqHash,
					ResourceType:   domain.ResourceNotification,
					ResourceID:     candidate.ID,
					ResponseStatus: http.StatusAccepted,
					ExpiresAt:      now.Add(h.cfg.IdempotencyTTL),
				})
				if err != nil {
					return err
				}
				if !ok {
					return errIdempotencyKeyConflictSentinel
				}
			}

			row, err := tx.InsertNotification(txCtx, candidate)
			if err != nil {
				return err
			}
			inserted = row
			return nil
		})
		if txErr == nil {
			return inserted, false, http.StatusAccepted, nil
		}
		if !errors.Is(txErr, errIdempotencyKeyConflictSentinel) {
			return nil, false, 0, txErr
		}

		existing, err := h.store.GetIdempotencyKey(ctx, idemKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return nil, false, 0, err
		}
		if existing.ResourceType != domain.ResourceNotification || existing.RequestHash != reqHash {
			return nil, false, 0, domain.ErrIdempotencyConflict
		}

		orig, err := h.store.GetNotification(ctx, existing.ResourceID)
		if err != nil {
			return nil, false, 0, err
		}
		return orig, true, existing.ResponseStatus, nil
	}

	return nil, false, 0, domain.ErrIdempotencyConflict
}

func (h *handler) handleCreateBatch(w http.ResponseWriter, r *http.Request) {
	var req batchCreateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	if len(req.Notifications) == 0 {
		ve := &domain.ValidationError{}
		ve.Add("notifications", "required", "must contain at least 1 notification")
		writeError(w, r, h.logger, ve)
		return
	}
	if len(req.Notifications) > h.cfg.BatchMaxItems {
		writeError(w, r, h.logger, &apiError{
			status:  http.StatusUnprocessableEntity,
			code:    "batch_too_large",
			message: fmt.Sprintf("batch contains %d notifications, max %d", len(req.Notifications), h.cfg.BatchMaxItems),
		})
		return
	}

	now := h.nowFn().UTC()
	ve := &domain.ValidationError{}
	batchKey, batchKeyValid := parseIdempotencyHeader(r)
	if !batchKeyValid {
		ve.Add("Idempotency-Key", "invalid_idempotency_key", "must be 1..255 printable ASCII characters")
	}

	normalizedItems := make([]domain.CreateNotificationInput, len(req.Notifications))
	itemKeyIndexes := make(map[string][]int)

	for i, itemReq := range req.Notifications {
		prefix := fmt.Sprintf("notifications[%d].", i)
		in := itemReq.toDomainInput()

		addPhase3UnsupportedErrors(prefix, in, ve)
		opts := domain.ValidationOptions{
			AllowEmptyContent:      in.TemplateID != nil,
			CheckScheduledAt:       false,
			ClockSkewTolerance:     h.cfg.ClockSkewTolerance,
			ScheduleMaxAhead:       h.cfg.ScheduleMaxAhead,
			ExtendedProviderFields: h.cfg.ProviderExtended,
		}
		if domErr := domain.ValidateNotificationWithPrefix(prefix, in, now, opts); domErr != nil {
			ve.Fields = append(ve.Fields, domErr.Fields...)
		}

		norm := in.Normalize()
		normalizedItems[i] = norm
		if norm.IdempotencyKey != nil {
			itemKeyIndexes[*norm.IdempotencyKey] = append(itemKeyIndexes[*norm.IdempotencyKey], i)
		}
	}

	addDuplicateBatchItemKeyErrors(itemKeyIndexes, batchKey, ve)
	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	batchHash, err := domain.RequestHash(domain.BatchCreateInput{Notifications: normalizedItems})
	if err != nil {
		writeError(w, r, h.logger, fmt.Errorf("computing batch request hash: %w", err))
		return
	}

	batchID, err := newUUIDv7()
	if err != nil {
		writeError(w, r, h.logger, fmt.Errorf("generating batch id: %w", err))
		return
	}

	cid := observability.CorrelationID(r.Context())
	notifications := make([]domain.Notification, len(normalizedItems))
	itemKeys := make([]string, 0)
	itemKeyRecords := make([]domain.IdempotencyKey, 0)
	indexByItemKey := make(map[string]int)

	for i, norm := range normalizedItems {
		notifID, idErr := newUUIDv7()
		if idErr != nil {
			writeError(w, r, h.logger, fmt.Errorf("generating notification id: %w", idErr))
			return
		}
		notifications[i] = domain.Notification{
			ID:             notifID,
			BatchID:        &batchID,
			Channel:        domain.Channel(norm.Channel),
			Priority:       domain.Priority(norm.Priority),
			Status:         domain.StatusPending,
			Recipient:      norm.Recipient,
			Subject:        norm.Subject,
			Title:          norm.Title,
			Content:        norm.Content,
			IdempotencyKey: norm.IdempotencyKey,
			MaxAttempts:    h.cfg.MaxAttempts,
			CorrelationID:  optionalNonEmpty(cid),
		}
		if norm.IdempotencyKey != nil {
			itemHash, hErr := domain.RequestHash(norm)
			if hErr != nil {
				writeError(w, r, h.logger, fmt.Errorf("computing item request hash: %w", hErr))
				return
			}
			k := *norm.IdempotencyKey
			itemKeys = append(itemKeys, k)
			indexByItemKey[k] = i
			itemKeyRecords = append(itemKeyRecords, domain.IdempotencyKey{
				Key:            k,
				RequestHash:    itemHash,
				ResourceType:   domain.ResourceNotification,
				ResourceID:     notifID,
				ResponseStatus: http.StatusAccepted,
				ExpiresAt:      now.Add(h.cfg.IdempotencyTTL),
			})
		}
	}

	resp, replayed, replayStatus, err := h.persistBatch(
		r.Context(),
		domain.Batch{ID: batchID, Total: len(notifications), CorrelationID: optionalNonEmpty(cid)},
		notifications,
		batchKey,
		batchHash,
		itemKeys,
		itemKeyRecords,
		indexByItemKey,
		now,
	)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, h.logger, replayStatus, resp)
		return
	}

	for _, n := range notifications {
		h.metrics.IncNotificationCreated(n.Channel.String(), n.Priority.String())
	}

	queueItems := make([]QueueItem, len(notifications))
	for i, n := range notifications {
		queueItems[i] = QueueItem{
			ID:            n.ID,
			Channel:       n.Channel,
			Priority:      n.Priority,
			CorrelationID: cid,
		}
	}
	if enqErr := h.enqueuer.Enqueue(r.Context(), queueItems); enqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue batch items; reconciler will recover",
			slog.String("batch_id", batchID.String()),
			slog.Int("count", len(queueItems)),
			slog.String("error", enqErr.Error()),
		)
	}

	writeJSON(w, h.logger, http.StatusAccepted, resp)
}

func (h *handler) persistBatch(
	ctx context.Context,
	batch domain.Batch,
	notifications []domain.Notification,
	batchKey string,
	batchHash string,
	itemKeys []string,
	itemKeyRecords []domain.IdempotencyKey,
	indexByItemKey map[string]int,
	now time.Time,
) (BatchCreateResponse, bool, int, error) {
	for attempt := 0; attempt < 2; attempt++ {
		txErr := h.store.WithTx(ctx, func(txCtx context.Context, tx Store) error {
			// Insert batch-level key first so whole-batch replay succeeds cleanly before item-key checks.
			if batchKey != "" {
				_, ok, err := tx.InsertIdempotencyKey(txCtx, domain.IdempotencyKey{
					Key:            batchKey,
					RequestHash:    batchHash,
					ResourceType:   domain.ResourceBatch,
					ResourceID:     batch.ID,
					ResponseStatus: http.StatusAccepted,
					ExpiresAt:      now.Add(h.cfg.IdempotencyTTL),
				})
				if err != nil {
					return err
				}
				if !ok {
					return errBatchIdempotencyExistsSentinel
				}
			}

			if len(itemKeys) > 0 {
				existing, err := tx.GetIdempotencyKeysByKeys(txCtx, itemKeys)
				if err != nil {
					return err
				}
				if len(existing) > 0 {
					return newItemIdempotencyConflictError(existing, indexByItemKey)
				}
				for _, rec := range itemKeyRecords {
					_, ok, err := tx.InsertIdempotencyKey(txCtx, rec)
					if err != nil {
						return err
					}
					if !ok {
						conflictRow, getErr := tx.GetIdempotencyKey(txCtx, rec.Key)
						if getErr != nil {
							return &apiError{
								status:  http.StatusConflict,
								code:    "item_idempotency_key_exists",
								message: "one or more item idempotency keys already exist",
							}
						}
						return newItemIdempotencyConflictError([]domain.IdempotencyKey{*conflictRow}, indexByItemKey)
					}
				}
			}

			if _, err := tx.InsertBatch(txCtx, batch); err != nil {
				return err
			}
			copied, err := tx.CopyNotifications(txCtx, notifications)
			if err != nil {
				return err
			}
			if int(copied) != len(notifications) {
				return fmt.Errorf("copied %d of %d batch notifications", copied, len(notifications))
			}
			return nil
		})
		if txErr == nil {
			ids := make([]uuid.UUID, len(notifications))
			for i, n := range notifications {
				ids[i] = n.ID
			}
			return BatchCreateResponse{
				BatchID:         batch.ID,
				Count:           len(ids),
				NotificationIDs: ids,
			}, false, http.StatusAccepted, nil
		}
		if !errors.Is(txErr, errBatchIdempotencyExistsSentinel) {
			return BatchCreateResponse{}, false, 0, txErr
		}

		existing, err := h.store.GetIdempotencyKey(ctx, batchKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return BatchCreateResponse{}, false, 0, err
		}
		if existing.ResourceType != domain.ResourceBatch || existing.RequestHash != batchHash {
			return BatchCreateResponse{}, false, 0, domain.ErrIdempotencyConflict
		}

		origBatch, err := h.store.GetBatch(ctx, existing.ResourceID)
		if err != nil {
			return BatchCreateResponse{}, false, 0, err
		}
		items, err := h.listAllBatchItemsOrdered(ctx, origBatch.ID, origBatch.Total)
		if err != nil {
			return BatchCreateResponse{}, false, 0, err
		}
		ids := make([]uuid.UUID, len(items))
		for i, item := range items {
			ids[i] = item.ID
		}
		return BatchCreateResponse{
			BatchID:         origBatch.ID,
			Count:           len(ids),
			NotificationIDs: ids,
		}, true, existing.ResponseStatus, nil
	}

	return BatchCreateResponse{}, false, 0, domain.ErrIdempotencyConflict
}

func (h *handler) listAllBatchItemsOrdered(ctx context.Context, batchID uuid.UUID, total int) ([]domain.Notification, error) {
	all := make([]domain.Notification, 0, total)
	var cursor *repository.Cursor
	pageSize := total
	if pageSize <= 0 {
		pageSize = h.cfg.BatchMaxItems
	}

	for {
		page, err := h.store.ListBatchItems(ctx, batchID, cursor, pageSize+1)
		if err != nil {
			return nil, err
		}
		hasMore := len(page) > pageSize
		if hasMore {
			page = page[:pageSize]
		}
		all = append(all, page...)
		if !hasMore || len(page) == 0 {
			break
		}
		last := page[len(page)-1]
		cursor = &repository.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	// Sort by monotonic UUIDv7 bytes ascending to restore original request submission order on replay.
	sort.Slice(all, func(i, j int) bool {
		return bytes.Compare(all[i].ID[:], all[j].ID[:]) < 0
	})
	return all, nil
}

func (h *handler) handleGetNotification(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	n, err := h.store.GetNotification(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	attempts, err := h.store.ListAttempts(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	writeJSON(w, h.logger, http.StatusOK, NotificationDetailResponse{
		NotificationResponse: toNotificationResponse(*n),
		DeliveryAttempts:     toAttemptResponses(attempts),
	})
}

func (h *handler) handleListNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ve := &domain.ValidationError{}

	var filter repository.ListFilter

	if rawStatuses := strings.TrimSpace(q.Get("status")); rawStatuses != "" {
		parts := strings.Split(rawStatuses, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				ve.Add("status", "invalid_status", "status filter contains an empty element")
				continue
			}
			st, err := domain.ParseStatus(trimmed)
			if err != nil {
				ve.Add("status", "invalid_status", fmt.Sprintf("unsupported status %q", trimmed))
				continue
			}
			filter.Statuses = append(filter.Statuses, st)
		}
	}

	if rawCh := strings.TrimSpace(q.Get("channel")); rawCh != "" {
		ch, err := domain.ParseChannel(rawCh)
		if err != nil {
			ve.Add("channel", "invalid_channel", "must be one of sms, email, push")
		} else {
			filter.Channel = &ch
		}
	}

	if rawPr := strings.TrimSpace(q.Get("priority")); rawPr != "" {
		pr, err := domain.ParsePriority(rawPr)
		if err != nil {
			ve.Add("priority", "invalid_priority", "must be one of high, normal, low")
		} else {
			filter.Priority = &pr
		}
	}

	if rawFrom := strings.TrimSpace(q.Get("from")); rawFrom != "" {
		t, err := parseRFC3339UTC(rawFrom)
		if err != nil {
			ve.Add("from", "invalid_timestamp", "from must be a valid RFC3339 timestamp")
		} else {
			filter.From = &t
		}
	}
	if rawTo := strings.TrimSpace(q.Get("to")); rawTo != "" {
		t, err := parseRFC3339UTC(rawTo)
		if err != nil {
			ve.Add("to", "invalid_timestamp", "to must be a valid RFC3339 timestamp")
		} else {
			filter.To = &t
		}
	}
	if filter.From != nil && filter.To != nil && filter.From.After(*filter.To) {
		ve.Add("to", "invalid_range", "to must be greater than or equal to from")
	}

	limit, ok := parsePageLimit(q.Get("limit"), h.cfg.PageLimitDefault, h.cfg.PageLimitMax, ve)
	if ok {
		filter.Limit = limit + 1
	}

	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	cursor, err := DecodeCursor(q.Get("cursor"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	filter.Cursor = cursor

	rows, err := h.store.ListNotifications(r.Context(), filter)
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
			writeError(w, r, h.logger, fmt.Errorf("encoding pagination cursor: %w", encErr))
			return
		}
		nextCursor = &enc
	}

	writeJSON(w, h.logger, http.StatusOK, NotificationListResponse{
		Data:       toNotificationResponses(rows),
		NextCursor: nextCursor,
	})
}

func (h *handler) handleCancelNotification(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	updated, err := h.store.Cancel(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			cur, getErr := h.store.GetNotification(r.Context(), id)
			if getErr != nil {
				writeError(w, r, h.logger, getErr)
				return
			}
			writeError(w, r, h.logger, &apiError{
				status:  http.StatusConflict,
				code:    "not_cancellable",
				message: fmt.Sprintf("notification cannot be cancelled in status %q", cur.Status),
				details: []domain.FieldError{{
					Field:   "status",
					Code:    "current_status",
					Message: cur.Status.String(),
				}},
			})
			return
		}
		writeError(w, r, h.logger, err)
		return
	}

	writeJSON(w, h.logger, http.StatusOK, toNotificationResponse(*updated))
}

func (h *handler) handleRetryNotification(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	updated, err := h.store.ManualRetry(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			cur, getErr := h.store.GetNotification(r.Context(), id)
			if getErr != nil {
				writeError(w, r, h.logger, getErr)
				return
			}
			writeError(w, r, h.logger, &apiError{
				status:  http.StatusConflict,
				code:    "not_retryable",
				message: fmt.Sprintf("notification cannot be retried in status %q", cur.Status),
				details: []domain.FieldError{{
					Field:   "status",
					Code:    "current_status",
					Message: cur.Status.String(),
				}},
			})
			return
		}
		writeError(w, r, h.logger, err)
		return
	}

	cid := observability.CorrelationID(r.Context())
	if cid == "" && updated.CorrelationID != nil {
		cid = *updated.CorrelationID
	}
	if enqErr := h.enqueuer.Enqueue(r.Context(), []QueueItem{{
		ID:            updated.ID,
		Channel:       updated.Channel,
		Priority:      updated.Priority,
		CorrelationID: cid,
	}}); enqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue manual retry; reconciler will recover",
			slog.String("notification_id", updated.ID.String()),
			slog.String("error", enqErr.Error()),
		)
	}

	writeJSON(w, h.logger, http.StatusOK, toNotificationResponse(*updated))
}

func parseIdempotencyHeader(r *http.Request) (string, bool) {
	vals := r.Header.Values("Idempotency-Key")
	if len(vals) == 0 {
		return "", true
	}
	trimmed := strings.TrimSpace(vals[0])
	if !domain.IsValidIdempotencyKey(trimmed) {
		return "", false
	}
	return trimmed, true
}

func addPhase3UnsupportedErrors(prefix string, in domain.CreateNotificationInput, ve *domain.ValidationError) {
	if in.ScheduledAt != nil {
		ve.Add(prefix+"scheduledAt", "not_supported_yet", "scheduledAt is not supported until Phase 8")
	}
	if in.TemplateID != nil {
		ve.Add(prefix+"templateId", "not_supported_yet", "templateId is not supported until Phase 9")
	} else if len(in.TemplateVars) > 0 {
		ve.Add(prefix+"templateVariables", "not_supported_yet", "templateVariables are not supported until Phase 9")
	}
}

func addDuplicateBatchItemKeyErrors(itemKeyIndexes map[string][]int, batchKey string, ve *domain.ValidationError) {
	if len(itemKeyIndexes) == 0 {
		return
	}
	keys := make([]string, 0, len(itemKeyIndexes))
	for k := range itemKeyIndexes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		idxs := itemKeyIndexes[k]
		if batchKey != "" && k == batchKey {
			for _, idx := range idxs {
				ve.Add(
					fmt.Sprintf("notifications[%d].idempotencyKey", idx),
					"duplicate_idempotency_key",
					"item idempotencyKey must differ from batch Idempotency-Key header",
				)
			}
			continue
		}
		if len(idxs) > 1 {
			for _, idx := range idxs {
				ve.Add(
					fmt.Sprintf("notifications[%d].idempotencyKey", idx),
					"duplicate_idempotency_key",
					fmt.Sprintf("duplicate idempotencyKey shared at batch indexes %v", idxs),
				)
			}
		}
	}
}

func newItemIdempotencyConflictError(existing []domain.IdempotencyKey, indexByKey map[string]int) error {
	sort.Slice(existing, func(i, j int) bool {
		return indexByKey[existing[i].Key] < indexByKey[existing[j].Key]
	})
	details := make([]domain.FieldError, 0, len(existing))
	for _, ex := range existing {
		idx, ok := indexByKey[ex.Key]
		field := "idempotencyKey"
		if ok {
			field = fmt.Sprintf("notifications[%d].idempotencyKey", idx)
		}
		details = append(details, domain.FieldError{
			Field:   field,
			Code:    "item_idempotency_key_exists",
			Message: fmt.Sprintf("already exists for %s %s", ex.ResourceType, ex.ResourceID),
		})
	}
	return &apiError{
		status:  http.StatusConflict,
		code:    "item_idempotency_key_exists",
		message: "one or more batch item idempotency keys already exist",
		details: details,
	}
}

func parseUUIDParam(r *http.Request, name string) (uuid.UUID, error) {
	raw := strings.TrimSpace(chi.URLParam(r, name))
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, &apiError{
			status:  http.StatusBadRequest,
			code:    "invalid_uuid",
			message: fmt.Sprintf("parameter %q must be a valid UUID", name),
		}
	}
	return id, nil
}

func parsePageLimit(raw string, def, maxLimit int, ve *domain.ValidationError) (int, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return def, true
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil || n < 1 || n > maxLimit {
		ve.Add("limit", "invalid_limit", fmt.Sprintf("limit must be an integer between 1 and %d", maxLimit))
		return 0, false
	}
	return n, true
}

func parseRFC3339UTC(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func newUUIDv7() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func optionalNonEmpty(s string) *string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
