package api

import (
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/TITANBGG/notification-system/internal/app"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/realtime"
	"github.com/TITANBGG/notification-system/internal/repository"
	"github.com/TITANBGG/notification-system/internal/template"
)

func (h *handler) handleCreateNotification(w http.ResponseWriter, r *http.Request) {
	var req createNotificationRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	now := h.nowFn().UTC()
	rawIn := req.toDomainInput()

	ve := &domain.ValidationError{}
	idemKey, idemValid := parseIdempotencyHeader(r)
	if !idemValid {
		ve.Add("Idempotency-Key", "invalid_idempotency_key", "must be 1..255 printable ASCII characters")
	}
	if rawIn.IdempotencyKey != nil {
		ve.Add("idempotencyKey", "unsupported_field", "use the Idempotency-Key header for single notifications; body idempotencyKey is only supported for batch items")
	}

	var tmplByID map[uuid.UUID]domain.Template
	if rawIn.TemplateID != nil {
		tmpl, getErr := h.store.GetTemplate(r.Context(), *rawIn.TemplateID)
		if getErr != nil {
			if !errors.Is(getErr, domain.ErrNotFound) {
				writeError(w, r, h.logger, getErr)
				return
			}
		} else {
			tmplByID = map[uuid.UUID]domain.Template{tmpl.ID: *tmpl}
		}
	}

	in := h.resolveAndValidateNotificationInput("", rawIn, tmplByID, now, ve)
	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	norm := rawIn.Normalize()
	cid := observability.CorrelationID(r.Context())
	tp := observability.InjectTraceParent(r.Context())
	if tp == "" {
		tp = strings.TrimSpace(r.Header.Get("traceparent"))
	}
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

	renderedNorm := in.Normalize()
	status, scheduledAt := resolveScheduleState(renderedNorm.ScheduledAt, now)

	candidate := domain.Notification{
		ID:             notifID,
		Channel:        domain.Channel(renderedNorm.Channel),
		Priority:       domain.Priority(renderedNorm.Priority),
		Status:         status,
		Recipient:      renderedNorm.Recipient,
		Subject:        renderedNorm.Subject,
		Title:          renderedNorm.Title,
		Content:        renderedNorm.Content,
		IdempotencyKey: storedIdemKey,
		MaxAttempts:    h.cfg.MaxAttempts,
		ScheduledAt:    scheduledAt,
		CorrelationID:  optionalNonEmpty(cid),
		TraceParent:    optionalNonEmpty(tp),
		TemplateID:     norm.TemplateID,
		TemplateVars:   norm.TemplateVars,
	}

	res, err := h.notifications.Create(r.Context(), candidate, idemKey, reqHash, now)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	created, replayed, replayStatus := res.Notification, res.Replayed, res.Status

	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, h.logger, replayStatus, toNotificationResponse(*created))
		return
	}

	h.metrics.IncNotificationCreated(created.Channel.String(), created.Priority.String())
	// One shared budget for all post-commit Redis work of this request (see redisFollowUpTimeout).
	followCtx, cancelFollow := redisFollowUpCtx(r.Context())
	defer cancelFollow()
	h.publishStatusEvents(followCtx, cid, *created)

	// Commit to PostgreSQL completed before pushing queue hint to Redis so reconciler repairs enqueue failures.
	if enqErr := h.enqueueWithSpan(followCtx, []QueueItem{{
		ID:            created.ID,
		Channel:       created.Channel,
		Priority:      created.Priority,
		CorrelationID: cid,
		TraceParent:   tp,
		ScheduledAt:   created.ScheduledAt,
	}}); enqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue notification; reconciler will recover",
			slog.String("notification_id", created.ID.String()),
			slog.String("error", enqErr.Error()),
		)
	}

	writeJSON(w, h.logger, http.StatusAccepted, toNotificationResponse(*created))
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

	rawItems := make([]domain.CreateNotificationInput, len(req.Notifications))
	var templateIDs []uuid.UUID
	seenTemplateIDs := make(map[uuid.UUID]struct{})
	for i, itemReq := range req.Notifications {
		in := itemReq.toDomainInput()
		rawItems[i] = in
		if in.TemplateID != nil {
			if _, ok := seenTemplateIDs[*in.TemplateID]; !ok {
				seenTemplateIDs[*in.TemplateID] = struct{}{}
				templateIDs = append(templateIDs, *in.TemplateID)
			}
		}
	}

	tmplByID := make(map[uuid.UUID]domain.Template, len(templateIDs))
	if len(templateIDs) > 0 {
		tmpls, getErr := h.store.GetTemplatesByIDs(r.Context(), templateIDs)
		if getErr != nil {
			writeError(w, r, h.logger, getErr)
			return
		}
		for _, t := range tmpls {
			tmplByID[t.ID] = t
		}
	}

	normalizedRawItems := make([]domain.CreateNotificationInput, len(req.Notifications))
	renderedItems := make([]domain.CreateNotificationInput, len(req.Notifications))
	itemKeyIndexes := make(map[string][]int)

	for i, rawIn := range rawItems {
		prefix := fmt.Sprintf("notifications[%d].", i)
		rendered := h.resolveAndValidateNotificationInput(prefix, rawIn, tmplByID, now, ve)
		renderedItems[i] = rendered.Normalize()

		normRaw := rawIn.Normalize()
		normalizedRawItems[i] = normRaw
		if normRaw.IdempotencyKey != nil {
			itemKeyIndexes[*normRaw.IdempotencyKey] = append(itemKeyIndexes[*normRaw.IdempotencyKey], i)
		}
	}

	addDuplicateBatchItemKeyErrors(itemKeyIndexes, batchKey, ve)
	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	batchHash, err := domain.RequestHash(domain.BatchCreateInput{Notifications: normalizedRawItems})
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
	tp := observability.InjectTraceParent(r.Context())
	if tp == "" {
		tp = strings.TrimSpace(r.Header.Get("traceparent"))
	}
	notifications := make([]domain.Notification, len(renderedItems))
	itemKeys := make([]app.ItemKey, 0)

	for i, renderedNorm := range renderedItems {
		normRaw := normalizedRawItems[i]
		notifID, idErr := newUUIDv7()
		if idErr != nil {
			writeError(w, r, h.logger, fmt.Errorf("generating notification id: %w", idErr))
			return
		}
		status, scheduledAt := resolveScheduleState(renderedNorm.ScheduledAt, now)
		notifications[i] = domain.Notification{
			ID:             notifID,
			BatchID:        &batchID,
			Channel:        domain.Channel(renderedNorm.Channel),
			Priority:       domain.Priority(renderedNorm.Priority),
			Status:         status,
			Recipient:      renderedNorm.Recipient,
			Subject:        renderedNorm.Subject,
			Title:          renderedNorm.Title,
			Content:        renderedNorm.Content,
			IdempotencyKey: normRaw.IdempotencyKey,
			MaxAttempts:    h.cfg.MaxAttempts,
			ScheduledAt:    scheduledAt,
			CorrelationID:  optionalNonEmpty(cid),
			TraceParent:    optionalNonEmpty(tp),
			TemplateID:     normRaw.TemplateID,
			TemplateVars:   normRaw.TemplateVars,
		}
		if normRaw.IdempotencyKey != nil {
			itemHash, hErr := domain.RequestHash(normRaw)
			if hErr != nil {
				writeError(w, r, h.logger, fmt.Errorf("computing item request hash: %w", hErr))
				return
			}
			itemKeys = append(itemKeys, app.ItemKey{Index: i, Record: domain.IdempotencyKey{
				Key:            *normRaw.IdempotencyKey,
				RequestHash:    itemHash,
				ResourceType:   domain.ResourceNotification,
				ResourceID:     notifID,
				ResponseStatus: app.AcceptedStatus,
				ExpiresAt:      now.Add(h.cfg.IdempotencyTTL),
			}})
		}
	}

	res, err := h.notifications.CreateBatch(r.Context(), app.BatchInput{
		Batch:         domain.Batch{ID: batchID, Total: len(notifications), CorrelationID: optionalNonEmpty(cid)},
		Notifications: notifications,
		BatchKey:      batchKey,
		BatchHash:     batchHash,
		ItemKeys:      itemKeys,
	}, now)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	resp := BatchCreateResponse{BatchID: res.BatchID, Count: len(res.NotificationIDs), NotificationIDs: res.NotificationIDs}
	replayed, replayStatus := res.Replayed, res.Status

	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, h.logger, replayStatus, resp)
		return
	}

	for _, n := range notifications {
		h.metrics.IncNotificationCreated(n.Channel.String(), n.Priority.String())
	}
	// One shared budget for all post-commit Redis work of this request (see redisFollowUpTimeout).
	followCtx, cancelFollow := redisFollowUpCtx(r.Context())
	defer cancelFollow()
	h.publishStatusEvents(followCtx, cid, notifications...)

	queueItems := make([]QueueItem, len(notifications))
	for i, n := range notifications {
		queueItems[i] = QueueItem{
			ID:            n.ID,
			Channel:       n.Channel,
			Priority:      n.Priority,
			CorrelationID: cid,
			TraceParent:   tp,
			TraceLink:     true,
			ScheduledAt:   n.ScheduledAt,
		}
	}
	if enqErr := h.enqueueWithSpan(followCtx, queueItems); enqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue batch items; reconciler will recover",
			slog.String("batch_id", batchID.String()),
			slog.Int("count", len(queueItems)),
			slog.String("error", enqErr.Error()),
		)
	}

	writeJSON(w, h.logger, http.StatusAccepted, resp)
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

	// Best-effort ZREM + HDEL frees Redis memory for cancelled scheduled/retrying items;
	// PostgreSQL status='cancelled' is authoritative so worker Claim skips the row even if Redis fails.
	cleanupCtx, cancelCleanup := redisFollowUpCtx(r.Context())
	defer cancelCleanup()
	if cancelErr := h.enqueuer.CancelDelayed(cleanupCtx, updated.ID); cancelErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed best-effort delayed cleanup on cancel",
			slog.String("notification_id", updated.ID.String()),
			slog.String("error", cancelErr.Error()),
		)
	}
	h.metrics.IncNotificationProcessed(updated.Channel.String(), domain.StatusCancelled.String())
	h.publishStatusEvents(cleanupCtx, observability.CorrelationID(r.Context()), *updated)

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
	tp := observability.InjectTraceParent(r.Context())
	if tp == "" && updated.TraceParent != nil {
		tp = *updated.TraceParent
	}
	// One shared budget for all post-commit Redis work of this request (see redisFollowUpTimeout).
	followCtx, cancelFollow := redisFollowUpCtx(r.Context())
	defer cancelFollow()
	if enqErr := h.enqueueWithSpan(followCtx, []QueueItem{{
		ID:            updated.ID,
		Channel:       updated.Channel,
		Priority:      updated.Priority,
		CorrelationID: cid,
		TraceParent:   tp,
	}}); enqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to enqueue manual retry; reconciler will recover",
			slog.String("notification_id", updated.ID.String()),
			slog.String("error", enqErr.Error()),
		)
	}
	// The row left the DLQ in PostgreSQL; drop the ID from the informational Redis DLQ list too so
	// dlq_depth reflects currently failed notifications. Best effort: PostgreSQL stays authoritative.
	if dlqErr := h.enqueuer.ReleaseFromDLQ(followCtx, updated.ID); dlqErr != nil {
		observability.LoggerFrom(r.Context(), h.logger).Warn("failed to remove manually retried notification from DLQ list",
			slog.String("notification_id", updated.ID.String()),
			slog.String("error", dlqErr.Error()),
		)
	}
	h.publishStatusEvents(followCtx, cid, *updated)

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

// immediateScheduleThreshold treats scheduledAt values within now + 1s as immediate pending dispatches.
const immediateScheduleThreshold = time.Second

func resolveScheduleState(scheduledAt *time.Time, now time.Time) (domain.Status, *time.Time) {
	if scheduledAt == nil {
		return domain.StatusPending, nil
	}
	target := scheduledAt.UTC()
	if !target.After(now.UTC().Add(immediateScheduleThreshold)) {
		return domain.StatusPending, nil
	}
	return domain.StatusScheduled, &target
}

func (h *handler) resolveAndValidateNotificationInput(
	prefix string,
	in domain.CreateNotificationInput,
	tmplByID map[uuid.UUID]domain.Template,
	now time.Time,
	ve *domain.ValidationError,
) domain.CreateNotificationInput {
	hasContent := strings.TrimSpace(in.Content) != ""
	hasTemplate := in.TemplateID != nil

	if hasContent && hasTemplate {
		ve.Add(prefix+"content", "content_and_template_conflict", "provide either content or templateId, not both")
	}
	if !hasTemplate && len(in.TemplateVars) > 0 {
		ve.Add(prefix+"templateVariables", "template_id_required", "templateVariables cannot be provided without templateId")
	}

	allowEmptyContent := hasContent && hasTemplate // avoid spurious "content required" when both were sent
	resolved := in

	if hasTemplate && !hasContent {
		allowEmptyContent = true
		tmpl, exists := tmplByID[*in.TemplateID]
		if !exists {
			ve.Add(prefix+"templateId", "template_not_found", fmt.Sprintf("active template %s was not found", *in.TemplateID))
		} else {
			reqCh, chErr := domain.ParseChannel(in.Channel)
			if chErr == nil && tmpl.Channel != reqCh {
				ve.Add(
					prefix+"templateId",
					"template_channel_mismatch",
					fmt.Sprintf("template channel %q does not match notification channel %q", tmpl.Channel, reqCh),
				)
			}
			rendered, renderErr := template.Render(prefix, tmpl, in.TemplateVars)
			if renderErr != nil {
				ve.Fields = append(ve.Fields, renderErr.Fields...)
			} else {
				resolved.Content = rendered.Content
				if resolved.Subject == nil && rendered.Subject != nil {
					resolved.Subject = rendered.Subject
				}
				if resolved.Title == nil && rendered.Title != nil {
					resolved.Title = rendered.Title
				}
				// Re-enable content emptiness check on the rendered output.
				allowEmptyContent = false
			}
		}
	}

	opts := domain.ValidationOptions{
		AllowEmptyContent:      allowEmptyContent,
		CheckScheduledAt:       true,
		ClockSkewTolerance:     h.cfg.ClockSkewTolerance,
		ScheduleMaxAhead:       h.cfg.ScheduleMaxAhead,
		ExtendedProviderFields: h.cfg.ProviderExtended,
	}
	if domErr := domain.ValidateNotificationWithPrefix(prefix, resolved, now, opts); domErr != nil {
		ve.Fields = append(ve.Fields, domErr.Fields...)
	}
	return resolved
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

// redisFollowUpTimeout bounds the best-effort Redis writes made after a PostgreSQL commit (enqueue,
// realtime publish, delayed/DLQ cleanup). The row is already durable and the reconciler repairs any
// missing Redis state, so during a Redis outage the API answers in ~2s instead of waiting out the
// client's dial/read timeouts and retries (measured at ~11s by scripts/drills.sh drill 1).
const redisFollowUpTimeout = 2 * time.Second

func redisFollowUpCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, redisFollowUpTimeout)
}

func (h *handler) enqueueWithSpan(ctx context.Context, items []QueueItem) error {
	ctx, cancel := redisFollowUpCtx(ctx)
	defer cancel()
	enqCtx, span := observability.Tracer("notification-api").Start(ctx, "queue.enqueue")
	span.SetAttributes(attribute.Int("notification.count", len(items)))
	if len(items) == 1 {
		span.SetAttributes(
			attribute.String("notification.id", items[0].ID.String()),
			attribute.String("notification.channel", items[0].Channel.String()),
			attribute.String("notification.priority", items[0].Priority.String()),
		)
	}
	err := h.enqueuer.Enqueue(enqCtx, items)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
	return err
}

func (h *handler) publishStatusEvents(ctx context.Context, correlationID string, items ...domain.Notification) {
	if h.publisher == nil || len(items) == 0 {
		return
	}
	now := h.nowFn().UTC()
	events := make([]realtime.Event, len(items))
	for i, n := range items {
		events[i] = realtime.NewEvent(n, now, correlationID)
	}
	pubCtx, cancel := redisFollowUpCtx(ctx)
	defer cancel()
	if err := h.publisher.Publish(pubCtx, events...); err != nil {
		observability.LoggerFrom(ctx, h.logger).Warn("failed publishing realtime status events",
			slog.Int("count", len(events)),
			slog.String("error", err.Error()),
		)
	}
}

func optionalNonEmpty(s string) *string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
