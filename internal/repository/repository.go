package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository/sqlc"
)

// Cursor identifies a deterministic keyset pagination boundary on (created_at DESC, id DESC).
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// ListFilter configures optional query filters and keyset cursor parameters for ListNotifications.
type ListFilter struct {
	Statuses []domain.Status
	Channel  *domain.Channel
	Priority *domain.Priority
	From     *time.Time
	To       *time.Time
	Cursor   *Cursor
	Limit    int
}

// TemplateListFilter configures optional channel filtering and keyset pagination for ListTemplates.
type TemplateListFilter struct {
	Channel *domain.Channel
	Cursor  *Cursor
	Limit   int
}

// MetricsWindowRow represents aggregated delivery counts for a single (window, channel) pair.
type MetricsWindowRow struct {
	Window        string
	Channel       domain.Channel
	SentCount     int64
	FailedCount   int64
	RetryingCount int64
}

// LatencyStats holds 15-minute p50/p95/p99 latency percentiles in milliseconds.
type LatencyStats struct {
	EndToEndP50MS float64
	EndToEndP95MS float64
	EndToEndP99MS float64
	ProviderP50MS float64
	ProviderP95MS float64
	ProviderP99MS float64
}

// Store wraps a pgxpool.Pool and sqlc.Queries to provide domain-typed PostgreSQL access.
type Store struct {
	pool *pgxpool.Pool
	q    *sqlc.Queries
}

// New constructs a Store backed by the provided pgx connection pool.
func New(pool *pgxpool.Pool) *Store {
	if pool == nil {
		return &Store{}
	}
	return &Store{
		pool: pool,
		q:    sqlc.New(pool),
	}
}

// Ping verifies PostgreSQL connectivity within ctx's deadline.
func (s *Store) Ping(ctx context.Context) error {
	if s.pool == nil {
		return errors.New("repository pool is nil")
	}
	return s.pool.Ping(ctx)
}

// WithTx executes fn inside a single PostgreSQL transaction, rolling back automatically on error or panic.
// Atomic transactions serialize idempotency key creation alongside notification/batch inserts.
func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context, txStore *Store) error) error {
	if s.pool == nil {
		return errors.New("repository pool is nil")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		// WithoutCancel ensures rollback still runs even if the caller's request context expired.
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()

	txStore := &Store{
		pool: s.pool,
		q:    s.q.WithTx(tx),
	}
	if err := fn(ctx, txStore); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// MapError translates pgx/Postgres errors into typed domain sentinel errors.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s", domain.ErrDuplicateKey, pgErr.ConstraintName)
	}
	// Second line of defence behind domain validation: text PostgreSQL cannot store (NUL, invalid
	// encoding) is the client's input, so it maps to a 422, never a 500.
	if errors.As(err, &pgErr) && (pgErr.Code == "22021" || pgErr.Code == "22P05") {
		ve := &domain.ValidationError{}
		ve.Add("body", "invalid_character", "request contains characters that cannot be stored (for example NUL)")
		return ve
	}
	return err
}

// InsertBatch inserts a batch header row and returns the persisted domain.Batch.
func (s *Store) InsertBatch(ctx context.Context, b domain.Batch) (*domain.Batch, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.InsertBatch(ctx, sqlc.InsertBatchParams{
		ID:            b.ID,
		Total:         clampInt32(b.Total),
		CorrelationID: b.CorrelationID,
	})
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainBatch(row)
	return &out, nil
}

// GetBatch retrieves a batch header by ID.
func (s *Store) GetBatch(ctx context.Context, id uuid.UUID) (*domain.Batch, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.GetBatch(ctx, id)
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainBatch(row)
	return &out, nil
}

// GetBatchSummary aggregates per-status notification counts for a batch.
func (s *Store) GetBatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int64, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.GetBatchSummary(ctx, &batchID)
	if err != nil {
		return nil, MapError(err)
	}
	counts := make(map[domain.Status]int64, len(rows))
	for _, r := range rows {
		counts[domain.Status(r.Status)] = r.Count
	}
	return counts, nil
}

// ListBatchItems returns one keyset-paginated page of notifications belonging to batchID.
func (s *Store) ListBatchItems(ctx context.Context, batchID uuid.UUID, cursor *Cursor, limit int) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	var cursorTS *time.Time
	var cursorID *uuid.UUID
	if cursor != nil {
		u := cursor.CreatedAt.UTC()
		cursorTS = &u
		idCopy := cursor.ID
		cursorID = &idCopy
	}
	rows, err := s.q.ListBatchItems(ctx, sqlc.ListBatchItemsParams{
		BatchID:         batchID,
		CursorCreatedAt: cursorTS,
		CursorID:        cursorID,
		PageLimit:       clampInt32(limit),
	})
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// CancelBatch atomically cancels every cancellable notification in a batch and returns their IDs.
func (s *Store) CancelBatch(ctx context.Context, batchID uuid.UUID) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	// RETURNING * hands back the cancelled rows themselves, so the caller can publish their
	// status events without reading the whole batch again.
	rows, err := s.q.CancelBatch(ctx, &batchID)
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// InsertNotification persists a single notification row and returns the persisted aggregate.
func (s *Store) InsertNotification(ctx context.Context, n domain.Notification) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	varsRaw, err := marshalTemplateVars(n.TemplateVars)
	if err != nil {
		return nil, err
	}
	row, err := s.q.InsertNotification(ctx, sqlc.InsertNotificationParams{
		ID:             n.ID,
		BatchID:        n.BatchID,
		Channel:        n.Channel.String(),
		Priority:       n.Priority.String(),
		Status:         n.Status.String(),
		Recipient:      n.Recipient,
		Subject:        n.Subject,
		Title:          n.Title,
		Content:        n.Content,
		IdempotencyKey: n.IdempotencyKey,
		MaxAttempts:    clampInt32(n.MaxAttempts),
		ScheduledAt:    normalizeNullUTC(n.ScheduledAt),
		CorrelationID:  n.CorrelationID,
		Traceparent:    n.TraceParent,
		TemplateID:     n.TemplateID,
		TemplateVars:   varsRaw,
	})
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// CopyNotifications bulk-inserts batch notification rows via PostgreSQL COPY protocol.
func (s *Store) CopyNotifications(ctx context.Context, items []domain.Notification) (int64, error) {
	if s.q == nil {
		return 0, errors.New("repository store is not initialized")
	}
	params := make([]sqlc.CopyNotificationsParams, 0, len(items))
	for _, n := range items {
		varsRaw, err := marshalTemplateVars(n.TemplateVars)
		if err != nil {
			return 0, err
		}
		params = append(params, sqlc.CopyNotificationsParams{
			ID:             n.ID,
			BatchID:        n.BatchID,
			Channel:        n.Channel.String(),
			Priority:       n.Priority.String(),
			Status:         n.Status.String(),
			Recipient:      n.Recipient,
			Subject:        n.Subject,
			Title:          n.Title,
			Content:        n.Content,
			IdempotencyKey: n.IdempotencyKey,
			MaxAttempts:    clampInt32(n.MaxAttempts),
			ScheduledAt:    normalizeNullUTC(n.ScheduledAt),
			CorrelationID:  n.CorrelationID,
			Traceparent:    n.TraceParent,
			TemplateID:     n.TemplateID,
			TemplateVars:   varsRaw,
		})
	}
	count, err := s.q.CopyNotifications(ctx, params)
	if err != nil {
		return 0, MapError(err)
	}
	return count, nil
}

// GetNotification fetches a single notification by ID.
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.GetNotification(ctx, id)
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// ListNotifications returns keyset-paginated notifications matching optional filters.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	var statuses []string
	if len(f.Statuses) > 0 {
		statuses = make([]string, 0, len(f.Statuses))
		for _, st := range f.Statuses {
			statuses = append(statuses, st.String())
		}
	}
	var chPtr, prPtr *string
	if f.Channel != nil {
		s := f.Channel.String()
		chPtr = &s
	}
	if f.Priority != nil {
		s := f.Priority.String()
		prPtr = &s
	}
	var cursorTS *time.Time
	var cursorID *uuid.UUID
	if f.Cursor != nil {
		u := f.Cursor.CreatedAt.UTC()
		cursorTS = &u
		idCopy := f.Cursor.ID
		cursorID = &idCopy
	}
	rows, err := s.q.ListNotifications(ctx, sqlc.ListNotificationsParams{
		Statuses:        statuses,
		Channel:         chPtr,
		Priority:        prPtr,
		FromTime:        normalizeNullUTC(f.From),
		ToTime:          normalizeNullUTC(f.To),
		CursorCreatedAt: cursorTS,
		CursorID:        cursorID,
		PageLimit:       clampInt32(f.Limit),
	})
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// Claim atomically claims an eligible notification for processing and increments attempts.
// Returns (nil, false, nil) when the row is cancelled, already claimed, terminal, or not yet due.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, skew time.Duration) (*domain.Notification, bool, error) {
	if s.q == nil {
		return nil, false, errors.New("repository store is not initialized")
	}
	row, err := s.q.Claim(ctx, sqlc.ClaimParams{
		ID:       id,
		SkewSecs: skew.Seconds(),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, MapError(err)
	}
	out := toDomainNotification(row)
	return &out, true, nil
}

// MarkSent transitions processing -> sent using fencing on claimedAttempts.
// Returns domain.ErrLeaseLost if another actor already reclaimed or finished the row.
func (s *Store) MarkSent(ctx context.Context, id uuid.UUID, claimedAttempts int, providerMessageID *string) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.MarkSent(ctx, sqlc.MarkSentParams{
		ProviderMessageID: providerMessageID,
		ID:                id,
		ClaimedAttempts:   clampInt32(claimedAttempts),
	})
	if err != nil {
		return nil, mapFencedError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// ScheduleRetry transitions processing -> retrying with fencing on claimedAttempts.
func (s *Store) ScheduleRetry(ctx context.Context, id uuid.UUID, claimedAttempts int, nextAttemptAt time.Time, lastError string) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.ScheduleRetry(ctx, sqlc.ScheduleRetryParams{
		NextAttemptAt:   normalizeNullUTC(&nextAttemptAt),
		LastError:       &lastError,
		ID:              id,
		ClaimedAttempts: clampInt32(claimedAttempts),
	})
	if err != nil {
		return nil, mapFencedError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// DeferCircuitOpen transitions processing -> retrying, rolls back attempts by 1, and increments deferrals.
func (s *Store) DeferCircuitOpen(ctx context.Context, id uuid.UUID, claimedAttempts int, nextAttemptAt time.Time, lastError string) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.DeferCircuitOpen(ctx, sqlc.DeferCircuitOpenParams{
		NextAttemptAt:   normalizeNullUTC(&nextAttemptAt),
		LastError:       &lastError,
		ID:              id,
		ClaimedAttempts: clampInt32(claimedAttempts),
	})
	if err != nil {
		return nil, mapFencedError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// MarkFailed transitions processing -> failed with fencing on claimedAttempts.
func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, claimedAttempts int, lastError string) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.MarkFailed(ctx, sqlc.MarkFailedParams{
		LastError:       &lastError,
		ID:              id,
		ClaimedAttempts: clampInt32(claimedAttempts),
	})
	if err != nil {
		return nil, mapFencedError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// Cancel transitions a scheduled/pending/retrying notification to cancelled.
// Returns domain.ErrNotFound when absent or domain.ErrInvalidTransition when non-cancellable.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.Cancel(ctx, id)
	if err == nil {
		out := toDomainNotification(row)
		return &out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, MapError(err)
	}
	if _, getErr := s.GetNotification(ctx, id); getErr != nil {
		return nil, getErr
	}
	return nil, domain.ErrInvalidTransition
}

// ManualRetry resets a failed notification back to pending with zero attempts and deferrals.
func (s *Store) ManualRetry(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.ManualRetry(ctx, id)
	if err == nil {
		out := toDomainNotification(row)
		return &out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, MapError(err)
	}
	if _, getErr := s.GetNotification(ctx, id); getErr != nil {
		return nil, getErr
	}
	return nil, domain.ErrInvalidTransition
}

// RetryFailedFilter selects failed notifications for a bulk DLQ replay.
type RetryFailedFilter struct {
	Channel    *domain.Channel
	FailedFrom *time.Time
	FailedTo   *time.Time
	Limit      int
}

// ManualRetryFailedBulk resets up to f.Limit failed notifications (oldest failure first) to pending in
// one statement and returns them.
func (s *Store) ManualRetryFailedBulk(ctx context.Context, f RetryFailedFilter) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	params := sqlc.ManualRetryFailedBulkParams{PageLimit: clampInt32(f.Limit)}
	if f.Channel != nil {
		ch := f.Channel.String()
		params.Channel = &ch
	}
	if f.FailedFrom != nil {
		t := f.FailedFrom.UTC()
		params.FailedFrom = &t
	}
	if f.FailedTo != nil {
		t := f.FailedTo.UTC()
		params.FailedTo = &t
	}
	rows, err := s.q.ManualRetryFailedBulk(ctx, params)
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// ExtendLease restarts the visibility timeout of a claimed row, fenced on the claimed attempt.
// domain.ErrLeaseLost means the reconciler already reclaimed it.
func (s *Store) ExtendLease(ctx context.Context, id uuid.UUID, claimedAttempts int) error {
	if s.q == nil {
		return errors.New("repository store is not initialized")
	}
	n, err := s.q.ExtendProcessingLease(ctx, sqlc.ExtendProcessingLeaseParams{ID: id, ClaimedAttempts: clampInt32(claimedAttempts)})
	if err != nil {
		return MapError(err)
	}
	if n == 0 {
		return domain.ErrLeaseLost
	}
	return nil
}

// FindStalePending scans pending rows older than staleAfter for reconciler step (a).
func (s *Store) FindStalePending(ctx context.Context, staleAfter time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.FindStalePending(ctx, sqlc.FindStalePendingParams{
		StaleSecs:  staleAfter.Seconds(),
		AfterID:    afterID,
		BatchLimit: clampInt32(limit),
	})
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// FindDelayedDueSoon scans retrying/scheduled rows due soon for reconciler step (b).
func (s *Store) FindDelayedDueSoon(ctx context.Context, staleAfter, lookahead time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.FindDelayedDueSoon(ctx, sqlc.FindDelayedDueSoonParams{
		StaleSecs:     staleAfter.Seconds(),
		LookaheadSecs: lookahead.Seconds(),
		AfterID:       afterID,
		BatchLimit:    clampInt32(limit),
	})
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// FindAbandonedProcessing scans processing rows past VISIBILITY_TIMEOUT for reconciler step (c).
func (s *Store) FindAbandonedProcessing(ctx context.Context, visibilityTimeout time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.FindAbandonedProcessing(ctx, sqlc.FindAbandonedProcessingParams{
		VisibilitySecs: visibilityTimeout.Seconds(),
		AfterID:        afterID,
		BatchLimit:     clampInt32(limit),
	})
	if err != nil {
		return nil, MapError(err)
	}
	return toDomainNotifications(rows), nil
}

// RequeueAbandoned resets an abandoned processing notification back to pending using fencing.
func (s *Store) RequeueAbandoned(ctx context.Context, id uuid.UUID, claimedAttempts int) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.RequeueAbandoned(ctx, sqlc.RequeueAbandonedParams{
		ID:              id,
		ClaimedAttempts: clampInt32(claimedAttempts),
	})
	if err != nil {
		return nil, mapFencedError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// FailAbandoned transitions an exhausted abandoned processing row to failed using fencing.
func (s *Store) FailAbandoned(ctx context.Context, id uuid.UUID, claimedAttempts int, lastError string) (*domain.Notification, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.FailAbandoned(ctx, sqlc.FailAbandonedParams{
		LastError:       &lastError,
		ID:              id,
		ClaimedAttempts: clampInt32(claimedAttempts),
	})
	if err != nil {
		return nil, mapFencedError(err)
	}
	out := toDomainNotification(row)
	return &out, nil
}

// InsertAttempt inserts an immutable attempt audit record.
func (s *Store) InsertAttempt(ctx context.Context, a domain.DeliveryAttempt) (*domain.DeliveryAttempt, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	var httpStatus *int32
	if a.HTTPStatus != nil {
		v := clampInt32(*a.HTTPStatus)
		httpStatus = &v
	}
	row, err := s.q.InsertAttempt(ctx, sqlc.InsertAttemptParams{
		NotificationID:    a.NotificationID,
		AttemptNo:         clampInt32(a.AttemptNo),
		Counted:           a.Counted,
		StartedAt:         a.StartedAt.UTC(),
		DurationMs:        clampInt32(a.DurationMS),
		HttpStatus:        httpStatus,
		Outcome:           a.Outcome.String(),
		Error:             a.Error,
		ProviderMessageID: a.ProviderMessageID,
		WorkerID:          a.WorkerID,
	})
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainAttempt(row)
	return &out, nil
}

// ListAttempts retrieves all attempt rows for notificationID in ascending attempt order.
func (s *Store) ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.ListAttempts(ctx, notificationID)
	if err != nil {
		return nil, MapError(err)
	}
	out := make([]domain.DeliveryAttempt, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDomainAttempt(r))
	}
	return out, nil
}

// InsertIdempotencyKey inserts a key or atomically overwrites an expired row with the same key.
// Returns (inserted, true, nil) on success, or (nil, false, nil) if an unexpired key already exists.
func (s *Store) InsertIdempotencyKey(ctx context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error) {
	if s.q == nil {
		return nil, false, errors.New("repository store is not initialized")
	}
	row, err := s.q.InsertIdempotencyKey(ctx, sqlc.InsertIdempotencyKeyParams{
		Key:            k.Key,
		RequestHash:    k.RequestHash,
		ResourceType:   string(k.ResourceType),
		ResourceID:     k.ResourceID,
		ResponseStatus: clampInt32(k.ResponseStatus),
		ExpiresAt:      k.ExpiresAt.UTC(),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, MapError(err)
	}
	out := toDomainIdempotencyKey(row)
	return &out, true, nil
}

// InsertIdempotencyKeys claims every key in one statement and returns the keys it claimed; a key that
// is missing from the result is still active for another resource (a conflict). All records must
// share ResourceType, ResponseStatus and ExpiresAt (true for the items of one batch).
func (s *Store) InsertIdempotencyKeys(ctx context.Context, keys []domain.IdempotencyKey) ([]string, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	if len(keys) == 0 {
		return nil, nil
	}
	params := sqlc.InsertIdempotencyKeysParams{
		ResourceType:   string(keys[0].ResourceType),
		ResponseStatus: clampInt32(keys[0].ResponseStatus),
		ExpiresAt:      keys[0].ExpiresAt.UTC(),
		Keys:           make([]string, len(keys)),
		RequestHashes:  make([]string, len(keys)),
		ResourceIds:    make([]uuid.UUID, len(keys)),
	}
	for i, k := range keys {
		if k.ResourceType != keys[0].ResourceType || k.ResponseStatus != keys[0].ResponseStatus || !k.ExpiresAt.Equal(keys[0].ExpiresAt) {
			return nil, errors.New("InsertIdempotencyKeys: records must share resource type, response status and expiry")
		}
		params.Keys[i], params.RequestHashes[i], params.ResourceIds[i] = k.Key, k.RequestHash, k.ResourceID
	}
	claimed, err := s.q.InsertIdempotencyKeys(ctx, params)
	if err != nil {
		return nil, MapError(err)
	}
	return claimed, nil
}

// GetIdempotencyKey returns an unexpired idempotency key record by key.
func (s *Store) GetIdempotencyKey(ctx context.Context, key string) (*domain.IdempotencyKey, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.GetIdempotencyKey(ctx, key)
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainIdempotencyKey(row)
	return &out, nil
}

// GetIdempotencyKeysByKeys returns all unexpired matching idempotency records for batch preflight checks.
func (s *Store) GetIdempotencyKeysByKeys(ctx context.Context, keys []string) ([]domain.IdempotencyKey, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.GetIdempotencyKeysByKeys(ctx, keys)
	if err != nil {
		return nil, MapError(err)
	}
	out := make([]domain.IdempotencyKey, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDomainIdempotencyKey(r))
	}
	return out, nil
}

// DeleteExpiredIdempotencyKeys deletes up to limit expired idempotency key rows.
func (s *Store) DeleteExpiredIdempotencyKeys(ctx context.Context, limit int) (int64, error) {
	if s.q == nil {
		return 0, errors.New("repository store is not initialized")
	}
	deleted, err := s.q.DeleteExpiredIdempotencyKeys(ctx, clampInt32(limit))
	if err != nil {
		return 0, MapError(err)
	}
	return deleted, nil
}

// MetricsWindowCounts computes per-channel 1m/5m/15m delivery counts from PostgreSQL.
func (s *Store) MetricsWindowCounts(ctx context.Context) ([]MetricsWindowRow, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	rows, err := s.q.MetricsWindowCounts(ctx)
	if err != nil {
		return nil, MapError(err)
	}
	out := make([]MetricsWindowRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, MetricsWindowRow{
			Window:        r.WindowName,
			Channel:       domain.Channel(r.Channel),
			SentCount:     r.SentCount,
			FailedCount:   r.FailedCount,
			RetryingCount: r.RetryingCount,
		})
	}
	return out, nil
}

// LatencyPercentiles computes 15-minute end-to-end and provider-call latency percentiles in ms.
func (s *Store) LatencyPercentiles(ctx context.Context) (LatencyStats, error) {
	if s.q == nil {
		return LatencyStats{}, errors.New("repository store is not initialized")
	}
	row, err := s.q.LatencyPercentiles(ctx)
	if err != nil {
		return LatencyStats{}, MapError(err)
	}
	return LatencyStats{
		EndToEndP50MS: row.E2eP50Ms,
		EndToEndP95MS: row.E2eP95Ms,
		EndToEndP99MS: row.E2eP99Ms,
		ProviderP50MS: row.ProviderP50Ms,
		ProviderP95MS: row.ProviderP95Ms,
		ProviderP99MS: row.ProviderP99Ms,
	}, nil
}

// InsertTemplate creates a new active message template row.
func (s *Store) InsertTemplate(ctx context.Context, t domain.Template) (*domain.Template, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	vars := t.Variables
	if vars == nil {
		vars = []string{}
	}
	row, err := s.q.InsertTemplate(ctx, sqlc.InsertTemplateParams{
		ID:        t.ID,
		Name:      t.Name,
		Channel:   t.Channel.String(),
		Subject:   t.Subject,
		Title:     t.Title,
		Body:      t.Body,
		Variables: vars,
	})
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainTemplate(row)
	return &out, nil
}

// GetTemplate retrieves an active (non-deleted) template by ID.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	row, err := s.q.GetTemplate(ctx, id)
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainTemplate(row)
	return &out, nil
}

// GetTemplatesByIDs retrieves all active templates matching ids.
func (s *Store) GetTemplatesByIDs(ctx context.Context, ids []uuid.UUID) ([]domain.Template, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	if len(ids) == 0 {
		return []domain.Template{}, nil
	}
	rows, err := s.q.GetTemplatesByIDs(ctx, ids)
	if err != nil {
		return nil, MapError(err)
	}
	out := make([]domain.Template, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDomainTemplate(r))
	}
	return out, nil
}

// ListTemplates returns keyset-paginated active templates optionally filtered by channel.
func (s *Store) ListTemplates(ctx context.Context, f TemplateListFilter) ([]domain.Template, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	var chPtr *string
	if f.Channel != nil {
		ch := f.Channel.String()
		chPtr = &ch
	}
	var cursorTS *time.Time
	var cursorID *uuid.UUID
	if f.Cursor != nil {
		u := f.Cursor.CreatedAt.UTC()
		cursorTS = &u
		idCopy := f.Cursor.ID
		cursorID = &idCopy
	}
	rows, err := s.q.ListTemplates(ctx, sqlc.ListTemplatesParams{
		Channel:         chPtr,
		CursorCreatedAt: cursorTS,
		CursorID:        cursorID,
		PageLimit:       clampInt32(f.Limit),
	})
	if err != nil {
		return nil, MapError(err)
	}
	out := make([]domain.Template, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDomainTemplate(r))
	}
	return out, nil
}

// UpdateTemplate updates an active template row by ID.
func (s *Store) UpdateTemplate(ctx context.Context, t domain.Template) (*domain.Template, error) {
	if s.q == nil {
		return nil, errors.New("repository store is not initialized")
	}
	vars := t.Variables
	if vars == nil {
		vars = []string{}
	}
	row, err := s.q.UpdateTemplate(ctx, sqlc.UpdateTemplateParams{
		ID:        t.ID,
		Name:      t.Name,
		Channel:   t.Channel.String(),
		Subject:   t.Subject,
		Title:     t.Title,
		Body:      t.Body,
		Variables: vars,
	})
	if err != nil {
		return nil, MapError(err)
	}
	out := toDomainTemplate(row)
	return &out, nil
}

// SoftDeleteTemplate marks an active template as deleted (deleted_at = now()).
func (s *Store) SoftDeleteTemplate(ctx context.Context, id uuid.UUID) error {
	if s.q == nil {
		return errors.New("repository store is not initialized")
	}
	_, err := s.q.SoftDeleteTemplate(ctx, id)
	return MapError(err)
}

func mapFencedError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrLeaseLost
	}
	return MapError(err)
}

func toDomainBatch(b sqlc.Batch) domain.Batch {
	return domain.Batch{
		ID:            b.ID,
		Total:         int(b.Total),
		CorrelationID: b.CorrelationID,
		CreatedAt:     b.CreatedAt.UTC(),
	}
}

func toDomainNotifications(rows []sqlc.Notification) []domain.Notification {
	out := make([]domain.Notification, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDomainNotification(r))
	}
	return out
}

func toDomainNotification(n sqlc.Notification) domain.Notification {
	return domain.Notification{
		ID:                  n.ID,
		BatchID:             n.BatchID,
		Channel:             domain.Channel(n.Channel),
		Priority:            domain.Priority(n.Priority),
		Status:              domain.Status(n.Status),
		Recipient:           n.Recipient,
		Subject:             n.Subject,
		Title:               n.Title,
		Content:             n.Content,
		IdempotencyKey:      n.IdempotencyKey,
		Attempts:            int(n.Attempts),
		MaxAttempts:         int(n.MaxAttempts),
		Deferrals:           int(n.Deferrals),
		ScheduledAt:         normalizeNullUTC(n.ScheduledAt),
		NextAttemptAt:       normalizeNullUTC(n.NextAttemptAt),
		ProcessingStartedAt: normalizeNullUTC(n.ProcessingStartedAt),
		LastError:           n.LastError,
		ProviderMessageID:   n.ProviderMessageID,
		CorrelationID:       n.CorrelationID,
		TraceParent:         n.Traceparent,
		TemplateID:          n.TemplateID,
		TemplateVars:        unmarshalTemplateVars(n.TemplateVars),
		CreatedAt:           n.CreatedAt.UTC(),
		UpdatedAt:           n.UpdatedAt.UTC(),
		SentAt:              normalizeNullUTC(n.SentAt),
		FailedAt:            normalizeNullUTC(n.FailedAt),
		CancelledAt:         normalizeNullUTC(n.CancelledAt),
	}
}

func toDomainTemplate(t sqlc.Template) domain.Template {
	vars := make([]string, len(t.Variables))
	copy(vars, t.Variables)
	return domain.Template{
		ID:        t.ID,
		Name:      t.Name,
		Channel:   domain.Channel(t.Channel),
		Subject:   t.Subject,
		Title:     t.Title,
		Body:      t.Body,
		Variables: vars,
		CreatedAt: t.CreatedAt.UTC(),
		UpdatedAt: t.UpdatedAt.UTC(),
		DeletedAt: normalizeNullUTC(t.DeletedAt),
	}
}

func marshalTemplateVars(vars map[string]any) ([]byte, error) {
	if len(vars) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(vars)
	if err != nil {
		return nil, fmt.Errorf("marshaling template variables: %w", err)
	}
	return raw, nil
}

func unmarshalTemplateVars(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func toDomainAttempt(a sqlc.DeliveryAttempt) domain.DeliveryAttempt {
	var httpStatus *int
	if a.HttpStatus != nil {
		v := int(*a.HttpStatus)
		httpStatus = &v
	}
	return domain.DeliveryAttempt{
		ID:                a.ID,
		NotificationID:    a.NotificationID,
		AttemptNo:         int(a.AttemptNo),
		Counted:           a.Counted,
		StartedAt:         a.StartedAt.UTC(),
		DurationMS:        int(a.DurationMs),
		HTTPStatus:        httpStatus,
		Outcome:           domain.AttemptOutcome(a.Outcome),
		Error:             a.Error,
		ProviderMessageID: a.ProviderMessageID,
		WorkerID:          a.WorkerID,
	}
}

func toDomainIdempotencyKey(k sqlc.IdempotencyKey) domain.IdempotencyKey {
	return domain.IdempotencyKey{
		Key:            k.Key,
		RequestHash:    k.RequestHash,
		ResourceType:   domain.ResourceType(k.ResourceType),
		ResourceID:     k.ResourceID,
		ResponseStatus: int(k.ResponseStatus),
		CreatedAt:      k.CreatedAt.UTC(),
		ExpiresAt:      k.ExpiresAt.UTC(),
	}
}

func normalizeNullUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func clampInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	return int32(n)
}
