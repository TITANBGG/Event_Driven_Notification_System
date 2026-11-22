package api_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository"
)

type fakeDBState struct {
	notifications map[uuid.UUID]domain.Notification
	batches       map[uuid.UUID]domain.Batch
	attempts      map[uuid.UUID][]domain.DeliveryAttempt
	idempotency   map[string]domain.IdempotencyKey
	templates     map[uuid.UUID]domain.Template
}

func newFakeDBState() *fakeDBState {
	return &fakeDBState{
		notifications: make(map[uuid.UUID]domain.Notification),
		batches:       make(map[uuid.UUID]domain.Batch),
		attempts:      make(map[uuid.UUID][]domain.DeliveryAttempt),
		idempotency:   make(map[string]domain.IdempotencyKey),
		templates:     make(map[uuid.UUID]domain.Template),
	}
}

func (s *fakeDBState) clone() *fakeDBState {
	cp := newFakeDBState()
	for k, v := range s.notifications {
		cp.notifications[k] = v
	}
	for k, v := range s.batches {
		cp.batches[k] = v
	}
	for k, v := range s.attempts {
		list := make([]domain.DeliveryAttempt, len(v))
		copy(list, v)
		cp.attempts[k] = list
	}
	for k, v := range s.idempotency {
		cp.idempotency[k] = v
	}
	for k, v := range s.templates {
		cp.templates[k] = v
	}
	return cp
}

// fakeStore is an in-memory transactional Store implementing Postgres semantics
// (atomic rollback in WithTx, expired idempotency key reclaim, fenced transitions, keyset pagination).
type fakeStore struct {
	txMu         sync.Mutex
	mu           sync.RWMutex
	now          func() time.Time
	seq          time.Time
	forcedErr    error
	pingErr      error
	metricsCalls int
	// bulkKeyInserts counts InsertIdempotencyKeys calls (one per batch, not one per item).
	bulkKeyInserts int
	st             *fakeDBState
}

func (f *fakeStore) SetForcedError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forcedErr = err
}

func (f *fakeStore) SetPingError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pingErr = err
}

func (f *fakeStore) BulkKeyInserts() int {
	f.txMu.Lock()
	defer f.txMu.Unlock()
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.bulkKeyInserts
}

func (f *fakeStore) MetricsCalls() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.metricsCalls
}

func newFakeStore() *fakeStore {
	base := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	return &fakeStore{
		now: func() time.Time { return time.Now().UTC() },
		seq: base,
		st:  newFakeDBState(),
	}
}

func (f *fakeStore) Ping(_ context.Context) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pingErr
}

func (f *fakeStore) WithTx(ctx context.Context, fn func(ctx context.Context, tx api.Store) error) error {
	f.mu.RLock()
	ferr := f.forcedErr
	f.mu.RUnlock()
	if ferr != nil {
		return ferr
	}

	// txMu serializes write transactions cleanly just like Postgres row/PK locks.
	f.txMu.Lock()
	defer f.txMu.Unlock()

	f.mu.RLock()
	working := f.st.clone()
	f.mu.RUnlock()

	txView := &fakeTxStore{
		root: f,
		st:   working,
	}
	if err := fn(ctx, txView); err != nil {
		return err
	}

	f.mu.Lock()
	f.st = working
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) nextStamp() time.Time {
	return f.now().UTC()
}

func (f *fakeStore) InsertBatch(_ context.Context, b domain.Batch) (*domain.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).insertBatchLocked(b)
}

func (f *fakeStore) GetBatch(_ context.Context, id uuid.UUID) (*domain.Batch, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).getBatchLocked(id)
}

func (f *fakeStore) GetBatchSummary(_ context.Context, batchID uuid.UUID) (map[domain.Status]int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).getBatchSummaryLocked(batchID)
}

func (f *fakeStore) ListBatchItems(_ context.Context, batchID uuid.UUID, cursor *repository.Cursor, limit int) ([]domain.Notification, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).listBatchItemsLocked(batchID, cursor, limit)
}

func (f *fakeStore) CancelBatch(_ context.Context, batchID uuid.UUID) ([]domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).cancelBatchLocked(batchID)
}

func (f *fakeStore) InsertNotification(_ context.Context, n domain.Notification) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).insertNotificationLocked(n)
}

func (f *fakeStore) CopyNotifications(_ context.Context, items []domain.Notification) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).copyNotificationsLocked(items)
}

func (f *fakeStore) GetNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.forcedErr != nil {
		return nil, f.forcedErr
	}
	return (&fakeTxStore{root: f, st: f.st}).getNotificationLocked(id)
}

func (f *fakeStore) ListNotifications(_ context.Context, filter repository.ListFilter) ([]domain.Notification, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).listNotificationsLocked(filter)
}

func (f *fakeStore) Cancel(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).cancelLocked(id)
}

func (f *fakeStore) ManualRetry(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).manualRetryLocked(id)
}

func (f *fakeStore) ManualRetryFailedBulk(_ context.Context, filter repository.RetryFailedFilter) ([]domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).manualRetryFailedBulkLocked(filter)
}

func (f *fakeStore) InsertIdempotencyKeys(_ context.Context, keys []domain.IdempotencyKey) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bulkKeyInserts++
	return (&fakeTxStore{root: f, st: f.st}).insertIdempotencyKeysLocked(keys)
}

func (f *fakeStore) ListAttempts(_ context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).listAttemptsLocked(notificationID)
}

func (f *fakeStore) InsertIdempotencyKey(_ context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).insertIdempotencyKeyLocked(k)
}

func (f *fakeStore) GetIdempotencyKey(_ context.Context, key string) (*domain.IdempotencyKey, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).getIdempotencyKeyLocked(key)
}

func (f *fakeStore) GetIdempotencyKeysByKeys(_ context.Context, keys []string) ([]domain.IdempotencyKey, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).getIdempotencyKeysByKeysLocked(keys)
}

func (f *fakeStore) InsertTemplate(_ context.Context, tmpl domain.Template) (*domain.Template, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).insertTemplateLocked(tmpl)
}

func (f *fakeStore) GetTemplate(_ context.Context, id uuid.UUID) (*domain.Template, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).getTemplateLocked(id)
}

func (f *fakeStore) GetTemplatesByIDs(_ context.Context, ids []uuid.UUID) ([]domain.Template, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).getTemplatesByIDsLocked(ids)
}

func (f *fakeStore) ListTemplates(_ context.Context, filter repository.TemplateListFilter) ([]domain.Template, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return (&fakeTxStore{root: f, st: f.st}).listTemplatesLocked(filter)
}

func (f *fakeStore) UpdateTemplate(_ context.Context, tmpl domain.Template) (*domain.Template, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).updateTemplateLocked(tmpl)
}

func (f *fakeStore) SoftDeleteTemplate(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return (&fakeTxStore{root: f, st: f.st}).softDeleteTemplateLocked(id)
}

func (f *fakeStore) MetricsWindowCounts(_ context.Context) ([]repository.MetricsWindowRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forcedErr != nil {
		return nil, f.forcedErr
	}
	f.metricsCalls++
	return (&fakeTxStore{root: f, st: f.st}).metricsWindowCountsLocked()
}

func (f *fakeStore) LatencyPercentiles(_ context.Context) (repository.LatencyStats, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.forcedErr != nil {
		return repository.LatencyStats{}, f.forcedErr
	}
	return (&fakeTxStore{root: f, st: f.st}).latencyPercentilesLocked()
}

// SeedNotification inserts or overwrites a notification row directly for test setup.
func (f *fakeStore) SeedNotification(n domain.Notification) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n.CreatedAt.IsZero() {
		n.CreatedAt = f.now().UTC()
	}
	if n.UpdatedAt.IsZero() {
		n.UpdatedAt = n.CreatedAt
	}
	f.st.notifications[n.ID] = n
}

func (f *fakeStore) SeedAttempt(a domain.DeliveryAttempt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st.attempts[a.NotificationID] = append(f.st.attempts[a.NotificationID], a)
}

func (f *fakeStore) SeedIdempotencyKey(k domain.IdempotencyKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st.idempotency[k.Key] = k
}

func (f *fakeStore) NotificationCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.st.notifications)
}

type fakeTxStore struct {
	root *fakeStore
	st   *fakeDBState
}

func (t *fakeTxStore) Ping(_ context.Context) error { return nil }
func (t *fakeTxStore) WithTx(ctx context.Context, fn func(ctx context.Context, tx api.Store) error) error {
	return fn(ctx, t)
}

func (t *fakeTxStore) InsertBatch(_ context.Context, b domain.Batch) (*domain.Batch, error) {
	return t.insertBatchLocked(b)
}
func (t *fakeTxStore) GetBatch(_ context.Context, id uuid.UUID) (*domain.Batch, error) {
	return t.getBatchLocked(id)
}
func (t *fakeTxStore) GetBatchSummary(_ context.Context, batchID uuid.UUID) (map[domain.Status]int64, error) {
	return t.getBatchSummaryLocked(batchID)
}
func (t *fakeTxStore) ListBatchItems(_ context.Context, batchID uuid.UUID, cursor *repository.Cursor, limit int) ([]domain.Notification, error) {
	return t.listBatchItemsLocked(batchID, cursor, limit)
}
func (t *fakeTxStore) CancelBatch(_ context.Context, batchID uuid.UUID) ([]domain.Notification, error) {
	return t.cancelBatchLocked(batchID)
}
func (t *fakeTxStore) InsertNotification(_ context.Context, n domain.Notification) (*domain.Notification, error) {
	return t.insertNotificationLocked(n)
}
func (t *fakeTxStore) CopyNotifications(_ context.Context, items []domain.Notification) (int64, error) {
	return t.copyNotificationsLocked(items)
}
func (t *fakeTxStore) GetNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	return t.getNotificationLocked(id)
}
func (t *fakeTxStore) ListNotifications(_ context.Context, filter repository.ListFilter) ([]domain.Notification, error) {
	return t.listNotificationsLocked(filter)
}
func (t *fakeTxStore) Cancel(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	return t.cancelLocked(id)
}
func (t *fakeTxStore) ManualRetry(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	return t.manualRetryLocked(id)
}
func (t *fakeTxStore) ManualRetryFailedBulk(_ context.Context, filter repository.RetryFailedFilter) ([]domain.Notification, error) {
	return t.manualRetryFailedBulkLocked(filter)
}
func (t *fakeTxStore) InsertIdempotencyKeys(_ context.Context, keys []domain.IdempotencyKey) ([]string, error) {
	t.root.bulkKeyInserts++
	return t.insertIdempotencyKeysLocked(keys)
}
func (t *fakeTxStore) ListAttempts(_ context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	return t.listAttemptsLocked(notificationID)
}
func (t *fakeTxStore) InsertIdempotencyKey(_ context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error) {
	return t.insertIdempotencyKeyLocked(k)
}
func (t *fakeTxStore) GetIdempotencyKey(_ context.Context, key string) (*domain.IdempotencyKey, error) {
	return t.getIdempotencyKeyLocked(key)
}
func (t *fakeTxStore) GetIdempotencyKeysByKeys(_ context.Context, keys []string) ([]domain.IdempotencyKey, error) {
	return t.getIdempotencyKeysByKeysLocked(keys)
}
func (t *fakeTxStore) InsertTemplate(_ context.Context, tmpl domain.Template) (*domain.Template, error) {
	return t.insertTemplateLocked(tmpl)
}
func (t *fakeTxStore) GetTemplate(_ context.Context, id uuid.UUID) (*domain.Template, error) {
	return t.getTemplateLocked(id)
}
func (t *fakeTxStore) GetTemplatesByIDs(_ context.Context, ids []uuid.UUID) ([]domain.Template, error) {
	return t.getTemplatesByIDsLocked(ids)
}
func (t *fakeTxStore) ListTemplates(_ context.Context, filter repository.TemplateListFilter) ([]domain.Template, error) {
	return t.listTemplatesLocked(filter)
}
func (t *fakeTxStore) UpdateTemplate(_ context.Context, tmpl domain.Template) (*domain.Template, error) {
	return t.updateTemplateLocked(tmpl)
}
func (t *fakeTxStore) SoftDeleteTemplate(_ context.Context, id uuid.UUID) error {
	return t.softDeleteTemplateLocked(id)
}
func (t *fakeTxStore) MetricsWindowCounts(_ context.Context) ([]repository.MetricsWindowRow, error) {
	return t.metricsWindowCountsLocked()
}
func (t *fakeTxStore) LatencyPercentiles(_ context.Context) (repository.LatencyStats, error) {
	return t.latencyPercentilesLocked()
}

func (t *fakeTxStore) insertBatchLocked(b domain.Batch) (*domain.Batch, error) {
	if _, exists := t.st.batches[b.ID]; exists {
		return nil, fmt.Errorf("%w: batches_pkey", domain.ErrDuplicateKey)
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = t.root.nextStamp()
	}
	t.st.batches[b.ID] = b
	cp := b
	return &cp, nil
}

func (t *fakeTxStore) getBatchLocked(id uuid.UUID) (*domain.Batch, error) {
	b, ok := t.st.batches[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := b
	return &cp, nil
}

func (t *fakeTxStore) getBatchSummaryLocked(batchID uuid.UUID) (map[domain.Status]int64, error) {
	out := make(map[domain.Status]int64)
	for _, n := range t.st.notifications {
		if n.BatchID != nil && *n.BatchID == batchID {
			out[n.Status]++
		}
	}
	return out, nil
}

func (t *fakeTxStore) listBatchItemsLocked(batchID uuid.UUID, cursor *repository.Cursor, limit int) ([]domain.Notification, error) {
	items := make([]domain.Notification, 0)
	for _, n := range t.st.notifications {
		if n.BatchID == nil || *n.BatchID != batchID {
			continue
		}
		if cursor != nil && !beforeCursor(n, *cursor) {
			continue
		}
		items = append(items, n)
	}
	sortNotificationsDesc(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (t *fakeTxStore) cancelBatchLocked(batchID uuid.UUID) ([]domain.Notification, error) {
	now := t.root.nextStamp()
	var out []domain.Notification
	for id, n := range t.st.notifications {
		if n.BatchID != nil && *n.BatchID == batchID && domain.IsCancellable(n.Status) {
			n.Status = domain.StatusCancelled
			n.CancelledAt = &now
			n.UpdatedAt = now
			t.st.notifications[id] = n
			out = append(out, n)
		}
	}
	return out, nil
}

func (t *fakeTxStore) manualRetryFailedBulkLocked(f repository.RetryFailedFilter) ([]domain.Notification, error) {
	var candidates []domain.Notification
	for _, n := range t.st.notifications {
		if n.Status != domain.StatusFailed || (f.Channel != nil && n.Channel != *f.Channel) {
			continue
		}
		if n.FailedAt != nil && ((f.FailedFrom != nil && n.FailedAt.Before(*f.FailedFrom)) || (f.FailedTo != nil && !n.FailedAt.Before(*f.FailedTo))) {
			continue
		}
		candidates = append(candidates, n)
	}
	sort.Slice(candidates, func(i, j int) bool { return bytes.Compare(candidates[i].ID[:], candidates[j].ID[:]) < 0 })
	if len(candidates) > f.Limit {
		candidates = candidates[:f.Limit]
	}
	out := make([]domain.Notification, 0, len(candidates))
	for _, n := range candidates {
		updated, err := t.manualRetryLocked(n.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, *updated)
	}
	return out, nil
}

func (t *fakeTxStore) insertIdempotencyKeysLocked(keys []domain.IdempotencyKey) ([]string, error) {
	claimed := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok, err := t.insertIdempotencyKeyLocked(k); err != nil {
			return nil, err
		} else if ok {
			claimed = append(claimed, k.Key)
		}
	}
	return claimed, nil
}

func (t *fakeTxStore) insertNotificationLocked(n domain.Notification) (*domain.Notification, error) {
	if _, exists := t.st.notifications[n.ID]; exists {
		return nil, fmt.Errorf("%w: notifications_pkey", domain.ErrDuplicateKey)
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = t.root.nextStamp()
	}
	if n.UpdatedAt.IsZero() {
		n.UpdatedAt = n.CreatedAt
	}
	t.st.notifications[n.ID] = n
	cp := n
	return &cp, nil
}

func (t *fakeTxStore) copyNotificationsLocked(items []domain.Notification) (int64, error) {
	now := t.root.nextStamp()
	for _, n := range items {
		if n.CreatedAt.IsZero() {
			n.CreatedAt = now
		}
		if n.UpdatedAt.IsZero() {
			n.UpdatedAt = now
		}
		t.st.notifications[n.ID] = n
	}
	return int64(len(items)), nil
}

func (t *fakeTxStore) getNotificationLocked(id uuid.UUID) (*domain.Notification, error) {
	n, ok := t.st.notifications[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := n
	return &cp, nil
}

func (t *fakeTxStore) listNotificationsLocked(f repository.ListFilter) ([]domain.Notification, error) {
	items := make([]domain.Notification, 0)
	for _, n := range t.st.notifications {
		if len(f.Statuses) > 0 {
			matched := false
			for _, st := range f.Statuses {
				if n.Status == st {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if f.Channel != nil && n.Channel != *f.Channel {
			continue
		}
		if f.Priority != nil && n.Priority != *f.Priority {
			continue
		}
		if f.From != nil && n.CreatedAt.Before(f.From.UTC()) {
			continue
		}
		if f.To != nil && !n.CreatedAt.Before(f.To.UTC()) {
			continue
		}
		if f.Cursor != nil && !beforeCursor(n, *f.Cursor) {
			continue
		}
		items = append(items, n)
	}
	sortNotificationsDesc(items)
	if f.Limit > 0 && len(items) > f.Limit {
		items = items[:f.Limit]
	}
	return items, nil
}

func (t *fakeTxStore) cancelLocked(id uuid.UUID) (*domain.Notification, error) {
	n, ok := t.st.notifications[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if !domain.CanTransition(n.Status, domain.StatusCancelled) {
		return nil, domain.ErrInvalidTransition
	}
	now := t.root.nextStamp()
	n.Status = domain.StatusCancelled
	n.CancelledAt = &now
	n.UpdatedAt = now
	t.st.notifications[id] = n
	cp := n
	return &cp, nil
}

func (t *fakeTxStore) manualRetryLocked(id uuid.UUID) (*domain.Notification, error) {
	n, ok := t.st.notifications[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if n.Status != domain.StatusFailed {
		return nil, domain.ErrInvalidTransition
	}
	now := t.root.nextStamp()
	n.Status = domain.StatusPending
	n.Attempts = 0
	n.Deferrals = 0
	n.LastError = nil
	n.FailedAt = nil
	n.NextAttemptAt = nil
	n.UpdatedAt = now
	t.st.notifications[id] = n
	cp := n
	return &cp, nil
}

func (t *fakeTxStore) listAttemptsLocked(notificationID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	src := t.st.attempts[notificationID]
	out := make([]domain.DeliveryAttempt, len(src))
	copy(out, src)
	sort.Slice(out, func(i, j int) bool {
		return out[i].AttemptNo < out[j].AttemptNo
	})
	return out, nil
}

func (t *fakeTxStore) insertIdempotencyKeyLocked(k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error) {
	now := t.root.now().UTC()
	if cur, exists := t.st.idempotency[k.Key]; exists && cur.ExpiresAt.After(now) {
		return nil, false, nil
	}
	k.CreatedAt = now
	t.st.idempotency[k.Key] = k
	cp := k
	return &cp, true, nil
}

func (t *fakeTxStore) getIdempotencyKeyLocked(key string) (*domain.IdempotencyKey, error) {
	now := t.root.now().UTC()
	k, ok := t.st.idempotency[key]
	if !ok || !k.ExpiresAt.After(now) {
		return nil, domain.ErrNotFound
	}
	cp := k
	return &cp, nil
}

func (t *fakeTxStore) getIdempotencyKeysByKeysLocked(keys []string) ([]domain.IdempotencyKey, error) {
	now := t.root.now().UTC()
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	var out []domain.IdempotencyKey
	for _, k := range t.st.idempotency {
		if _, ok := set[k.Key]; ok && k.ExpiresAt.After(now) {
			out = append(out, k)
		}
	}
	return out, nil
}

func sortNotificationsDesc(items []domain.Notification) {
	sort.Slice(items, func(i, j int) bool {
		ti, tj := items[i].CreatedAt.UTC(), items[j].CreatedAt.UTC()
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return bytes.Compare(items[i].ID[:], items[j].ID[:]) > 0
	})
}

func (t *fakeTxStore) metricsWindowCountsLocked() ([]repository.MetricsWindowRow, error) {
	now := t.root.now().UTC()
	windows := []struct {
		name   string
		cutoff time.Time
	}{
		{name: "1m", cutoff: now.Add(-time.Minute)},
		{name: "5m", cutoff: now.Add(-5 * time.Minute)},
		{name: "15m", cutoff: now.Add(-15 * time.Minute)},
	}
	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	out := make([]repository.MetricsWindowRow, 0, len(windows)*len(channels))
	for _, w := range windows {
		for _, ch := range channels {
			row := repository.MetricsWindowRow{Window: w.name, Channel: ch}
			for _, n := range t.st.notifications {
				if n.Channel != ch {
					continue
				}
				switch n.Status {
				case domain.StatusSent:
					if n.SentAt != nil && !n.SentAt.Before(w.cutoff) {
						row.SentCount++
					}
				case domain.StatusFailed:
					if n.FailedAt != nil && !n.FailedAt.Before(w.cutoff) {
						row.FailedCount++
					}
				case domain.StatusRetrying:
					row.RetryingCount++
				}
			}
			out = append(out, row)
		}
	}
	return out, nil
}

func (t *fakeTxStore) latencyPercentilesLocked() (repository.LatencyStats, error) {
	cutoff := t.root.now().UTC().Add(-15 * time.Minute)
	var e2e []float64
	for _, n := range t.st.notifications {
		if n.Status == domain.StatusSent && n.SentAt != nil && !n.SentAt.Before(cutoff) {
			ms := n.SentAt.Sub(n.CreatedAt).Seconds() * 1000.0
			if ms < 0 {
				ms = 0
			}
			e2e = append(e2e, ms)
		}
	}
	var prov []float64
	for _, attempts := range t.st.attempts {
		for _, a := range attempts {
			if a.Counted && !a.StartedAt.Before(cutoff) {
				prov = append(prov, float64(a.DurationMS))
			}
		}
	}
	return repository.LatencyStats{
		EndToEndP50MS: percentileFloat(e2e, 0.50),
		EndToEndP95MS: percentileFloat(e2e, 0.95),
		EndToEndP99MS: percentileFloat(e2e, 0.99),
		ProviderP50MS: percentileFloat(prov, 0.50),
		ProviderP95MS: percentileFloat(prov, 0.95),
		ProviderP99MS: percentileFloat(prov, 0.99),
	}, nil
}

func percentileFloat(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)
	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func beforeCursor(n domain.Notification, c repository.Cursor) bool {
	tn, tc := n.CreatedAt.UTC(), c.CreatedAt.UTC()
	if tn.Before(tc) {
		return true
	}
	if tn.After(tc) {
		return false
	}
	return bytes.Compare(n.ID[:], c.ID[:]) < 0
}

func (t *fakeTxStore) insertTemplateLocked(tmpl domain.Template) (*domain.Template, error) {
	for _, existing := range t.st.templates {
		if existing.DeletedAt == nil && existing.Name == tmpl.Name {
			return nil, fmt.Errorf("%w: templates_name_active", domain.ErrDuplicateKey)
		}
	}
	now := t.root.nextStamp()
	if tmpl.CreatedAt.IsZero() {
		tmpl.CreatedAt = now
	}
	if tmpl.UpdatedAt.IsZero() {
		tmpl.UpdatedAt = tmpl.CreatedAt
	}
	t.st.templates[tmpl.ID] = tmpl
	cp := tmpl
	return &cp, nil
}

func (t *fakeTxStore) getTemplateLocked(id uuid.UUID) (*domain.Template, error) {
	tmpl, ok := t.st.templates[id]
	if !ok || tmpl.DeletedAt != nil {
		return nil, domain.ErrNotFound
	}
	cp := tmpl
	return &cp, nil
}

func (t *fakeTxStore) getTemplatesByIDsLocked(ids []uuid.UUID) ([]domain.Template, error) {
	set := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	out := make([]domain.Template, 0)
	for _, tmpl := range t.st.templates {
		if _, ok := set[tmpl.ID]; ok && tmpl.DeletedAt == nil {
			out = append(out, tmpl)
		}
	}
	return out, nil
}

func (t *fakeTxStore) listTemplatesLocked(f repository.TemplateListFilter) ([]domain.Template, error) {
	items := make([]domain.Template, 0)
	for _, tmpl := range t.st.templates {
		if tmpl.DeletedAt != nil {
			continue
		}
		if f.Channel != nil && tmpl.Channel != *f.Channel {
			continue
		}
		if f.Cursor != nil {
			tn, tc := tmpl.CreatedAt.UTC(), f.Cursor.CreatedAt.UTC()
			if tn.After(tc) || (tn.Equal(tc) && bytes.Compare(tmpl.ID[:], f.Cursor.ID[:]) >= 0) {
				continue
			}
		}
		items = append(items, tmpl)
	}
	sort.Slice(items, func(i, j int) bool {
		ti, tj := items[i].CreatedAt.UTC(), items[j].CreatedAt.UTC()
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return bytes.Compare(items[i].ID[:], items[j].ID[:]) > 0
	})
	if f.Limit > 0 && len(items) > f.Limit {
		items = items[:f.Limit]
	}
	return items, nil
}

func (t *fakeTxStore) updateTemplateLocked(tmpl domain.Template) (*domain.Template, error) {
	cur, ok := t.st.templates[tmpl.ID]
	if !ok || cur.DeletedAt != nil {
		return nil, domain.ErrNotFound
	}
	for _, existing := range t.st.templates {
		if existing.ID != tmpl.ID && existing.DeletedAt == nil && existing.Name == tmpl.Name {
			return nil, fmt.Errorf("%w: templates_name_active", domain.ErrDuplicateKey)
		}
	}
	now := t.root.nextStamp()
	cur.Name = tmpl.Name
	cur.Channel = tmpl.Channel
	cur.Subject = tmpl.Subject
	cur.Title = tmpl.Title
	cur.Body = tmpl.Body
	cur.Variables = tmpl.Variables
	cur.UpdatedAt = now
	t.st.templates[tmpl.ID] = cur
	cp := cur
	return &cp, nil
}

func (t *fakeTxStore) softDeleteTemplateLocked(id uuid.UUID) error {
	cur, ok := t.st.templates[id]
	if !ok || cur.DeletedAt != nil {
		return domain.ErrNotFound
	}
	now := t.root.nextStamp()
	cur.DeletedAt = &now
	cur.UpdatedAt = now
	t.st.templates[id] = cur
	return nil
}
