package app_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/app"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository"
)

// memStore is a minimal transactional store: WithTx runs on a copy and commits it only on success,
// and one mutex serializes transactions the way the idempotency primary key does in PostgreSQL.
type memStore struct {
	mu      sync.Mutex
	now     time.Time
	keys    map[string]domain.IdempotencyKey
	notifs  map[uuid.UUID]domain.Notification
	batches map[uuid.UUID]domain.Batch
}

func newMemStore(now time.Time) *memStore {
	return &memStore{now: now, keys: map[string]domain.IdempotencyKey{}, notifs: map[uuid.UUID]domain.Notification{}, batches: map[uuid.UUID]domain.Batch{}}
}

type memTx struct{ s *memStore }

func (s *memStore) WithTx(ctx context.Context, fn func(ctx context.Context, tx app.Store) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := &memStore{now: s.now, keys: map[string]domain.IdempotencyKey{}, notifs: map[uuid.UUID]domain.Notification{}, batches: map[uuid.UUID]domain.Batch{}}
	for k, v := range s.keys {
		snapshot.keys[k] = v
	}
	for k, v := range s.notifs {
		snapshot.notifs[k] = v
	}
	for k, v := range s.batches {
		snapshot.batches[k] = v
	}
	if err := fn(ctx, memTx{snapshot}); err != nil {
		return err // rollback: the snapshot is dropped
	}
	s.keys, s.notifs, s.batches = snapshot.keys, snapshot.notifs, snapshot.batches
	return nil
}

func (t memTx) WithTx(context.Context, func(context.Context, app.Store) error) error {
	return errors.New("nested transaction")
}

func (s *memStore) locked(fn func(*memStore)) { s.mu.Lock(); defer s.mu.Unlock(); fn(s) }

func insertKey(s *memStore, k domain.IdempotencyKey) bool {
	if cur, ok := s.keys[k.Key]; ok && cur.ExpiresAt.After(s.now) {
		return false
	}
	s.keys[k.Key] = k
	return true
}

func (t memTx) InsertIdempotencyKey(_ context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error) {
	return &k, insertKey(t.s, k), nil
}

func (t memTx) InsertIdempotencyKeys(_ context.Context, keys []domain.IdempotencyKey) ([]string, error) {
	var claimed []string
	for _, k := range keys {
		if insertKey(t.s, k) {
			claimed = append(claimed, k.Key)
		}
	}
	return claimed, nil
}

func getKey(s *memStore, key string) (*domain.IdempotencyKey, error) {
	k, ok := s.keys[key]
	if !ok || !k.ExpiresAt.After(s.now) {
		return nil, domain.ErrNotFound
	}
	return &k, nil
}

func (t memTx) GetIdempotencyKey(_ context.Context, key string) (*domain.IdempotencyKey, error) {
	return getKey(t.s, key)
}

func (t memTx) GetIdempotencyKeysByKeys(_ context.Context, keys []string) ([]domain.IdempotencyKey, error) {
	var out []domain.IdempotencyKey
	for _, key := range keys {
		if k, err := getKey(t.s, key); err == nil {
			out = append(out, *k)
		}
	}
	return out, nil
}

func (t memTx) InsertNotification(_ context.Context, n domain.Notification) (*domain.Notification, error) {
	t.s.notifs[n.ID] = n
	return &n, nil
}

func (t memTx) CopyNotifications(_ context.Context, items []domain.Notification) (int64, error) {
	for _, n := range items {
		t.s.notifs[n.ID] = n
	}
	return int64(len(items)), nil
}

func (t memTx) GetNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, ok := t.s.notifs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &n, nil
}

func (t memTx) InsertBatch(_ context.Context, b domain.Batch) (*domain.Batch, error) {
	t.s.batches[b.ID] = b
	return &b, nil
}

func (t memTx) GetBatch(_ context.Context, id uuid.UUID) (*domain.Batch, error) {
	b, ok := t.s.batches[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &b, nil
}

func (t memTx) ListBatchItems(_ context.Context, batchID uuid.UUID, cursor *repository.Cursor, limit int) ([]domain.Notification, error) {
	var items []domain.Notification
	for _, n := range t.s.notifs {
		if n.BatchID != nil && *n.BatchID == batchID && (cursor == nil || n.ID.String() < cursor.ID.String()) {
			items = append(items, n)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID.String() > items[j].ID.String() })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

// Non-transactional reads on the store itself.
func (s *memStore) InsertIdempotencyKey(ctx context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error) {
	var (
		out *domain.IdempotencyKey
		ok  bool
	)
	s.locked(func(s *memStore) { out, ok, _ = memTx{s}.InsertIdempotencyKey(ctx, k) })
	return out, ok, nil
}

func (s *memStore) InsertIdempotencyKeys(ctx context.Context, keys []domain.IdempotencyKey) ([]string, error) {
	var out []string
	s.locked(func(s *memStore) { out, _ = memTx{s}.InsertIdempotencyKeys(ctx, keys) })
	return out, nil
}

func (s *memStore) GetIdempotencyKey(_ context.Context, key string) (k *domain.IdempotencyKey, err error) {
	s.locked(func(s *memStore) { k, err = getKey(s, key) })
	return k, err
}

func (s *memStore) GetIdempotencyKeysByKeys(ctx context.Context, keys []string) (out []domain.IdempotencyKey, err error) {
	s.locked(func(s *memStore) { out, err = memTx{s}.GetIdempotencyKeysByKeys(ctx, keys) })
	return out, err
}

func (s *memStore) InsertNotification(ctx context.Context, n domain.Notification) (out *domain.Notification, err error) {
	s.locked(func(s *memStore) { out, err = memTx{s}.InsertNotification(ctx, n) })
	return out, err
}

func (s *memStore) CopyNotifications(ctx context.Context, items []domain.Notification) (n int64, err error) {
	s.locked(func(s *memStore) { n, err = memTx{s}.CopyNotifications(ctx, items) })
	return n, err
}

func (s *memStore) GetNotification(ctx context.Context, id uuid.UUID) (out *domain.Notification, err error) {
	s.locked(func(s *memStore) { out, err = memTx{s}.GetNotification(ctx, id) })
	return out, err
}

func (s *memStore) InsertBatch(ctx context.Context, b domain.Batch) (out *domain.Batch, err error) {
	s.locked(func(s *memStore) { out, err = memTx{s}.InsertBatch(ctx, b) })
	return out, err
}

func (s *memStore) GetBatch(ctx context.Context, id uuid.UUID) (out *domain.Batch, err error) {
	s.locked(func(s *memStore) { out, err = memTx{s}.GetBatch(ctx, id) })
	return out, err
}

func (s *memStore) ListBatchItems(ctx context.Context, batchID uuid.UUID, cursor *repository.Cursor, limit int) (out []domain.Notification, err error) {
	s.locked(func(s *memStore) { out, err = memTx{s}.ListBatchItems(ctx, batchID, cursor, limit) })
	return out, err
}

var (
	_  app.Store = (*memStore)(nil)
	_  app.Store = memTx{}
	t0           = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
)

func notif() domain.Notification {
	return domain.Notification{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Status: domain.StatusPending}
}

func TestCreateReplaysSameKeyAndRejectsDifferentBody(t *testing.T) {
	t.Parallel()
	svc := app.NewNotifications(newMemStore(t0), 24*time.Hour, 1000)

	first, err := svc.Create(t.Context(), notif(), "order-42", "hash-a", t0)
	require.NoError(t, err)
	assert.False(t, first.Replayed)
	assert.Equal(t, app.AcceptedStatus, first.Status)

	again, err := svc.Create(t.Context(), notif(), "order-42", "hash-a", t0)
	require.NoError(t, err)
	assert.True(t, again.Replayed)
	assert.Equal(t, first.Notification.ID, again.Notification.ID, "the replay returns the original resource")

	_, err = svc.Create(t.Context(), notif(), "order-42", "hash-b", t0)
	assert.ErrorIs(t, err, domain.ErrIdempotencyConflict)
}

// Concurrent requests with one key create exactly one notification; every loser replays it.
func TestCreateConcurrentSameKeyCreatesOnce(t *testing.T) {
	t.Parallel()
	store := newMemStore(t0)
	svc := app.NewNotifications(store, 24*time.Hour, 1000)

	var wg sync.WaitGroup
	ids := make(chan uuid.UUID, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.Create(t.Context(), notif(), "storm", "same-hash", t0)
			if assert.NoError(t, err) {
				ids <- res.Notification.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	distinct := map[uuid.UUID]bool{}
	for id := range ids {
		distinct[id] = true
	}
	assert.Len(t, distinct, 1)
	assert.Len(t, store.notifs, 1)
}

func TestCreateBatchReplayAndItemKeyConflict(t *testing.T) {
	t.Parallel()
	svc := app.NewNotifications(newMemStore(t0), 24*time.Hour, 2) // tiny pages exercise the replay read loop

	batchID := uuid.Must(uuid.NewV7())
	items := make([]domain.Notification, 5)
	for i := range items {
		items[i] = notif()
		items[i].BatchID = &batchID
	}
	key := func(i int, k string) app.ItemKey {
		return app.ItemKey{Index: i, Record: domain.IdempotencyKey{Key: k, ResourceType: domain.ResourceNotification, ResourceID: items[i].ID, ResponseStatus: app.AcceptedStatus, ExpiresAt: t0.Add(time.Hour)}}
	}
	in := app.BatchInput{
		Batch: domain.Batch{ID: batchID, Total: len(items)}, Notifications: items,
		BatchKey: "batch-1", BatchHash: "bh", ItemKeys: []app.ItemKey{key(1, "item-b"), key(3, "item-d")},
	}
	res, err := svc.CreateBatch(t.Context(), in, t0)
	require.NoError(t, err)
	require.Len(t, res.NotificationIDs, 5)

	replay, err := svc.CreateBatch(t.Context(), in, t0)
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, res.NotificationIDs, replay.NotificationIDs, "replay returns the original IDs in request order")

	// A different batch reusing item-d is rejected whole, reporting the item's position.
	other := uuid.Must(uuid.NewV7())
	n := notif()
	n.BatchID = &other
	_, err = svc.CreateBatch(t.Context(), app.BatchInput{
		Batch: domain.Batch{ID: other, Total: 1}, Notifications: []domain.Notification{n},
		ItemKeys: []app.ItemKey{{Index: 0, Record: domain.IdempotencyKey{Key: "item-d", ResourceType: domain.ResourceNotification, ResourceID: n.ID, ExpiresAt: t0.Add(time.Hour)}}},
	}, t0)
	var conflict *app.ItemKeyConflictError
	require.ErrorAs(t, err, &conflict)
	require.Len(t, conflict.Items, 1)
	assert.Equal(t, 0, conflict.Items[0].Index)
	assert.Equal(t, items[3].ID, conflict.Items[0].Existing.ResourceID)

	_, err = svc.CreateBatch(t.Context(), app.BatchInput{Batch: in.Batch, Notifications: items, BatchKey: "batch-1", BatchHash: "different"}, t0)
	assert.ErrorIs(t, err, domain.ErrIdempotencyConflict)
}
