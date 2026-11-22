// Package app holds the notification use cases that sit between transports and persistence:
// creating single and batch notifications atomically with their idempotency keys, and replaying the
// original result for a repeated key. It knows nothing about HTTP; the API maps its typed errors to
// status codes in one place (internal/api/errors.go).
package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository"
)

// AcceptedStatus is the response status recorded in idempotency_keys.response_status for a created
// resource; a replay returns it unchanged.
const AcceptedStatus = 202

var (
	errKeyTaken      = errors.New("idempotency key exists")
	errBatchKeyTaken = errors.New("batch idempotency key exists")
)

// Store is the persistence the create use cases need. internal/api's store satisfies it.
type Store interface {
	WithTx(ctx context.Context, fn func(ctx context.Context, tx Store) error) error

	InsertIdempotencyKey(ctx context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error)
	InsertIdempotencyKeys(ctx context.Context, keys []domain.IdempotencyKey) ([]string, error)
	GetIdempotencyKey(ctx context.Context, key string) (*domain.IdempotencyKey, error)
	GetIdempotencyKeysByKeys(ctx context.Context, keys []string) ([]domain.IdempotencyKey, error)

	InsertNotification(ctx context.Context, n domain.Notification) (*domain.Notification, error)
	CopyNotifications(ctx context.Context, items []domain.Notification) (int64, error)
	GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error)

	InsertBatch(ctx context.Context, b domain.Batch) (*domain.Batch, error)
	GetBatch(ctx context.Context, id uuid.UUID) (*domain.Batch, error)
	ListBatchItems(ctx context.Context, batchID uuid.UUID, cursor *repository.Cursor, limit int) ([]domain.Notification, error)
}

// ItemKeyConflict is one batch item whose idempotency key is already active for another resource.
type ItemKeyConflict struct {
	Index    int // position in the batch request, -1 if unknown
	Existing domain.IdempotencyKey
}

// ItemKeyConflictError rejects a whole batch because item keys already exist (all-or-nothing).
// Items may be empty when the conflicting key expired between the insert and the lookup.
type ItemKeyConflictError struct {
	Items []ItemKeyConflict
}

func (e *ItemKeyConflictError) Error() string {
	return "one or more batch item idempotency keys already exist"
}

// Notifications implements the create use cases.
type Notifications struct {
	store          Store
	idempotencyTTL time.Duration
	pageSize       int
}

// NewNotifications returns the create use cases backed by store. pageSize bounds the page used to
// read a batch back for a replay (BATCH_MAX_ITEMS).
func NewNotifications(store Store, idempotencyTTL time.Duration, pageSize int) *Notifications {
	return &Notifications{store: store, idempotencyTTL: idempotencyTTL, pageSize: pageSize}
}

// CreateResult is the outcome of a create: the stored (or, on replay, the original) notification.
type CreateResult struct {
	Notification *domain.Notification
	Replayed     bool
	Status       int
}

// Create inserts candidate, and its Idempotency-Key when idemKey is set, in one transaction. The
// key's primary key serializes concurrent duplicates: the loser rolls back and replays the winner's
// result (same hash) or returns domain.ErrIdempotencyConflict (different hash).
func (s *Notifications) Create(ctx context.Context, candidate domain.Notification, idemKey, reqHash string, now time.Time) (CreateResult, error) {
	// At most 2 attempts handles the narrow window where a conflicting key expires between insert and lookup.
	for attempt := 0; attempt < 2; attempt++ {
		var inserted *domain.Notification
		txErr := s.store.WithTx(ctx, func(txCtx context.Context, tx Store) error {
			if idemKey != "" {
				_, ok, err := tx.InsertIdempotencyKey(txCtx, domain.IdempotencyKey{
					Key:            idemKey,
					RequestHash:    reqHash,
					ResourceType:   domain.ResourceNotification,
					ResourceID:     candidate.ID,
					ResponseStatus: AcceptedStatus,
					ExpiresAt:      now.Add(s.idempotencyTTL),
				})
				if err != nil {
					return err
				}
				if !ok {
					return errKeyTaken
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
			return CreateResult{Notification: inserted, Status: AcceptedStatus}, nil
		}
		if !errors.Is(txErr, errKeyTaken) {
			return CreateResult{}, txErr
		}

		existing, err := s.store.GetIdempotencyKey(ctx, idemKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return CreateResult{}, err
		}
		if existing.ResourceType != domain.ResourceNotification || existing.RequestHash != reqHash {
			return CreateResult{}, domain.ErrIdempotencyConflict
		}
		orig, err := s.store.GetNotification(ctx, existing.ResourceID)
		if err != nil {
			return CreateResult{}, err
		}
		return CreateResult{Notification: orig, Replayed: true, Status: existing.ResponseStatus}, nil
	}
	return CreateResult{}, domain.ErrIdempotencyConflict
}

// ItemKey is a batch item's idempotency key record and its position in the request.
type ItemKey struct {
	Index  int
	Record domain.IdempotencyKey
}

// BatchInput is a validated, fully built batch ready to persist.
type BatchInput struct {
	Batch         domain.Batch
	Notifications []domain.Notification
	BatchKey      string // whole-batch Idempotency-Key, optional
	BatchHash     string
	ItemKeys      []ItemKey
}

// BatchResult is the outcome of a batch create: the stored (or, on replay, the original) batch.
type BatchResult struct {
	BatchID         uuid.UUID
	NotificationIDs []uuid.UUID
	Replayed        bool
	Status          int
}

// CreateBatch inserts the batch, every notification (one COPY) and every key in one transaction:
// all or nothing. A repeated batch key replays the original batch; an item key that is already
// active rejects the whole batch with *ItemKeyConflictError.
func (s *Notifications) CreateBatch(ctx context.Context, in BatchInput, now time.Time) (BatchResult, error) {
	indexByKey := make(map[string]int, len(in.ItemKeys))
	itemKeys := make([]string, len(in.ItemKeys))
	records := make([]domain.IdempotencyKey, len(in.ItemKeys))
	for i, k := range in.ItemKeys {
		indexByKey[k.Record.Key] = k.Index
		itemKeys[i] = k.Record.Key
		records[i] = k.Record
	}

	for attempt := 0; attempt < 2; attempt++ {
		txErr := s.store.WithTx(ctx, func(txCtx context.Context, tx Store) error {
			// Insert the batch-level key first so a whole-batch replay wins before item-key checks.
			if in.BatchKey != "" {
				_, ok, err := tx.InsertIdempotencyKey(txCtx, domain.IdempotencyKey{
					Key:            in.BatchKey,
					RequestHash:    in.BatchHash,
					ResourceType:   domain.ResourceBatch,
					ResourceID:     in.Batch.ID,
					ResponseStatus: AcceptedStatus,
					ExpiresAt:      now.Add(s.idempotencyTTL),
				})
				if err != nil {
					return err
				}
				if !ok {
					return errBatchKeyTaken
				}
			}

			if len(records) > 0 {
				existing, err := tx.GetIdempotencyKeysByKeys(txCtx, itemKeys)
				if err != nil {
					return err
				}
				if len(existing) > 0 {
					return newItemKeyConflictError(existing, indexByKey)
				}
				// One statement for every item key (was one round trip per item). A key missing from
				// the result was claimed by a concurrent request after the read above: reject the batch.
				claimed, err := tx.InsertIdempotencyKeys(txCtx, records)
				if err != nil {
					return err
				}
				if len(claimed) != len(records) {
					got := make(map[string]struct{}, len(claimed))
					for _, k := range claimed {
						got[k] = struct{}{}
					}
					var conflicts []domain.IdempotencyKey
					for _, rec := range records {
						if _, ok := got[rec.Key]; ok {
							continue
						}
						if row, getErr := tx.GetIdempotencyKey(txCtx, rec.Key); getErr == nil {
							conflicts = append(conflicts, *row)
						}
					}
					return newItemKeyConflictError(conflicts, indexByKey)
				}
			}

			if _, err := tx.InsertBatch(txCtx, in.Batch); err != nil {
				return err
			}
			copied, err := tx.CopyNotifications(txCtx, in.Notifications)
			if err != nil {
				return err
			}
			if int(copied) != len(in.Notifications) {
				return fmt.Errorf("copied %d of %d batch notifications", copied, len(in.Notifications))
			}
			return nil
		})
		if txErr == nil {
			ids := make([]uuid.UUID, len(in.Notifications))
			for i, n := range in.Notifications {
				ids[i] = n.ID
			}
			return BatchResult{BatchID: in.Batch.ID, NotificationIDs: ids, Status: AcceptedStatus}, nil
		}
		if !errors.Is(txErr, errBatchKeyTaken) {
			return BatchResult{}, txErr
		}

		existing, err := s.store.GetIdempotencyKey(ctx, in.BatchKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return BatchResult{}, err
		}
		if existing.ResourceType != domain.ResourceBatch || existing.RequestHash != in.BatchHash {
			return BatchResult{}, domain.ErrIdempotencyConflict
		}
		origBatch, err := s.store.GetBatch(ctx, existing.ResourceID)
		if err != nil {
			return BatchResult{}, err
		}
		items, err := s.BatchItemsOrdered(ctx, origBatch.ID, origBatch.Total)
		if err != nil {
			return BatchResult{}, err
		}
		ids := make([]uuid.UUID, len(items))
		for i, item := range items {
			ids[i] = item.ID
		}
		return BatchResult{BatchID: origBatch.ID, NotificationIDs: ids, Replayed: true, Status: existing.ResponseStatus}, nil
	}
	return BatchResult{}, domain.ErrIdempotencyConflict
}

// BatchItemsOrdered reads every item of a batch in request order (UUIDv7 order).
func (s *Notifications) BatchItemsOrdered(ctx context.Context, batchID uuid.UUID, total int) ([]domain.Notification, error) {
	all := make([]domain.Notification, 0, max(total, 0))
	var cursor *repository.Cursor
	pageSize := total
	if pageSize <= 0 {
		pageSize = s.pageSize
	}
	for {
		page, err := s.store.ListBatchItems(ctx, batchID, cursor, pageSize+1)
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
	sort.Slice(all, func(i, j int) bool { return bytes.Compare(all[i].ID[:], all[j].ID[:]) < 0 })
	return all, nil
}

func newItemKeyConflictError(existing []domain.IdempotencyKey, indexByKey map[string]int) error {
	items := make([]ItemKeyConflict, 0, len(existing))
	for _, ex := range existing {
		idx, ok := indexByKey[ex.Key]
		if !ok {
			idx = -1
		}
		items = append(items, ItemKeyConflict{Index: idx, Existing: ex})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Index < items[j].Index })
	return &ItemKeyConflictError{Items: items}
}
