package api

import (
	"context"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository"
)

// Store defines the persistence boundary consumed by the HTTP API handlers.
type Store interface {
	Ping(ctx context.Context) error
	WithTx(ctx context.Context, fn func(ctx context.Context, tx Store) error) error

	InsertBatch(ctx context.Context, b domain.Batch) (*domain.Batch, error)
	GetBatch(ctx context.Context, id uuid.UUID) (*domain.Batch, error)
	GetBatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int64, error)
	ListBatchItems(ctx context.Context, batchID uuid.UUID, cursor *repository.Cursor, limit int) ([]domain.Notification, error)
	CancelBatch(ctx context.Context, batchID uuid.UUID) ([]domain.Notification, error)

	InsertNotification(ctx context.Context, n domain.Notification) (*domain.Notification, error)
	CopyNotifications(ctx context.Context, items []domain.Notification) (int64, error)
	GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ListNotifications(ctx context.Context, f repository.ListFilter) ([]domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ManualRetry(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ManualRetryFailedBulk(ctx context.Context, f repository.RetryFailedFilter) ([]domain.Notification, error)

	ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error)

	InsertIdempotencyKey(ctx context.Context, k domain.IdempotencyKey) (*domain.IdempotencyKey, bool, error)
	InsertIdempotencyKeys(ctx context.Context, keys []domain.IdempotencyKey) ([]string, error)
	GetIdempotencyKey(ctx context.Context, key string) (*domain.IdempotencyKey, error)
	GetIdempotencyKeysByKeys(ctx context.Context, keys []string) ([]domain.IdempotencyKey, error)

	InsertTemplate(ctx context.Context, t domain.Template) (*domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error)
	GetTemplatesByIDs(ctx context.Context, ids []uuid.UUID) ([]domain.Template, error)
	ListTemplates(ctx context.Context, f repository.TemplateListFilter) ([]domain.Template, error)
	UpdateTemplate(ctx context.Context, t domain.Template) (*domain.Template, error)
	SoftDeleteTemplate(ctx context.Context, id uuid.UUID) error

	MetricsWindowCounts(ctx context.Context) ([]repository.MetricsWindowRow, error)
	LatencyPercentiles(ctx context.Context) (repository.LatencyStats, error)
}

type repoStoreAdapter struct {
	*repository.Store
}

// NewRepositoryStore adapts *repository.Store to the api.Store interface.
func NewRepositoryStore(s *repository.Store) Store {
	if s == nil {
		return nil
	}
	return &repoStoreAdapter{Store: s}
}

func (a *repoStoreAdapter) WithTx(ctx context.Context, fn func(ctx context.Context, tx Store) error) error {
	return a.Store.WithTx(ctx, func(txCtx context.Context, txRepo *repository.Store) error {
		return fn(txCtx, &repoStoreAdapter{Store: txRepo})
	})
}
