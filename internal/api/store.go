package api

import (
	"context"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/app"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository"
)

// Store defines the persistence boundary consumed by the HTTP API handlers.
type Store interface {
	// app.Store: the transactional create use cases (WithTx, idempotency keys, inserts, batch reads).
	app.Store

	Ping(ctx context.Context) error

	GetBatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int64, error)
	CancelBatch(ctx context.Context, batchID uuid.UUID) ([]domain.Notification, error)

	ListNotifications(ctx context.Context, f repository.ListFilter) ([]domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ManualRetry(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ManualRetryFailedBulk(ctx context.Context, f repository.RetryFailedFilter) ([]domain.Notification, error)

	ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error)

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

func (a *repoStoreAdapter) WithTx(ctx context.Context, fn func(ctx context.Context, tx app.Store) error) error {
	return a.Store.WithTx(ctx, func(txCtx context.Context, txRepo *repository.Store) error {
		return fn(txCtx, &repoStoreAdapter{Store: txRepo})
	})
}
