package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
)

const (
	defaultReconcilerLockTTL = 30 * time.Second
	defaultReconcileBatch    = 1000
	defaultDelayedLookahead  = 5 * time.Minute
	idempotencyDeleteBatch   = 10000
	idempotencyMaxBatches    = 10
	// maxPagesPerCycle bounds each step to 5 pages of batchLimit rows per cycle. Repairs are bounded by
	// the scanned pages, not by a separate repair count: with a repair cap of one batch (the former
	// rule) a mass Redis loss was rebuilt at only ~1000 rows per 15s cycle (~67/s), below the 300/s
	// the workers can send; now up to 5000 rows per cycle (~333/s) are repaired.
	maxPagesPerCycle     = 5
	abandonedFailMessage = "visibility timeout; worker presumed dead"
	// abandonedExhaustedMessage is the last_error of a row failed after its final attempt was
	// abandoned. The worker may have died after the provider accepted the message, so it says the
	// delivery outcome is unknown instead of implying the message was not delivered.
	abandonedExhaustedMessage = "delivery outcome unknown: worker died during the final attempt (visibility timeout)"
)

// ReconcilerStore defines the PostgreSQL queries and mutations used by the distributed reconciler.
type ReconcilerStore interface {
	FindStalePending(ctx context.Context, staleAfter time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error)
	FindDelayedDueSoon(ctx context.Context, staleAfter, lookahead time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error)
	FindAbandonedProcessing(ctx context.Context, visibilityTimeout time.Duration, afterID *uuid.UUID, limit int) ([]domain.Notification, error)
	RequeueAbandoned(ctx context.Context, id uuid.UUID, claimedAttempts int) (*domain.Notification, error)
	FailAbandoned(ctx context.Context, id uuid.UUID, claimedAttempts int, lastError string) (*domain.Notification, error)
	InsertAttempt(ctx context.Context, a domain.DeliveryAttempt) (*domain.DeliveryAttempt, error)
	DeleteExpiredIdempotencyKeys(ctx context.Context, limit int) (int64, error)
}

// ReconcilerQueue defines the Redis lock, inspection, and repair operations used by the reconciler.
type ReconcilerQueue interface {
	AcquireReconcilerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ReleaseReconcilerLock(ctx context.Context, owner string) (bool, error)
	ExtendReconcilerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ScanInflightWorkerIDs(ctx context.Context) ([]string, error)
	HasHeartbeats(ctx context.Context, workerIDs []string) (map[string]bool, error)
	RequeueInflight(ctx context.Context, workerID string) (int64, error)
	DeleteInflightKey(ctx context.Context, workerID string) error
	IsTracked(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error)
	ForceEnqueue(ctx context.Context, items ...queue.Meta) error
	Schedule(ctx context.Context, meta queue.Meta, dueAt time.Time, workerID string) error
	DeadLetter(ctx context.Context, workerID string, id uuid.UUID) error
}

// ReconcilerDeps configures DistributedReconciler construction.
type ReconcilerDeps struct {
	Config           config.Config
	Logger           *slog.Logger
	Store            ReconcilerStore
	Queue            ReconcilerQueue
	Now              func() time.Time
	LockTTL          time.Duration
	BatchLimit       int
	DelayedLookahead time.Duration
	OnAction         func(step string, count int)
}

// DistributedReconciler repairs PostgreSQL/Redis divergences across worker replicas under a shared Redis lease.
type DistributedReconciler struct {
	workerID          string
	pendingStaleAfter time.Duration
	visibilityTimeout time.Duration
	lockTTL           time.Duration
	batchLimit        int
	delayedLookahead  time.Duration
	logger            *slog.Logger
	store             ReconcilerStore
	queue             ReconcilerQueue
	nowFn             func() time.Time
	onAction          func(step string, count int)

	// Keyset cursors persist across cycles so a large tracked backlog (e.g. a flash sale) cannot
	// hide untracked rows beyond the per-cycle page budget; each scan wraps to the start at the end.
	cycleMu         sync.Mutex
	pendingCursor   *uuid.UUID
	delayedCursor   *uuid.UUID
	abandonedCursor *uuid.UUID
}

// NewReconciler constructs a DistributedReconciler with safe production defaults.
func NewReconciler(deps ReconcilerDeps) (*DistributedReconciler, error) {
	if deps.Store == nil {
		return nil, errors.New("reconciler store is required")
	}
	if deps.Queue == nil {
		return nil, errors.New("reconciler queue is required")
	}

	logger := deps.Logger
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}
	nowFn := deps.Now
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	lockTTL := deps.LockTTL
	if lockTTL <= 0 {
		lockTTL = defaultReconcilerLockTTL
	}
	batchLimit := deps.BatchLimit
	if batchLimit <= 0 {
		batchLimit = defaultReconcileBatch
	}
	lookahead := deps.DelayedLookahead
	if lookahead <= 0 {
		lookahead = defaultDelayedLookahead
	}
	staleAfter := deps.Config.PendingStaleAfter
	if staleAfter <= 0 {
		staleAfter = 30 * time.Second
	}
	visibilityTimeout := deps.Config.VisibilityTimeout
	if visibilityTimeout <= 0 {
		visibilityTimeout = 60 * time.Second
	}
	workerID := deps.Config.WorkerID
	if workerID == "" {
		workerID = "reconciler"
	}

	return &DistributedReconciler{
		workerID:          workerID,
		pendingStaleAfter: staleAfter,
		visibilityTimeout: visibilityTimeout,
		lockTTL:           lockTTL,
		batchLimit:        batchLimit,
		delayedLookahead:  lookahead,
		logger:            logger,
		store:             deps.Store,
		queue:             deps.Queue,
		nowFn:             nowFn,
		onAction:          deps.OnAction,
	}, nil
}

// RunCycle acquires the distributed reconciler lease and executes all five reconciliation steps once.
func (r *DistributedReconciler) RunCycle(ctx context.Context) error {
	r.cycleMu.Lock()
	defer r.cycleMu.Unlock()

	lockOwner := r.workerID + ":" + uuid.NewString()
	acquired, err := r.queue.AcquireReconcilerLock(ctx, lockOwner, r.lockTTL)
	if err != nil {
		return fmt.Errorf("acquiring reconciler lock: %w", err)
	}
	if !acquired {
		return nil
	}

	defer func() {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if _, relErr := r.queue.ReleaseReconcilerLock(relCtx, lockOwner); relErr != nil {
			r.logger.Warn("failed releasing reconciler lock", slog.String("error", relErr.Error()))
		}
	}()

	// Requeue orphaned inflight lists first so step (a) observes recovered Redis tracking entries.
	steps := []struct {
		name string
		run  func(context.Context) error
	}{
		{"orphan_inflight", r.reconcileOrphanInflight},
		{"stale_pending", r.reconcileStalePending},
		{"delayed_due", r.reconcileDelayedDueSoon},
		{"abandoned_processing", r.reconcileAbandonedProcessing},
		{"idempotency_cleanup", r.cleanupExpiredIdempotencyKeys},
	}
	var stepErrs []error
	for i, step := range steps {
		if ctx.Err() != nil {
			break
		}
		// The lease is renewed before every step after the first: a slow cycle (large backlog, slow
		// database) must not outlive its lease while still repairing, or a second replica would run
		// the same repairs concurrently.
		if i > 0 {
			held, extErr := r.queue.ExtendReconcilerLock(ctx, lockOwner, r.lockTTL)
			if extErr != nil || !held {
				r.logger.Warn("reconciler lease lost mid-cycle; stopping before the next step",
					slog.String("next_step", step.name), slog.Any("error", extErr))
				stepErrs = append(stepErrs, fmt.Errorf("reconciler lease lost before step %s", step.name))
				break
			}
		}
		if err := step.run(ctx); err != nil {
			stepErrs = append(stepErrs, fmt.Errorf("step %s: %w", step.name, err))
		}
	}
	return errors.Join(stepErrs...)
}

func (r *DistributedReconciler) reconcileOrphanInflight(ctx context.Context) error {
	workerIDs, err := r.queue.ScanInflightWorkerIDs(ctx)
	if err != nil {
		return err
	}
	if len(workerIDs) == 0 {
		return nil
	}

	heartbeats, err := r.queue.HasHeartbeats(ctx, workerIDs)
	if err != nil {
		return err
	}

	var errs []error
	recovered := 0
	for _, wid := range workerIDs {
		if heartbeats[wid] {
			continue
		}
		moved, reqErr := r.queue.RequeueInflight(ctx, wid)
		if reqErr != nil {
			errs = append(errs, fmt.Errorf("requeue inflight for dead worker %s: %w", wid, reqErr))
			continue
		}
		if delErr := r.queue.DeleteInflightKey(ctx, wid); delErr != nil {
			errs = append(errs, fmt.Errorf("delete inflight key for dead worker %s: %w", wid, delErr))
		}
		r.logger.Warn("requeued orphaned inflight items from dead worker",
			slog.String("dead_worker_id", wid),
			slog.Int64("count", moved),
		)
		recovered += int(moved)
	}
	r.emitAction("orphan_inflight", recovered)
	return errors.Join(errs...)
}

func (r *DistributedReconciler) reconcileStalePending(ctx context.Context) error {
	afterID := r.pendingCursor
	reachedEnd := false
	repaired := 0
	for page := 0; page < maxPagesPerCycle; page++ {
		rows, err := r.store.FindStalePending(ctx, r.pendingStaleAfter, afterID, r.batchLimit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			reachedEnd = true
			break
		}

		untracked, err := r.filterUntracked(ctx, rows)
		if err != nil {
			return err
		}
		if len(untracked) > 0 {
			metas := make([]queue.Meta, 0, len(untracked))
			for _, row := range untracked {
				metas = append(metas, metaFromNotification(&row, nil))
			}
			if err := r.queue.ForceEnqueue(ctx, metas...); err != nil {
				return err
			}
			repaired += len(untracked)
			r.logger.Warn("re-enqueued stale pending notifications missing from redis",
				slog.Int("count", len(untracked)),
			)
		}

		if len(rows) < r.batchLimit {
			reachedEnd = true
			break
		}
		lastID := rows[len(rows)-1].ID
		afterID = &lastID
	}
	if reachedEnd {
		r.pendingCursor = nil
	} else {
		r.pendingCursor = afterID
	}
	r.emitAction("stale_pending", repaired)
	return nil
}

func (r *DistributedReconciler) reconcileDelayedDueSoon(ctx context.Context) error {
	afterID := r.delayedCursor
	reachedEnd := false
	repaired := 0
	var errs []error

	for page := 0; page < maxPagesPerCycle; page++ {
		rows, err := r.store.FindDelayedDueSoon(ctx, r.pendingStaleAfter, r.delayedLookahead, afterID, r.batchLimit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			reachedEnd = true
			break
		}

		untracked, err := r.filterUntracked(ctx, rows)
		if err != nil {
			return err
		}
		for _, row := range untracked {
			due := r.resolveDueAt(row)
			if err := r.queue.Schedule(ctx, metaFromNotification(&row, nil), due, ""); err != nil {
				errs = append(errs, fmt.Errorf("rescheduling %s: %w", row.ID, err))
				continue
			}
			repaired++
		}
		if len(untracked) > 0 {
			r.logger.Warn("repaired missing delayed entries in redis", slog.Int("count", len(untracked)))
		}

		if len(rows) < r.batchLimit {
			reachedEnd = true
			break
		}
		lastID := rows[len(rows)-1].ID
		afterID = &lastID
	}
	if reachedEnd {
		r.delayedCursor = nil
	} else {
		r.delayedCursor = afterID
	}
	r.emitAction("delayed_due", repaired)
	return errors.Join(errs...)
}

func (r *DistributedReconciler) reconcileAbandonedProcessing(ctx context.Context) error {
	afterID := r.abandonedCursor
	reachedEnd := false
	recovered := 0
	var errs []error

	for page := 0; page < maxPagesPerCycle; page++ {
		rows, err := r.store.FindAbandonedProcessing(ctx, r.visibilityTimeout, afterID, r.batchLimit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			reachedEnd = true
			break
		}

		for _, row := range rows {
			if err := r.recoverSingleAbandoned(ctx, row); err != nil {
				errs = append(errs, err)
				continue
			}
			recovered++
		}

		if len(rows) < r.batchLimit {
			reachedEnd = true
			break
		}
		lastID := rows[len(rows)-1].ID
		afterID = &lastID
	}
	if reachedEnd {
		r.abandonedCursor = nil
	} else {
		r.abandonedCursor = afterID
	}
	r.emitAction("abandoned_processing", recovered)
	return errors.Join(errs...)
}

func (r *DistributedReconciler) recoverSingleAbandoned(ctx context.Context, row domain.Notification) error {
	exhausted := row.Attempts >= row.MaxAttempts
	var (
		updated *domain.Notification
		err     error
	)
	if exhausted {
		updated, err = r.store.FailAbandoned(ctx, row.ID, row.Attempts, abandonedExhaustedMessage)
	} else {
		updated, err = r.store.RequeueAbandoned(ctx, row.ID, row.Attempts)
	}
	if err != nil {
		if errors.Is(err, domain.ErrLeaseLost) {
			return nil
		}
		return fmt.Errorf("transitioning abandoned %s: %w", row.ID, err)
	}

	r.logger.Warn("worker likely crashed; recovering abandoned processing notification",
		slog.String("notification_id", row.ID.String()),
		slog.Int("attempts", row.Attempts),
		slog.Int("max_attempts", row.MaxAttempts),
		slog.Bool("exhausted", exhausted),
	)

	errMsg := abandonedFailMessage
	if _, insErr := r.store.InsertAttempt(ctx, domain.DeliveryAttempt{
		NotificationID: row.ID,
		AttemptNo:      row.Attempts,
		Counted:        false,
		StartedAt:      r.nowFn(),
		DurationMS:     0,
		Outcome:        domain.OutcomeAbandoned,
		Error:          &errMsg,
		WorkerID:       &r.workerID,
	}); insErr != nil {
		r.logger.Warn("failed inserting abandoned attempt audit row",
			slog.String("notification_id", row.ID.String()),
			slog.String("error", insErr.Error()),
		)
	}

	if exhausted {
		if dlErr := r.queue.DeadLetter(ctx, "", row.ID); dlErr != nil {
			return fmt.Errorf("deadlettering abandoned %s: %w", row.ID, dlErr)
		}
		return nil
	}

	if eqErr := r.queue.ForceEnqueue(ctx, metaFromNotification(updated, nil)); eqErr != nil {
		return fmt.Errorf("re-enqueuing abandoned %s: %w", row.ID, eqErr)
	}
	return nil
}

func (r *DistributedReconciler) cleanupExpiredIdempotencyKeys(ctx context.Context) error {
	var totalDeleted int64
	for i := 0; i < idempotencyMaxBatches; i++ {
		deleted, err := r.store.DeleteExpiredIdempotencyKeys(ctx, idempotencyDeleteBatch)
		if err != nil {
			return err
		}
		totalDeleted += deleted
		if deleted < idempotencyDeleteBatch {
			break
		}
	}
	if totalDeleted > 0 {
		r.logger.Info("deleted expired idempotency keys", slog.Int64("count", totalDeleted))
	}
	r.emitAction("idempotency_cleanup", int(totalDeleted))
	return nil
}

func (r *DistributedReconciler) filterUntracked(ctx context.Context, rows []domain.Notification) ([]domain.Notification, error) {
	ids := make([]uuid.UUID, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	tracked, err := r.queue.IsTracked(ctx, ids)
	if err != nil {
		return nil, err
	}
	untracked := make([]domain.Notification, 0, len(rows))
	for _, row := range rows {
		if !tracked[row.ID] {
			untracked = append(untracked, row)
		}
	}
	return untracked, nil
}

func (r *DistributedReconciler) resolveDueAt(row domain.Notification) time.Time {
	if row.NextAttemptAt != nil {
		return *row.NextAttemptAt
	}
	if row.ScheduledAt != nil {
		return *row.ScheduledAt
	}
	return r.nowFn()
}

func (r *DistributedReconciler) emitAction(step string, count int) {
	if r.onAction != nil && count > 0 {
		r.onAction(step, count)
	}
}
