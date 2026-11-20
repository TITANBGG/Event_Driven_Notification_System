package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
)

const (
	idleBackoffMin       = 50 * time.Millisecond
	idleBackoffMax       = time.Second
	limiterErrorBackoff  = 200 * time.Millisecond
	delayedMoverBatchMax = 500
	shutdownCleanupWait  = 5 * time.Second
)

// Deps supplies all dependencies required to construct a Worker.
type Deps struct {
	Config      config.Config
	Logger      *slog.Logger
	Store       Store
	Queue       Queue
	RateLimiter RateLimiter
	Sender      Sender
	RetryPolicy RetryPolicy
	Breaker     Breaker
	Hooks       Hooks
	Now         func() time.Time
}

// Worker runs channel consumer pools, the delayed-set mover, and worker heartbeats.
type Worker struct {
	cfg         config.Config
	logger      *slog.Logger
	store       Store
	queue       Queue
	limiter     RateLimiter
	sender      Sender
	retryPolicy RetryPolicy
	breaker     Breaker
	hooks       Hooks
	nowFn       func() time.Time
}

// New constructs a Worker with safe defaults for optional dependencies.
func New(deps Deps) (*Worker, error) {
	if deps.Store == nil {
		return nil, errors.New("worker store is required")
	}
	if deps.Queue == nil {
		return nil, errors.New("worker queue is required")
	}
	if deps.RateLimiter == nil {
		return nil, errors.New("worker rate limiter is required")
	}
	if deps.Sender == nil {
		return nil, errors.New("worker sender is required")
	}

	logger := deps.Logger
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}
	retryPolicy := deps.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = NoopRetryPolicy{}
	}
	breaker := deps.Breaker
	if breaker == nil {
		breaker = NoopBreaker{}
	}
	nowFn := deps.Now
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}

	return &Worker{
		cfg:         deps.Config,
		logger:      logger,
		store:       deps.Store,
		queue:       deps.Queue,
		limiter:     deps.RateLimiter,
		sender:      deps.Sender,
		retryPolicy: retryPolicy,
		breaker:     breaker,
		hooks:       deps.Hooks,
		nowFn:       nowFn,
	}, nil
}

// Run starts channel consumer goroutines, heartbeat, and delayed-set mover loops and shuts down
// gracefully on ctx cancellation: stops dequeuing, lets in-flight work finish within ShutdownTimeout,
// requeues unstarted inflight items, and removes the worker heartbeat key.
func (w *Worker) Run(ctx context.Context) error {
	// Separate drainCtx keeps in-flight claims/provider calls/fenced writes alive during ShutdownTimeout
	// even after loop ctx cancels on SIGTERM.
	drainCtx, cancelDrain := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDrain()

	var consumerWg sync.WaitGroup
	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	for _, ch := range channels {
		for g := 0; g < w.cfg.WorkerConcurrency; g++ {
			consumerWg.Add(1)
			go func(channel domain.Channel, slot int) {
				defer consumerWg.Done()
				w.consumerLoop(ctx, drainCtx, channel, slot)
			}(ch, g)
		}
	}

	var bgWg sync.WaitGroup
	bgWg.Add(2)
	go func() {
		defer bgWg.Done()
		w.heartbeatLoop(ctx)
	}()
	go func() {
		defer bgWg.Done()
		w.delayedMoverLoop(ctx)
	}()

	<-ctx.Done()
	w.logger.Info("worker stopping dequeue loops", slog.Duration("shutdown_timeout", w.cfg.ShutdownTimeout))

	doneCh := make(chan struct{})
	go func() {
		consumerWg.Wait()
		close(doneCh)
	}()

	shutdownTimer := time.NewTimer(w.cfg.ShutdownTimeout)
	select {
	case <-doneCh:
		shutdownTimer.Stop()
		cancelDrain()
	case <-shutdownTimer.C:
		w.logger.Warn("worker shutdown timeout elapsed; cancelling remaining in-flight sends",
			slog.Duration("timeout", w.cfg.ShutdownTimeout),
		)
		cancelDrain()
		<-doneCh
	}

	cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownCleanupWait)
	defer cleanCancel()

	if count, err := w.queue.RequeueInflight(cleanCtx, w.cfg.WorkerID); err != nil {
		w.logger.Warn("failed to requeue inflight items on shutdown", slog.String("error", err.Error()))
	} else if count > 0 {
		w.logger.Info("requeued inflight items on shutdown", slog.Int64("count", count))
	}

	if err := w.queue.DeleteHeartbeat(cleanCtx, w.cfg.WorkerID); err != nil {
		w.logger.Warn("failed to delete worker heartbeat on shutdown", slog.String("error", err.Error()))
	}

	bgWg.Wait()
	w.logger.Info("worker loops stopped cleanly")
	return nil
}

func (w *Worker) consumerLoop(ctx, drainCtx context.Context, ch domain.Channel, workerNum int) {
	backoff := idleBackoffMin

	for {
		if ctx.Err() != nil {
			return
		}

		panicked := false
		didWork := false
		w.runGuarded(ctx, "consumer", ch, func() {
			meta, ok, err := w.queue.Dequeue(ctx, ch, w.cfg.WorkerID)
			if err != nil {
				if ctx.Err() == nil {
					w.logger.Warn("failed dequeuing from redis",
						slog.String("channel", ch.String()),
						slog.Int("goroutine", workerNum),
						slog.String("error", err.Error()),
					)
				}
				return
			}
			if !ok {
				return
			}

			didWork = true
			w.processDequeuedItem(ctx, drainCtx, ch, meta)
		}, func() {
			panicked = true
		})

		if panicked || !didWork {
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, idleBackoffMax)
			continue
		}
		backoff = idleBackoffMin
	}
}

func (w *Worker) processDequeuedItem(ctx, drainCtx context.Context, ch domain.Channel, meta *queue.Meta) {
	itemCtx := observability.WithCorrelationID(ctx, meta.CorrelationID)
	itemDrainCtx := observability.WithCorrelationID(drainCtx, meta.CorrelationID)
	itemLogger := observability.LoggerFrom(itemCtx, w.logger).With(
		slog.String("notification_id", meta.ID.String()),
		slog.String("channel", ch.String()),
		slog.String("priority", meta.Priority.String()),
	)

	// Acquire GCRA rate-limit token BEFORE Claim so rate-limit wait time never eats VISIBILITY_TIMEOUT.
	for {
		err := w.limiter.Wait(itemCtx, ch)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		itemLogger.Warn("rate limiter error; failing closed and backing off before claim", slog.String("error", err.Error()))
		if !waitWithContext(ctx, limiterErrorBackoff) {
			return
		}
	}

	n, claimed, err := w.store.Claim(itemDrainCtx, meta.ID, w.cfg.ClockSkewTolerance)
	if err != nil {
		itemLogger.Error("database claim failed; deferring item out of inflight list", slog.String("error", err.Error()))
		if schedErr := w.queue.Schedule(itemDrainCtx, *meta, w.nowFn().Add(time.Second), w.cfg.WorkerID); schedErr != nil {
			itemLogger.Warn("failed to defer unclaimed item", slog.String("error", schedErr.Error()))
		}
		return
	}
	if !claimed {
		// Cancelled, already finished by another actor, or scheduled due time not yet reached.
		if ackErr := w.queue.Ack(itemDrainCtx, w.cfg.WorkerID, meta.ID); ackErr != nil {
			itemLogger.Warn("failed to ack unclaimed notification", slog.String("error", ackErr.Error()))
		}
		return
	}

	attemptLogger := itemLogger.With(slog.Int("attempt", n.Attempts))
	if n.BatchID != nil {
		attemptLogger = attemptLogger.With(slog.String("batch_id", n.BatchID.String()))
	}

	startedAt := w.nowFn()
	res, sendErr := w.breaker.Execute(ch, func() (provider.Result, error) {
		return w.sender.Send(itemDrainCtx, *n)
	})
	if sendErr != nil {
		attemptLogger.Warn("provider execution aborted by context cancellation", slog.String("error", sendErr.Error()))
		return
	}

	attemptErr := errorStringPtr(res.Err)
	durationMS := int(res.Duration.Milliseconds())
	if _, insErr := w.store.InsertAttempt(itemDrainCtx, domain.DeliveryAttempt{
		NotificationID:    n.ID,
		AttemptNo:         n.Attempts,
		Counted:           true,
		StartedAt:         startedAt,
		DurationMS:        durationMS,
		HTTPStatus:        res.HTTPStatus,
		Outcome:           mapProviderOutcomeToAttempt(res.Outcome),
		Error:             attemptErr,
		ProviderMessageID: res.ProviderMessageID,
		WorkerID:          &w.cfg.WorkerID,
	}); insErr != nil {
		attemptLogger.Warn("failed recording delivery attempt row", slog.String("error", insErr.Error()))
	}

	switch res.Outcome {
	case provider.OutcomeSent:
		if _, err := w.store.MarkSent(itemDrainCtx, n.ID, n.Attempts, res.ProviderMessageID); err != nil {
			w.handleFencedError(itemDrainCtx, attemptLogger, n.ID, "MarkSent", err)
			return
		}
		if ackErr := w.queue.Ack(itemDrainCtx, w.cfg.WorkerID, n.ID); ackErr != nil {
			attemptLogger.Warn("failed acking sent notification", slog.String("error", ackErr.Error()))
		}
		w.emitProcessed(ch, domain.StatusSent)
		attemptLogger.Info("notification sent", slog.Int("duration_ms", durationMS))

	default:
		// Phase 4 handles both Permanent and Transient as terminal failure; Phase 5 enables exponential retries.
		errText := "delivery failed"
		if attemptErr != nil {
			errText = *attemptErr
		}
		if _, err := w.store.MarkFailed(itemDrainCtx, n.ID, n.Attempts, errText); err != nil {
			w.handleFencedError(itemDrainCtx, attemptLogger, n.ID, "MarkFailed", err)
			return
		}
		if dlqErr := w.queue.DeadLetter(itemDrainCtx, w.cfg.WorkerID, n.ID); dlqErr != nil {
			attemptLogger.Warn("failed deadlettering failed notification", slog.String("error", dlqErr.Error()))
		}
		w.emitProcessed(ch, domain.StatusFailed)
		attemptLogger.Warn("notification failed permanently", slog.String("outcome", string(res.Outcome)), slog.String("error", errText))
	}
}

func (w *Worker) handleFencedError(ctx context.Context, logger *slog.Logger, id uuid.UUID, op string, err error) {
	if errors.Is(err, domain.ErrLeaseLost) {
		logger.Warn("worker lease lost before fenced transition", slog.String("op", op))
		if w.hooks.OnLeaseLost != nil {
			w.hooks.OnLeaseLost()
		}
		if ackErr := w.queue.Ack(ctx, w.cfg.WorkerID, id); ackErr != nil {
			logger.Warn("failed acking after lease lost", slog.String("error", ackErr.Error()))
		}
		return
	}
	logger.Error("fenced database transition failed", slog.String("op", op), slog.String("error", err.Error()))
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	w.runGuarded(ctx, "heartbeat", "", func() {
		if err := w.queue.Heartbeat(ctx, w.cfg.WorkerID, w.cfg.HeartbeatTTL); err != nil && ctx.Err() == nil {
			w.logger.Warn("failed writing initial heartbeat", slog.String("error", err.Error()))
		}
	}, nil)

	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runGuarded(ctx, "heartbeat", "", func() {
				if err := w.queue.Heartbeat(ctx, w.cfg.WorkerID, w.cfg.HeartbeatTTL); err != nil && ctx.Err() == nil {
					w.logger.Warn("failed refreshing heartbeat", slog.String("error", err.Error()))
				}
			}, nil)
		}
	}
}

func (w *Worker) delayedMoverLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.DelayedMoverIntv)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runGuarded(ctx, "delayed_mover", "", func() {
				if _, err := w.queue.MoveDue(ctx, w.nowFn(), delayedMoverBatchMax); err != nil && ctx.Err() == nil {
					w.logger.Warn("delayed mover cycle failed", slog.String("error", err.Error()))
				}
			}, nil)
		}
	}
}

func (w *Worker) runGuarded(ctx context.Context, component string, ch domain.Channel, fn, onPanicFn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			attrs := []any{
				slog.String("component", component),
				slog.String("panic", fmt.Sprint(rec)),
				slog.String("stack", string(debug.Stack())),
			}
			if ch != "" {
				attrs = append(attrs, slog.String("channel", ch.String()))
			}
			observability.LoggerFrom(ctx, w.logger).Error("panic recovered in worker goroutine", attrs...)
			if w.hooks.OnPanic != nil {
				w.hooks.OnPanic()
			}
			if onPanicFn != nil {
				onPanicFn()
			}
		}
	}()
	fn()
}

func (w *Worker) emitProcessed(ch domain.Channel, status domain.Status) {
	if w.hooks.OnProcessed != nil {
		w.hooks.OnProcessed(ch, status)
	}
}

func waitWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return false
	case <-timer.C:
		return true
	}
}

func mapProviderOutcomeToAttempt(o provider.OutcomeKind) domain.AttemptOutcome {
	switch o {
	case provider.OutcomeSent:
		return domain.OutcomeSent
	case provider.OutcomePermanent:
		return domain.OutcomePermanent
	default:
		return domain.OutcomeTransient
	}
}

func errorStringPtr(err error) *string {
	if err == nil {
		return nil
	}
	s := err.Error()
	return &s
}
