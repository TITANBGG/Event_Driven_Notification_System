package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	circuitOpenJitterMax = 5 * time.Second
	defaultSweepInterval = 15 * time.Second
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
	Reconciler  Reconciler
	Hooks       Hooks
	Now         func() time.Time
	Jitter      func(maxJitter time.Duration) time.Duration
}

// Worker runs channel consumer pools, heartbeat, delayed-set mover, and optional reconciler loops.
type Worker struct {
	cfg         config.Config
	logger      *slog.Logger
	store       Store
	queue       Queue
	limiter     RateLimiter
	sender      Sender
	retryPolicy RetryPolicy
	breaker     Breaker
	reconciler  Reconciler
	hooks       Hooks
	nowFn       func() time.Time
	jitterFn    func(maxJitter time.Duration) time.Duration

	// active counts IDs this process is currently handling, so the inflight sweeper can tell a
	// live item from one stranded in Redis after a failed post-transition write.
	activeMu sync.Mutex
	active   map[uuid.UUID]int
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
	jitterFn := deps.Jitter
	if jitterFn == nil {
		if js, ok := retryPolicy.(interface {
			Jitter(time.Duration) time.Duration
		}); ok {
			jitterFn = js.Jitter
		} else {
			jitterFn = func(maxJitter time.Duration) time.Duration {
				if maxJitter <= 0 {
					return 0
				}
				return rand.N(maxJitter)
			}
		}
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
		reconciler:  deps.Reconciler,
		hooks:       deps.Hooks,
		nowFn:       nowFn,
		jitterFn:    jitterFn,
		active:      make(map[uuid.UUID]int),
	}, nil
}

// Run starts channel consumer goroutines, heartbeat, delayed-set mover, and reconciler loops and
// shuts down gracefully on ctx cancellation: stops dequeuing, lets in-flight work finish within
// ShutdownTimeout, requeues unstarted inflight items, and removes the worker heartbeat key.
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
	bgWg.Add(3)
	go func() {
		defer bgWg.Done()
		w.heartbeatLoop(ctx)
	}()
	go func() {
		defer bgWg.Done()
		w.delayedMoverLoop(ctx)
	}()
	go func() {
		defer bgWg.Done()
		w.inflightSweepLoop(ctx)
	}()
	if w.reconciler != nil && w.cfg.ReconcileInterval > 0 {
		bgWg.Add(1)
		go func() {
			defer bgWg.Done()
			w.reconcilerLoop(ctx)
		}()
	}

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
			w.markActive(meta.ID)
			defer w.unmarkActive(meta.ID)
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
	if errors.Is(sendErr, ErrCircuitOpen) {
		w.handleCircuitOpen(itemDrainCtx, attemptLogger, ch, n, meta, startedAt)
		return
	}
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

	case provider.OutcomeTransient:
		w.handleTransientOutcome(itemDrainCtx, attemptLogger, ch, n, meta, res, attemptErr)

	default:
		w.failNotification(
			itemDrainCtx,
			attemptLogger,
			ch,
			n.ID,
			n.Attempts,
			errorStringValue(attemptErr, "delivery failed"),
			"permanent_failure",
		)
	}
}

func (w *Worker) handleCircuitOpen(
	drainCtx context.Context,
	logger *slog.Logger,
	ch domain.Channel,
	n *domain.Notification,
	meta *queue.Meta,
	startedAt time.Time,
) {
	errMsg := ErrCircuitOpen.Error()
	if _, insErr := w.store.InsertAttempt(drainCtx, domain.DeliveryAttempt{
		NotificationID: n.ID,
		AttemptNo:      n.Attempts,
		Counted:        false,
		StartedAt:      startedAt,
		DurationMS:     0,
		Outcome:        domain.OutcomeCircuitOpen,
		Error:          &errMsg,
		WorkerID:       &w.cfg.WorkerID,
	}); insErr != nil {
		logger.Warn("failed recording circuit-open deferral attempt row", slog.String("error", insErr.Error()))
	}

	if n.Deferrals+1 > w.cfg.MaxCircuitDeferral {
		w.failNotification(drainCtx, logger, ch, n.ID, n.Attempts, "circuit open too long", "circuit_open_exhausted")
		return
	}

	nextAt := w.nowFn().Add(w.cfg.CBOpenTimeout + w.jitterFn(circuitOpenJitterMax))
	updated, err := w.store.DeferCircuitOpen(drainCtx, n.ID, n.Attempts, nextAt, errMsg)
	if err != nil {
		w.handleFencedError(drainCtx, logger, n.ID, "DeferCircuitOpen", err)
		return
	}
	if schedErr := w.queue.Schedule(drainCtx, metaFromNotification(updated, meta), nextAt, w.cfg.WorkerID); schedErr != nil {
		logger.Warn("failed scheduling circuit-open deferral", slog.String("error", schedErr.Error()))
	}
	logger.Warn("notification deferred while circuit breaker is open",
		slog.Int("deferrals", updated.Deferrals),
		slog.Time("next_attempt_at", nextAt),
	)
}

func (w *Worker) handleTransientOutcome(
	drainCtx context.Context,
	logger *slog.Logger,
	ch domain.Channel,
	n *domain.Notification,
	meta *queue.Meta,
	res provider.Result,
	attemptErr *string,
) {
	errText := errorStringValue(attemptErr, "transient delivery failure")
	if n.Attempts < n.MaxAttempts {
		delay := w.retryPolicy.NextDelay(n.Attempts, res.RetryAfter)
		nextAt := w.nowFn().Add(delay)
		updated, err := w.store.ScheduleRetry(drainCtx, n.ID, n.Attempts, nextAt, errText)
		if err != nil {
			w.handleFencedError(drainCtx, logger, n.ID, "ScheduleRetry", err)
			return
		}
		if schedErr := w.queue.Schedule(drainCtx, metaFromNotification(updated, meta), nextAt, w.cfg.WorkerID); schedErr != nil {
			logger.Warn("failed scheduling notification retry", slog.String("error", schedErr.Error()))
		}
		if w.hooks.OnRetryScheduled != nil {
			w.hooks.OnRetryScheduled(ch)
		}
		logger.Warn("notification scheduled for retry",
			slog.Duration("delay", delay),
			slog.Time("next_attempt_at", nextAt),
			slog.String("error", errText),
		)
		return
	}

	w.failNotification(drainCtx, logger, ch, n.ID, n.Attempts, "max attempts reached: "+errText, "max_attempts_exhausted")
}

func (w *Worker) failNotification(
	drainCtx context.Context,
	logger *slog.Logger,
	ch domain.Channel,
	id uuid.UUID,
	claimedAttempts int,
	lastError string,
	reason string,
) {
	if _, err := w.store.MarkFailed(drainCtx, id, claimedAttempts, lastError); err != nil {
		w.handleFencedError(drainCtx, logger, id, "MarkFailed", err)
		return
	}
	if dlqErr := w.queue.DeadLetter(drainCtx, w.cfg.WorkerID, id); dlqErr != nil {
		logger.Warn("failed deadlettering failed notification", slog.String("error", dlqErr.Error()))
	}
	w.emitProcessed(ch, domain.StatusFailed)
	logger.Warn("notification failed permanently", slog.String("reason", reason), slog.String("error", lastError))
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

func errorStringValue(s *string, fallback string) string {
	if s != nil && *s != "" {
		return *s
	}
	return fallback
}

func metaFromNotification(n *domain.Notification, fallback *queue.Meta) queue.Meta {
	if n == nil {
		if fallback != nil {
			return *fallback
		}
		return queue.Meta{}
	}
	meta := queue.Meta{
		ID:       n.ID,
		Channel:  n.Channel,
		Priority: n.Priority,
	}
	if n.CorrelationID != nil {
		meta.CorrelationID = *n.CorrelationID
	} else if fallback != nil {
		meta.CorrelationID = fallback.CorrelationID
	}
	if n.TraceParent != nil {
		meta.TraceParent = *n.TraceParent
	} else if fallback != nil {
		meta.TraceParent = fallback.TraceParent
	}
	return meta
}

func (w *Worker) markActive(id uuid.UUID) {
	w.activeMu.Lock()
	w.active[id]++
	w.activeMu.Unlock()
}

func (w *Worker) unmarkActive(id uuid.UUID) {
	w.activeMu.Lock()
	if w.active[id] <= 1 {
		delete(w.active, id)
	} else {
		w.active[id]--
	}
	w.activeMu.Unlock()
}

func (w *Worker) isActive(id uuid.UUID) bool {
	w.activeMu.Lock()
	defer w.activeMu.Unlock()
	return w.active[id] > 0
}

// inflightSweepLoop repairs IDs stranded in this live worker's inflight list. That happens when a DB
// transition succeeded but the follow-up Redis write (Schedule/Ack/DeadLetter) failed, or after a
// recovered panic: the ID stays "tracked" in {notif}:msg, so reconciler steps (a)/(b) skip it, and
// step (d) skips it because this worker's heartbeat is alive. Moving it back to its queue lets the
// normal Claim path converge: claim succeeds, or Claim refuses and Ack untracks it so the reconciler
// repairs it from PostgreSQL.
func (w *Worker) inflightSweepLoop(ctx context.Context) {
	interval := w.cfg.ReconcileInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	suspects := map[uuid.UUID]struct{}{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runGuarded(ctx, "inflight_sweeper", "", func() {
				suspects = w.sweepInflightOnce(ctx, suspects)
			}, nil)
		}
	}
}

// sweepInflightOnce requeues IDs that were idle in two consecutive sweeps. The two-sweep rule covers
// the tiny window between LMOVE in Redis and markActive in this process.
func (w *Worker) sweepInflightOnce(ctx context.Context, suspects map[uuid.UUID]struct{}) map[uuid.UUID]struct{} {
	ids, err := w.queue.InflightIDs(ctx, w.cfg.WorkerID)
	if err != nil {
		if ctx.Err() == nil {
			w.logger.Warn("inflight sweep failed listing ids", slog.String("error", err.Error()))
		}
		return suspects
	}

	next := make(map[uuid.UUID]struct{})
	var stranded []uuid.UUID
	for _, id := range ids {
		if w.isActive(id) {
			continue
		}
		if _, seen := suspects[id]; seen {
			stranded = append(stranded, id)
			continue
		}
		next[id] = struct{}{}
	}
	if len(stranded) == 0 {
		return next
	}

	moved, err := w.queue.RequeueInflightIDs(ctx, w.cfg.WorkerID, stranded)
	if err != nil {
		if ctx.Err() == nil {
			w.logger.Warn("inflight sweep failed requeueing stranded ids", slog.String("error", err.Error()))
		}
		for _, id := range stranded {
			next[id] = struct{}{}
		}
		return next
	}
	w.logger.Warn("requeued stranded inflight items from live worker",
		slog.Int("stranded", len(stranded)),
		slog.Int64("moved", moved),
	)
	if w.hooks.OnInflightSwept != nil && moved > 0 {
		w.hooks.OnInflightSwept(int(moved))
	}
	return next
}

func (w *Worker) reconcilerLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runGuarded(ctx, "reconciler", "", func() {
				if err := w.reconciler.RunCycle(ctx); err != nil && ctx.Err() == nil {
					w.logger.Warn("reconciler cycle completed with errors", slog.String("error", err.Error()))
				}
			}, nil)
		}
	}
}
