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
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

const (
	idleBackoffInitial   = 50 * time.Millisecond
	idleBackoffMax       = time.Second
	limiterErrorBackoff  = 200 * time.Millisecond
	delayedMoverBatchMax = 500
	shutdownCleanupWait  = 5 * time.Second
	circuitOpenJitterMax = 5 * time.Second
	defaultSweepInterval = 15 * time.Second

	sendTokenBudgetMargin       = 5 * time.Second
	postSendWriteBudget         = 5 * time.Second
	postSendWriteBackoffInitial = 100 * time.Millisecond
	postSendWriteBackoffMax     = time.Second

	// leaseExtendAfterWait: a rate-limit wait longer than this re-arms the row's visibility timeout
	// before the send, so wait + provider call + post-send writes never add up past the lease.
	leaseExtendAfterWait = time.Second
	// publishTimeout bounds a status-event publish so a slow Redis cannot eat into the row's lease.
	publishTimeout = time.Second
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
	Publisher   realtime.Publisher
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
	publisher   realtime.Publisher
	hooks       Hooks
	nowFn       func() time.Time
	jitterFn    func(maxJitter time.Duration) time.Duration

	// active counts IDs this process is currently handling, so the inflight sweeper can tell a
	// live item from one stranded in Redis after a failed post-transition write.
	activeMu sync.Mutex
	active   map[uuid.UUID]int

	// consumerTicks holds, per consumer goroutine, the wall time (Unix ns) it last started a loop
	// iteration. An idle consumer ticks at least once per idleBackoffMax; one item takes at most
	// ~VISIBILITY_TIMEOUT, so a tick older than livenessStallAfter means a consumer is stuck.
	consumerTicks []atomic.Int64
	running       atomic.Bool
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
	publisher := deps.Publisher
	if publisher == nil {
		publisher = realtime.NoopPublisher{}
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
		cfg:           deps.Config,
		logger:        logger,
		store:         deps.Store,
		queue:         deps.Queue,
		limiter:       deps.RateLimiter,
		sender:        deps.Sender,
		retryPolicy:   retryPolicy,
		breaker:       breaker,
		reconciler:    deps.Reconciler,
		publisher:     publisher,
		hooks:         deps.Hooks,
		nowFn:         nowFn,
		jitterFn:      jitterFn,
		active:        make(map[uuid.UUID]int),
		consumerTicks: make([]atomic.Int64, len(consumerChannels)*max(deps.Config.WorkerConcurrency, 0)),
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
	startTick := w.nowFn().UnixNano()
	for i := range w.consumerTicks {
		w.consumerTicks[i].Store(startTick)
	}
	w.running.Store(true)
	for ci, ch := range consumerChannels {
		for g := 0; g < w.cfg.WorkerConcurrency; g++ {
			consumerWg.Add(1)
			go func(channel domain.Channel, slot int, tick *atomic.Int64) {
				defer consumerWg.Done()
				w.consumerLoop(ctx, drainCtx, channel, slot, tick)
			}(ch, g, &w.consumerTicks[ci*w.cfg.WorkerConcurrency+g])
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
	w.running.Store(false)

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

func (w *Worker) consumerLoop(ctx, drainCtx context.Context, ch domain.Channel, workerNum int, tick *atomic.Int64) {
	backoff := idleBackoffInitial

	for {
		if ctx.Err() != nil {
			return
		}
		tick.Store(w.nowFn().UnixNano())

		// While the channel's breaker is open every claim would only be handed back (Claim, a
		// circuit_open attempt row and DeferCircuitOpen per item, plus a burnt GCRA token), so leave
		// the items queued in Redis and look again after the idle backoff.
		if w.breakerOpen(ch) {
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, idleBackoffMax)
			continue
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
			w.processDequeuedItem(drainCtx, ch, meta)
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
		backoff = idleBackoffInitial
	}
}

func (w *Worker) breakerOpen(ch domain.Channel) bool {
	sr, ok := w.breaker.(BreakerStateReader)
	return ok && sr.Open(ch)
}

// consumerChannels is the fixed order of consumer pools; consumerTicks is indexed by it.
var consumerChannels = [...]domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}

// livenessStallAfter is how long a consumer may go without starting a loop iteration before Live
// reports it. One item is bounded by VISIBILITY_TIMEOUT (claim, token budget, provider call and the
// post-send writes all fit in it), so twice that never flags a slow-but-healthy send.
func (w *Worker) livenessStallAfter() time.Duration {
	return max(2*w.cfg.VisibilityTimeout, 30*time.Second)
}

// Live reports a consumer goroutine that has not made progress for livenessStallAfter (deadlock,
// a call that ignores its context, a stuck lock). A worker that is not running reports nothing:
// readiness, not liveness, covers startup, and shutdown is bounded by SHUTDOWN_TIMEOUT.
func (w *Worker) Live(now time.Time) error {
	if !w.running.Load() {
		return nil
	}
	limit := w.livenessStallAfter()
	conc := max(w.cfg.WorkerConcurrency, 1)
	for i := range w.consumerTicks {
		last := time.Unix(0, w.consumerTicks[i].Load())
		if stalled := now.Sub(last); stalled > limit {
			return fmt.Errorf("consumer %s/%d made no progress for %s (limit %s)",
				consumerChannels[i/conc], i%conc, stalled.Truncate(time.Second), limit)
		}
	}
	return nil
}

// processDequeuedItem runs entirely on the drain context: once an item is dequeued, its claim, rate-limit
// wait, provider call and fenced writes may finish during graceful shutdown (bounded by SHUTDOWN_TIMEOUT).
func (w *Worker) processDequeuedItem(drainCtx context.Context, ch domain.Channel, meta *queue.Meta) {
	itemDrainCtx := observability.WithCorrelationID(drainCtx, meta.CorrelationID)
	itemDrainCtx = observability.ExtractTraceParent(itemDrainCtx, meta.TraceParent)

	tracer := observability.Tracer("notification-worker")
	baseAttrs := []attribute.KeyValue{
		attribute.String("notification.id", meta.ID.String()),
		attribute.String("notification.channel", ch.String()),
		attribute.String("notification.priority", meta.Priority.String()),
	}

	startOpts := []trace.SpanStartOption{trace.WithAttributes(baseAttrs...)}
	if meta.TraceLink {
		// Batch item: start a new trace per notification and link it to the batch request's span.
		if parent := trace.SpanContextFromContext(itemDrainCtx); parent.IsValid() {
			startOpts = append(startOpts, trace.WithNewRoot(), trace.WithLinks(trace.Link{SpanContext: parent}))
		}
	}
	itemDrainCtx, procSpan := tracer.Start(itemDrainCtx, "worker.process_notification", startOpts...)
	defer procSpan.End()

	_, dqSpan := tracer.Start(itemDrainCtx, "dequeue", trace.WithAttributes(baseAttrs...))
	dqSpan.End()

	itemLogger := observability.LoggerFrom(itemDrainCtx, w.logger).With(
		slog.String("notification_id", meta.ID.String()),
		slog.String("channel", ch.String()),
		slog.String("priority", meta.Priority.String()),
	)

	claimCtx, claimSpan := tracer.Start(itemDrainCtx, "claim", trace.WithAttributes(baseAttrs...))
	n, claimed, err := w.store.Claim(claimCtx, meta.ID, w.cfg.ClockSkewTolerance)
	if err != nil {
		claimSpan.RecordError(err)
		claimSpan.SetStatus(codes.Error, err.Error())
		claimSpan.End()
		procSpan.RecordError(err)
		procSpan.SetStatus(codes.Error, err.Error())
		itemLogger.Error("database claim failed; deferring item out of inflight list", slog.String("error", err.Error()))
		if schedErr := w.queue.Schedule(itemDrainCtx, *meta, w.nowFn().Add(time.Second), w.cfg.WorkerID); schedErr != nil {
			itemLogger.Warn("failed to defer unclaimed item", slog.String("error", schedErr.Error()))
		}
		return
	}
	if !claimed {
		claimSpan.SetAttributes(attribute.Bool("notification.claimed", false))
		claimSpan.End()
		// Cancelled, already finished by another actor, or scheduled due time not yet reached.
		if ackErr := w.queue.Ack(itemDrainCtx, w.cfg.WorkerID, meta.ID); ackErr != nil {
			itemLogger.Warn("failed to ack unclaimed notification", slog.String("error", ackErr.Error()))
		}
		return
	}
	claimSpan.SetAttributes(
		attribute.Bool("notification.claimed", true),
		attribute.Int("notification.attempt", n.Attempts),
	)
	claimSpan.End()
	procSpan.SetAttributes(attribute.Int("notification.attempt", n.Attempts))

	w.publishEvent(itemDrainCtx, itemLogger, *n, meta.CorrelationID)

	attemptLogger := itemLogger.With(
		slog.Int("attempt", n.Attempts),
		slog.String("recipient", observability.MaskRecipient(ch.String(), n.Recipient)),
	)
	if n.BatchID != nil {
		attemptLogger = attemptLogger.With(slog.String("batch_id", n.BatchID.String()))
	}

	// The GCRA token is taken AFTER Claim, immediately before the provider call. The limiter spaces
	// token grants, not sends: with the token taken before Claim, any stall in between (DB pool wait,
	// GC, CPU) let sends bunch up and break the 100/s window (measured 107 in one second with 6 worker
	// replicas while PostgreSQL refused connections). The wait is bounded so a claimed row is never
	// held past its lease; on timeout or limiter outage the claim is handed back without burning an attempt.
	waitStart := w.nowFn()
	if !w.waitForSendToken(itemDrainCtx, attemptLogger, tracer, baseAttrs, ch) {
		w.releaseUnsentClaim(itemDrainCtx, attemptLogger, n, meta, "rate limiter unavailable before send")
		return
	}
	// A long token wait used part of the lease. Re-arm it (fenced on the claimed attempt) before the
	// send; if the reconciler already reclaimed the row, do not send at all: another worker owns it,
	// and sending here would be exactly the duplicate the fence exists to prevent.
	if w.nowFn().Sub(waitStart) > leaseExtendAfterWait {
		if extErr := w.store.ExtendLease(itemDrainCtx, n.ID, n.Attempts); extErr != nil {
			if errors.Is(extErr, domain.ErrLeaseLost) {
				w.handleFencedError(itemDrainCtx, attemptLogger, n.ID, "ExtendLease", extErr)
				return
			}
			// A DB error here is not fatal: the token budget alone still keeps the send inside the lease.
			attemptLogger.Warn("failed extending lease after rate-limit wait; sending within the original lease",
				slog.String("error", extErr.Error()))
		}
	}

	startedAt := w.nowFn()
	sendSpanCtx, sendSpan := tracer.Start(itemDrainCtx, "provider.send", trace.WithAttributes(
		attribute.String("notification.id", n.ID.String()),
		attribute.String("notification.channel", ch.String()),
		attribute.String("notification.priority", n.Priority.String()),
		attribute.Int("notification.attempt", n.Attempts),
	))
	res, sendErr := w.breaker.Execute(ch, func() (provider.Result, error) {
		return w.sender.Send(sendSpanCtx, *n)
	})
	if errors.Is(sendErr, ErrCircuitOpen) {
		sendSpan.RecordError(sendErr)
		sendSpan.SetStatus(codes.Error, sendErr.Error())
		sendSpan.SetAttributes(attribute.String("outcome", domain.OutcomeCircuitOpen.String()))
		sendSpan.End()
		w.handleCircuitOpen(itemDrainCtx, attemptLogger, ch, n, meta, startedAt)
		return
	}
	if sendErr != nil {
		sendSpan.RecordError(sendErr)
		sendSpan.SetStatus(codes.Error, sendErr.Error())
		sendSpan.End()
		attemptLogger.Warn("provider execution aborted by context cancellation", slog.String("error", sendErr.Error()))
		w.recordAbortedSend(itemDrainCtx, attemptLogger, ch, n, meta, startedAt)
		return
	}
	if res.HTTPStatus != nil {
		sendSpan.SetAttributes(attribute.Int("http.response.status_code", *res.HTTPStatus))
		procSpan.SetAttributes(attribute.Int("http.response.status_code", *res.HTTPStatus))
	}
	sendSpan.SetAttributes(attribute.String("outcome", string(res.Outcome)))
	procSpan.SetAttributes(attribute.String("outcome", string(res.Outcome)))
	if res.Err != nil {
		sendSpan.RecordError(res.Err)
		sendSpan.SetStatus(codes.Error, res.Err.Error())
		procSpan.RecordError(res.Err)
	}
	sendSpan.End()

	if w.hooks.OnProviderRequest != nil {
		w.hooks.OnProviderRequest(ch, res.HTTPStatus, res.Err, res.Duration)
	}

	attemptErr := errorStringPtr(res.Err)
	durationMS := int(res.Duration.Milliseconds())
	attempt := domain.DeliveryAttempt{
		NotificationID: n.ID,
		AttemptNo:      n.Attempts,
		// A 429 is not counted: the provider rejected the rate, not the message.
		Counted:           res.Outcome != provider.OutcomeThrottled,
		StartedAt:         startedAt,
		DurationMS:        durationMS,
		HTTPStatus:        res.HTTPStatus,
		Outcome:           mapProviderOutcomeToAttempt(res.Outcome),
		Error:             attemptErr,
		ProviderMessageID: res.ProviderMessageID,
		WorkerID:          &w.cfg.WorkerID,
	}
	if insErr := w.retryPostSendWrite(itemDrainCtx, attemptLogger, "InsertAttempt", func(ctx context.Context) error {
		_, err := w.store.InsertAttempt(ctx, attempt)
		return err
	}); insErr != nil {
		attemptLogger.Warn("failed recording delivery attempt row", slog.String("error", insErr.Error()))
	}

	switch res.Outcome {
	case provider.OutcomeSent:
		stCtx, stSpan := tracer.Start(itemDrainCtx, "status.update", trace.WithAttributes(
			attribute.String("notification.id", n.ID.String()),
			attribute.String("notification.status", domain.StatusSent.String()),
		))
		// The provider has accepted the message: failing this write would leave the row in processing
		// until the reconciler re-sends it, so retry a short DB blip instead of creating a duplicate.
		var sentRow *domain.Notification
		err := w.retryPostSendWrite(stCtx, attemptLogger, "MarkSent", func(ctx context.Context) error {
			row, markErr := w.store.MarkSent(ctx, n.ID, n.Attempts, res.ProviderMessageID)
			sentRow = row
			return markErr
		})
		if err != nil {
			stSpan.RecordError(err)
			stSpan.SetStatus(codes.Error, err.Error())
			stSpan.End()
			w.handleFencedError(itemDrainCtx, attemptLogger, n.ID, "MarkSent", err)
			return
		}
		stSpan.End()
		if ackErr := w.queue.Ack(itemDrainCtx, w.cfg.WorkerID, n.ID); ackErr != nil {
			attemptLogger.Warn("failed acking sent notification", slog.String("error", ackErr.Error()))
		}
		w.publishEvent(itemDrainCtx, attemptLogger, *sentRow, meta.CorrelationID)
		if w.hooks.OnEndToEndLatency != nil && !n.CreatedAt.IsZero() {
			w.hooks.OnEndToEndLatency(ch, max(w.nowFn().Sub(n.CreatedAt), 0))
		}
		w.emitProcessed(ch, domain.StatusSent)
		attemptLogger.Info("notification sent",
			slog.String("outcome", domain.OutcomeSent.String()),
			slog.Int("duration_ms", durationMS),
		)

	case provider.OutcomeTransient:
		w.handleTransientOutcome(itemDrainCtx, attemptLogger, ch, n, meta, res, attemptErr, durationMS)

	case provider.OutcomeThrottled:
		w.handleThrottled(itemDrainCtx, attemptLogger, ch, n, meta, res, attemptErr, durationMS)

	default:
		w.failNotification(
			itemDrainCtx,
			attemptLogger,
			ch,
			n.ID,
			n.Attempts,
			errorStringValue(attemptErr, "delivery failed"),
			"permanent_failure",
			domain.OutcomePermanent,
			durationMS,
			meta.CorrelationID,
		)
	}
}

// abortedSendMessage is recorded when SHUTDOWN_TIMEOUT cancels a send already in flight.
const abortedSendMessage = "send aborted by worker shutdown; provider outcome unknown"

// recordAbortedSend writes what happened to a send cancelled by the shutdown timeout. Without it the
// row stayed in processing with no attempt row until the reconciler reset it after VISIBILITY_TIMEOUT.
// The request may already have reached the provider, so it counts as an attempt and is retried like a
// transient failure (same Idempotency-Key), on a context that survives the cancelled drain context.
func (w *Worker) recordAbortedSend(ctx context.Context, logger *slog.Logger, ch domain.Channel, n *domain.Notification, meta *queue.Meta, startedAt time.Time) {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownCleanupWait)
	defer cancel()
	msg := abortedSendMessage
	if _, err := w.store.InsertAttempt(writeCtx, domain.DeliveryAttempt{
		NotificationID: n.ID,
		AttemptNo:      n.Attempts,
		Counted:        true,
		StartedAt:      startedAt,
		DurationMS:     int(max(w.nowFn().Sub(startedAt), 0).Milliseconds()),
		Outcome:        domain.OutcomeTransient,
		Error:          &msg,
		WorkerID:       &w.cfg.WorkerID,
	}); err != nil {
		logger.Warn("failed recording aborted send attempt", slog.String("error", err.Error()))
	}
	w.handleTransientOutcome(writeCtx, logger, ch, n, meta, provider.Result{Outcome: provider.OutcomeTransient}, &msg, 0)
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
		w.failNotification(
			drainCtx,
			logger,
			ch,
			n.ID,
			n.Attempts,
			"circuit open too long",
			"circuit_open_exhausted",
			domain.OutcomeCircuitOpen,
			0,
			meta.CorrelationID,
		)
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
	w.publishEvent(drainCtx, logger, *updated, meta.CorrelationID)
	logger.Warn("notification deferred while circuit breaker is open",
		slog.String("outcome", domain.OutcomeCircuitOpen.String()),
		slog.Int("duration_ms", 0),
		slog.Int("deferrals", updated.Deferrals),
		slog.Time("next_attempt_at", nextAt),
	)
}

// handleThrottled defers a 429 like an open circuit: the fenced DeferCircuitOpen transition gives the
// attempt back (attempts-1, deferrals+1), so a long throttling period cannot exhaust MAX_ATTEMPTS and
// push healthy messages into the DLQ. The delay grows with consecutive deferrals (full-jitter
// exponential backoff, at least Retry-After) and MAX_CIRCUIT_DEFERRALS still bounds the total wait.
func (w *Worker) handleThrottled(
	drainCtx context.Context,
	logger *slog.Logger,
	ch domain.Channel,
	n *domain.Notification,
	meta *queue.Meta,
	res provider.Result,
	attemptErr *string,
	durationMS int,
) {
	errText := errorStringValue(attemptErr, "provider throttled the request (429)")
	if n.Deferrals+1 > w.cfg.MaxCircuitDeferral {
		w.failNotification(drainCtx, logger, ch, n.ID, n.Attempts, "provider throttled too long: "+errText,
			"throttled_exhausted", domain.OutcomeTransient, durationMS, meta.CorrelationID)
		return
	}
	delay := w.retryPolicy.NextDelay(n.Deferrals+1, res.RetryAfter)
	nextAt := w.nowFn().Add(delay)
	updated, err := w.store.DeferCircuitOpen(drainCtx, n.ID, n.Attempts, nextAt, errText)
	if err != nil {
		w.handleFencedError(drainCtx, logger, n.ID, "DeferCircuitOpen", err)
		return
	}
	if schedErr := w.queue.Schedule(drainCtx, metaFromNotification(updated, meta), nextAt, w.cfg.WorkerID); schedErr != nil {
		logger.Warn("failed scheduling throttled notification", slog.String("error", schedErr.Error()))
	}
	w.publishEvent(drainCtx, logger, *updated, meta.CorrelationID)
	w.emitProcessed(ch, domain.StatusRetrying)
	if w.hooks.OnRetryScheduled != nil {
		w.hooks.OnRetryScheduled(ch)
	}
	logger.Warn("provider throttled; notification deferred without using an attempt",
		slog.String("outcome", string(provider.OutcomeThrottled)),
		slog.Int("duration_ms", durationMS),
		slog.Duration("delay", delay),
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
	durationMS int,
) {
	errText := errorStringValue(attemptErr, "transient delivery failure")
	if n.Attempts < n.MaxAttempts {
		delay := w.retryPolicy.NextDelay(n.Attempts, res.RetryAfter)
		nextAt := w.nowFn().Add(delay)
		retryCtx, retrySpan := observability.Tracer("notification-worker").Start(drainCtx, "schedule.retry", trace.WithAttributes(
			attribute.String("notification.id", n.ID.String()),
			attribute.String("notification.channel", ch.String()),
			attribute.Int("notification.attempt", n.Attempts),
		))
		updated, err := w.store.ScheduleRetry(retryCtx, n.ID, n.Attempts, nextAt, errText)
		if err != nil {
			retrySpan.RecordError(err)
			retrySpan.SetStatus(codes.Error, err.Error())
			retrySpan.End()
			w.handleFencedError(drainCtx, logger, n.ID, "ScheduleRetry", err)
			return
		}
		if schedErr := w.queue.Schedule(retryCtx, metaFromNotification(updated, meta), nextAt, w.cfg.WorkerID); schedErr != nil {
			retrySpan.RecordError(schedErr)
			logger.Warn("failed scheduling notification retry", slog.String("error", schedErr.Error()))
		}
		retrySpan.End()
		w.publishEvent(drainCtx, logger, *updated, meta.CorrelationID)
		w.emitProcessed(ch, domain.StatusRetrying)
		if w.hooks.OnRetryScheduled != nil {
			w.hooks.OnRetryScheduled(ch)
		}
		logger.Warn("notification scheduled for retry",
			slog.String("outcome", domain.OutcomeTransient.String()),
			slog.Int("duration_ms", durationMS),
			slog.Duration("delay", delay),
			slog.Time("next_attempt_at", nextAt),
			slog.String("error", errText),
		)
		return
	}

	w.failNotification(
		drainCtx,
		logger,
		ch,
		n.ID,
		n.Attempts,
		"max attempts reached: "+errText,
		"max_attempts_exhausted",
		domain.OutcomeTransient,
		durationMS,
		meta.CorrelationID,
	)
}

func (w *Worker) failNotification(
	drainCtx context.Context,
	logger *slog.Logger,
	ch domain.Channel,
	id uuid.UUID,
	claimedAttempts int,
	lastError string,
	reason string,
	outcome domain.AttemptOutcome,
	durationMS int,
	correlationID string,
) {
	stCtx, stSpan := observability.Tracer("notification-worker").Start(drainCtx, "status.update", trace.WithAttributes(
		attribute.String("notification.id", id.String()),
		attribute.String("notification.status", domain.StatusFailed.String()),
		attribute.String("outcome", outcome.String()),
	))
	failedRow, err := w.store.MarkFailed(stCtx, id, claimedAttempts, lastError)
	if err != nil {
		stSpan.RecordError(err)
		stSpan.SetStatus(codes.Error, err.Error())
		stSpan.End()
		w.handleFencedError(drainCtx, logger, id, "MarkFailed", err)
		return
	}
	stSpan.End()
	if dlqErr := w.queue.DeadLetter(drainCtx, w.cfg.WorkerID, id); dlqErr != nil {
		logger.Warn("failed deadlettering failed notification", slog.String("error", dlqErr.Error()))
	}
	w.publishEvent(drainCtx, logger, *failedRow, correlationID)
	w.emitProcessed(ch, domain.StatusFailed)
	logger.Warn("notification failed permanently",
		slog.String("outcome", outcome.String()),
		slog.Int("duration_ms", durationMS),
		slog.String("reason", reason),
		slog.String("error", lastError),
	)
}

func (w *Worker) publishEvent(ctx context.Context, logger *slog.Logger, n domain.Notification, correlationID string) {
	if w.publisher == nil {
		return
	}
	// Best effort and bounded: the publish after Claim runs inside the row's lease.
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := w.publisher.Publish(ctx, realtime.NewEvent(n, w.nowFn(), correlationID)); err != nil {
		logger.Warn("failed publishing realtime status event", slog.String("error", err.Error()))
	}
}

// sendTokenBudget bounds the rate-limit wait of a claimed row. The provider call that follows may take
// up to PROVIDER_TIMEOUT, so the wait must leave room for it plus a margin before VISIBILITY_TIMEOUT,
// after which the reconciler may hand the row to another worker. The startup guard
// VISIBILITY_TIMEOUT >= 2*PROVIDER_TIMEOUT + 15s keeps this at 10s or more with valid config.
func (w *Worker) sendTokenBudget() time.Duration {
	return max(w.cfg.VisibilityTimeout-2*w.cfg.ProviderTimeout-sendTokenBudgetMargin, time.Second)
}

// waitForSendToken blocks for a GCRA token of channel ch. It uses the drain context so a claimed row
// still gets sent during graceful shutdown, and fails closed (returns false) when the limiter is
// unavailable for the whole budget.
func (w *Worker) waitForSendToken(
	ctx context.Context,
	logger *slog.Logger,
	tracer trace.Tracer,
	attrs []attribute.KeyValue,
	ch domain.Channel,
) bool {
	waitCtx, cancel := context.WithTimeout(ctx, w.sendTokenBudget())
	defer cancel()
	waitCtx, span := tracer.Start(waitCtx, "ratelimit.wait", trace.WithAttributes(attrs...))
	defer span.End()
	for {
		err := w.limiter.Wait(waitCtx, ch)
		if err == nil {
			return true
		}
		if waitCtx.Err() == nil {
			logger.Warn("rate limiter error; failing closed and backing off before send", slog.String("error", err.Error()))
			if waitWithContext(waitCtx, limiterErrorBackoff) {
				continue
			}
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "no rate-limit token within budget")
		return false
	}
}

// releaseUnsentClaim hands a claimed row back when it could not be sent. DeferCircuitOpen is the
// existing fenced "not attempted" transition (processing -> retrying, attempts-1, deferrals+1), so no
// attempt is burned; the row is retried after a second through the delayed set.
func (w *Worker) releaseUnsentClaim(ctx context.Context, logger *slog.Logger, n *domain.Notification, meta *queue.Meta, reason string) {
	// The drain context may already be cancelled (shutdown timeout); the hand-back must still be written.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownCleanupWait)
	defer cancel()
	nextAt := w.nowFn().Add(time.Second)
	updated, err := w.store.DeferCircuitOpen(writeCtx, n.ID, n.Attempts, nextAt, reason)
	if err != nil {
		w.handleFencedError(writeCtx, logger, n.ID, "DeferCircuitOpen", err)
		return
	}
	if schedErr := w.queue.Schedule(writeCtx, metaFromNotification(updated, meta), nextAt, w.cfg.WorkerID); schedErr != nil {
		logger.Warn("failed scheduling unsent claim", slog.String("error", schedErr.Error()))
	}
	logger.Warn("claimed notification released without sending", slog.String("reason", reason))
}

// retryPostSendWrite retries a database write that follows a provider call for up to
// postSendWriteBudget. Giving up leaves the row in processing until the reconciler re-sends it after
// VISIBILITY_TIMEOUT, so a short DB blip (pool exhaustion, failover) would otherwise turn an accepted
// send into a duplicate. A lost lease is final and is never retried.
func (w *Worker) retryPostSendWrite(ctx context.Context, logger *slog.Logger, op string, write func(context.Context) error) error {
	writeCtx, cancel := context.WithTimeout(ctx, postSendWriteBudget)
	defer cancel()
	backoff := postSendWriteBackoffInitial
	for retry := 1; ; retry++ {
		err := write(writeCtx)
		if err == nil || errors.Is(err, domain.ErrLeaseLost) {
			return err
		}
		if !waitWithContext(writeCtx, backoff) {
			return err
		}
		logger.Warn("retrying database write after provider call",
			slog.String("op", op), slog.Int("retry", retry), slog.String("error", err.Error()))
		backoff = min(backoff*2, postSendWriteBackoffMax)
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
	// Derived from the row, not only the fallback, so items rebuilt by the reconciler after Redis data
	// loss keep linking instead of growing the batch request's trace.
	meta.TraceLink = n.BatchID != nil || (fallback != nil && fallback.TraceLink)
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
