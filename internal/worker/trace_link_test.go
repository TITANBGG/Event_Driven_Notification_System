package worker_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

// A batch item must start its own trace linked to the batch request instead of becoming a child of it:
// as children, one 1,000-item batch produced a 28,000-span trace (~145 MB in Jaeger). A single
// notification still continues the request's trace, which keeps the end-to-end demo intact.
// Not parallel: it swaps the global TracerProvider.
func TestBatchItemsLinkToTheRequestTraceWhileSinglesContinueIt(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	// The API request span whose traceparent is stored with the queued items.
	reqCtx, reqSpan := tp.Tracer("test").Start(context.Background(), "POST /api/v1/notifications/batch")
	traceParent := observability.InjectTraceParent(reqCtx)
	reqSpan.End()
	require.NotEmpty(t, traceParent)

	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()
	q := queue.New(rdb)
	store := newMemWorkerStore()
	batchItem, single := pendingSMS(store), pendingSMS(store)

	w, err := worker.New(worker.Deps{Config: testWorkerConfig(), Logger: observability.NewLogger("error", &bytes.Buffer{}), Store: store, Queue: q, RateLimiter: noopLimiter{}, Sender: sentSender()})
	require.NoError(t, err)
	require.NoError(t, q.Enqueue(t.Context(),
		queue.Meta{ID: batchItem, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, TraceParent: traceParent, TraceLink: true},
		queue.Meta{ID: single, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, TraceParent: traceParent},
	))
	runWorkerUntil(t, w, func() bool {
		return store.get(batchItem).Status == domain.StatusSent && store.get(single).Status == domain.StatusSent
	})

	spanFor := func(id uuid.UUID) sdktrace.ReadOnlySpan {
		for _, s := range recorder.Ended() {
			if s.Name() != "worker.process_notification" {
				continue
			}
			for _, kv := range s.Attributes() {
				if string(kv.Key) == "notification.id" && kv.Value.AsString() == id.String() {
					return s
				}
			}
		}
		t.Fatalf("no worker span for %s", id)
		return nil
	}
	reqSC := reqSpan.SpanContext()

	linked := spanFor(batchItem)
	assert.False(t, linked.Parent().IsValid(), "batch item must start a new trace")
	assert.NotEqual(t, reqSC.TraceID(), linked.SpanContext().TraceID())
	require.Len(t, linked.Links(), 1)
	assert.Equal(t, reqSC.TraceID(), linked.Links()[0].SpanContext.TraceID(), "and link back to the batch request")
	assert.Equal(t, reqSC.SpanID(), linked.Links()[0].SpanContext.SpanID())

	child := spanFor(single)
	assert.Equal(t, reqSC.TraceID(), child.SpanContext().TraceID(), "a single notification continues the request trace")
	assert.Equal(t, reqSC.SpanID(), child.Parent().SpanID())
	assert.Empty(t, child.Links())
}
