package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/TITANBGG/notification-system/internal/observability"
)

func TestInjectExtractTraceParentRoundTripAndSlogTraceID(t *testing.T) {
	// Set up an in-memory recording TracerProvider for deterministic span generation.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)

	ctx, parentSpan := tp.Tracer("test").Start(t.Context(), "api.create_notification")
	parentSC := parentSpan.SpanContext()
	require.True(t, parentSC.IsValid())

	// 1. Inject active span context into a W3C traceparent string (stored in queue.Meta.TraceParent).
	traceparent := observability.InjectTraceParent(ctx)
	require.NotEmpty(t, traceparent)
	assert.Contains(t, traceparent, parentSC.TraceID().String())
	parentSpan.End()

	// 2. Extract traceparent into a fresh background context and verify a child span inherits the same TraceID.
	extractedCtx := observability.ExtractTraceParent(context.Background(), traceparent)
	remoteSC := trace.SpanContextFromContext(extractedCtx)
	require.True(t, remoteSC.IsValid())
	assert.True(t, remoteSC.IsRemote())
	assert.Equal(t, parentSC.TraceID(), remoteSC.TraceID())
	assert.Equal(t, parentSC.SpanID(), remoteSC.SpanID())

	childCtx, childSpan := tp.Tracer("test").Start(extractedCtx, "worker.process")
	childSC := childSpan.SpanContext()
	assert.Equal(t, parentSC.TraceID(), childSC.TraceID())
	assert.NotEqual(t, parentSC.SpanID(), childSC.SpanID())

	// 3. Verify slog handler wrapper adds trace_id and span_id when a span is active, and omits them otherwise.
	var activeBuf bytes.Buffer
	logger := observability.NewLogger("info", &activeBuf)
	logger.InfoContext(childCtx, "processing with active span")
	childSpan.End()

	var activeLog map[string]any
	require.NoError(t, json.Unmarshal(activeBuf.Bytes(), &activeLog))
	assert.Equal(t, childSC.TraceID().String(), activeLog["trace_id"])
	assert.Equal(t, childSC.SpanID().String(), activeLog["span_id"])

	var inactiveBuf bytes.Buffer
	inactiveLogger := observability.NewLogger("info", &inactiveBuf)
	inactiveLogger.InfoContext(context.Background(), "log without span")

	var inactiveLog map[string]any
	require.NoError(t, json.Unmarshal(inactiveBuf.Bytes(), &inactiveLog))
	_, hasTraceID := inactiveLog["trace_id"]
	_, hasSpanID := inactiveLog["span_id"]
	assert.False(t, hasTraceID, "trace_id must not be logged when no span is active")
	assert.False(t, hasSpanID, "span_id must not be logged when no span is active")

	// 4. Also verify LoggerFrom(childCtx, base) attaches trace_id and span_id for Info(...) calls.
	var fromBuf bytes.Buffer
	fromBase := observability.NewLogger("info", &fromBuf)
	observability.LoggerFrom(childCtx, fromBase).Info("logged via LoggerFrom")
	var fromLog map[string]any
	require.NoError(t, json.Unmarshal(fromBuf.Bytes(), &fromLog))
	assert.Equal(t, childSC.TraceID().String(), fromLog["trace_id"])
	assert.Equal(t, childSC.SpanID().String(), fromLog["span_id"])

	// 5. Verify InitTracing with empty endpoint installs no-op provider cleanly.
	shutdown, err := observability.InitTracing(t.Context(), observability.TracingConfig{
		ServiceName: "notification-test",
		Endpoint:    "",
	})
	require.NoError(t, err)
	require.NoError(t, shutdown(t.Context()))
}
