package observability

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime/debug"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const tracerScopeName = "github.com/TITANBGG/notification-system"

var w3cPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// TracingConfig configures OpenTelemetry TracerProvider initialization.
type TracingConfig struct {
	ServiceName  string
	Endpoint     string
	SamplerRatio float64
	Environment  string
}

// InitTracing initializes the global OpenTelemetry W3C TraceContext + Baggage propagator and
// TracerProvider. When Endpoint is empty, a zero-overhead no-op TracerProvider is installed.
// The returned shutdown function flushes pending spans on process exit.
func InitTracing(ctx context.Context, cfg TracingConfig) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(w3cPropagator)

	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	opts := buildOTLPHTTPOptions(endpoint)
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating otlptracehttp exporter: %w", err)
	}

	serviceName := strings.TrimSpace(cfg.ServiceName)
	if serviceName == "" {
		serviceName = "notification-service"
	}
	env := strings.TrimSpace(cfg.Environment)
	if env == "" {
		env = strings.TrimSpace(os.Getenv("DEPLOYMENT_ENVIRONMENT"))
		if env == "" {
			env = "local"
		}
	}

	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(buildVersion()),
			semconv.DeploymentEnvironmentKey.String(env),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating otel resource: %w", err)
	}

	ratio := cfg.SamplerRatio
	if ratio < 0 {
		ratio = 0
	} else if ratio > 1 {
		ratio = 1
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns an application tracer from the global TracerProvider.
func Tracer(name string) trace.Tracer {
	if strings.TrimSpace(name) == "" {
		name = tracerScopeName
	}
	return otel.Tracer(name)
}

// InjectTraceParent serializes the active span context from ctx into a W3C `traceparent` string
// for propagation through Redis queue metadata and PostgreSQL storage.
// Returns "" when ctx has no valid active span context.
func InjectTraceParent(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	carrier := propagation.MapCarrier{}
	w3cPropagator.Inject(ctx, carrier)
	return strings.TrimSpace(carrier.Get("traceparent"))
}

// ExtractTraceParent reconstructs a context carrying the remote parent span context from a W3C
// `traceparent` string so worker processing spans become children of the originating API span.
func ExtractTraceParent(ctx context.Context, traceParent string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	tp := strings.TrimSpace(traceParent)
	if tp == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{
		"traceparent": tp,
	}
	return w3cPropagator.Extract(ctx, carrier)
}

func buildOTLPHTTPOptions(rawEndpoint string) []otlptracehttp.Option {
	if strings.Contains(rawEndpoint, "://") {
		if u, err := url.Parse(rawEndpoint); err == nil && u.Host != "" {
			opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
			if u.Scheme == "http" {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
			if u.Path != "" && u.Path != "/" {
				opts = append(opts, otlptracehttp.WithURLPath(u.Path))
			}
			return opts
		}
	}
	return []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(rawEndpoint),
		otlptracehttp.WithInsecure(),
	}
}

func buildVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok && info != nil {
		if v := strings.TrimSpace(info.Main.Version); v != "" && v != "(devel)" {
			return v
		}
	}
	return "0.3.0"
}
