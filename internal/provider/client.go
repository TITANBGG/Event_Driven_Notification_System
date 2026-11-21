package provider

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
)

const maxResponseBodyBytes = 64 << 10

// OutcomeKind classifies an HTTP delivery result for worker retry and breaker policies.
type OutcomeKind string

// Supported provider delivery classification outcomes.
const (
	OutcomeSent      OutcomeKind = "sent"
	OutcomeTransient OutcomeKind = "transient"
	OutcomePermanent OutcomeKind = "permanent"
)

// Result summarizes the outcome, latency, HTTP status, and parsed metadata of one provider call.
type Result struct {
	Outcome           OutcomeKind
	HTTPStatus        *int
	ProviderMessageID *string
	RetryAfter        *time.Duration
	Duration          time.Duration
	Err               error
}

// ClientConfig tunes the external provider HTTP client and connection pool.
type ClientConfig struct {
	URL            string
	Timeout        time.Duration
	ExtendedFields bool
	MaxIdleConns   int
	Logger         *slog.Logger
}

// Client sends notification requests over HTTP with tuned connection pooling and strict
// response/redirect classification per the EXT-1 contract.
type Client struct {
	endpoint       string
	timeout        time.Duration
	extendedFields bool
	httpClient     *http.Client
	logger         *slog.Logger
	nowFn          func() time.Time
}

type basePayload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type extendedPayload struct {
	To      string  `json:"to"`
	Channel string  `json:"channel"`
	Content string  `json:"content"`
	Subject *string `json:"subject,omitempty"`
	Title   *string `json:"title,omitempty"`
}

type acceptedBody struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// NewClient constructs a pooled provider HTTP client that never follows 3xx redirects.
func NewClient(cfg ClientConfig) *Client {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 100
	}
	logger := cfg.Logger
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: maxIdle,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}

	return &Client{
		endpoint:       strings.TrimSpace(cfg.URL),
		timeout:        timeout,
		extendedFields: cfg.ExtendedFields,
		logger:         logger,
		nowFn:          func() time.Time { return time.Now().UTC() },
		httpClient: &http.Client{
			Transport: otelhttp.NewTransport(transport),
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Send dispatches a notification to the external provider and classifies the response into
// Sent, Transient (with optional Retry-After), or Permanent.
func (c *Client) Send(ctx context.Context, n domain.Notification) (Result, error) {
	start := time.Now()
	cid := observability.CorrelationID(ctx)
	if cid == "" && n.CorrelationID != nil {
		cid = *n.CorrelationID
	}

	parsed, err := url.Parse(c.endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return Result{
			Outcome:  OutcomePermanent,
			Duration: time.Since(start),
			Err:      fmt.Errorf("invalid provider URL %q", c.endpoint),
		}, nil
	}

	var bodyObj any = basePayload{
		To:      n.Recipient,
		Channel: n.Channel.String(),
		Content: n.Content,
	}
	if c.extendedFields {
		bodyObj = extendedPayload{
			To:      n.Recipient,
			Channel: n.Channel.String(),
			Content: n.Content,
			Subject: n.Subject,
			Title:   n.Title,
		}
	}

	rawBody, err := json.Marshal(bodyObj)
	if err != nil {
		return Result{
			Outcome:  OutcomePermanent,
			Duration: time.Since(start),
			Err:      fmt.Errorf("marshaling provider payload: %w", err),
		}, nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	//nolint:gosec // G704: provider endpoint URL comes from validated startup configuration.
	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, parsed.String(), bytes.NewReader(rawBody))
	if err != nil {
		return Result{
			Outcome:  OutcomePermanent,
			Duration: time.Since(start),
			Err:      fmt.Errorf("building provider request: %w", err),
		}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.ID.String())
	if cid != "" {
		req.Header.Set("X-Correlation-ID", cid)
	}
	if tp := observability.InjectTraceParent(sendCtx); tp != "" {
		req.Header.Set("traceparent", tp)
	} else if n.TraceParent != nil && strings.TrimSpace(*n.TraceParent) != "" {
		req.Header.Set("traceparent", strings.TrimSpace(*n.TraceParent))
	}

	//nolint:gosec // G704: provider endpoint URL comes from validated startup configuration.
	resp, doErr := c.httpClient.Do(req)
	elapsed := time.Since(start)
	if doErr != nil {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		return c.classifyTransportError(doErr, elapsed), nil
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBodyBytes))
		_ = resp.Body.Close()
	}()

	bodyBytes, readErr := readBounded(resp.Body)
	elapsed = time.Since(start)
	if readErr != nil && ctx.Err() != nil {
		return Result{}, ctx.Err()
	}

	status := resp.StatusCode
	statusPtr := &status

	if status >= 200 && status < 300 {
		return c.handleSuccessResponse(ctx, n, status, bodyBytes, elapsed, readErr), nil
	}

	if status >= 300 && status < 400 {
		return Result{
			Outcome:    OutcomePermanent,
			HTTPStatus: statusPtr,
			Duration:   elapsed,
			Err:        fmt.Errorf("provider returned unexpected redirect HTTP %d", status),
		}, nil
	}

	if status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests {
		return Result{
			Outcome:    OutcomeTransient,
			HTTPStatus: statusPtr,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.nowFn()),
			Duration:   elapsed,
			Err:        fmt.Errorf("provider returned transient HTTP %d", status),
		}, nil
	}

	if status >= 400 && status < 500 {
		return Result{
			Outcome:    OutcomePermanent,
			HTTPStatus: statusPtr,
			Duration:   elapsed,
			Err:        fmt.Errorf("provider rejected request with HTTP %d", status),
		}, nil
	}

	// 5xx or any remaining HTTP status triggers transient retries (honoring Retry-After on 503).
	return Result{
		Outcome:    OutcomeTransient,
		HTTPStatus: statusPtr,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.nowFn()),
		Duration:   elapsed,
		Err:        fmt.Errorf("provider returned server error HTTP %d", status),
	}, nil
}

func (c *Client) handleSuccessResponse(
	ctx context.Context,
	n domain.Notification,
	status int,
	bodyBytes []byte,
	elapsed time.Duration,
	readErr error,
) Result {
	reqLogger := observability.LoggerFrom(ctx, c.logger)
	var msgID *string

	if status != http.StatusAccepted {
		reqLogger.Warn("provider returned non-202 2xx status",
			slog.String("notification_id", n.ID.String()),
			slog.Int("http_status", status),
		)
	}

	if readErr != nil {
		reqLogger.Warn("failed reading provider 2xx response body; treating delivery as sent",
			slog.String("notification_id", n.ID.String()),
			slog.Int("http_status", status),
			slog.String("error", readErr.Error()),
		)
	} else if len(bytes.TrimSpace(bodyBytes)) == 0 {
		reqLogger.Warn("provider returned empty 2xx response body; treating delivery as sent",
			slog.String("notification_id", n.ID.String()),
			slog.Int("http_status", status),
		)
	} else {
		var parsed acceptedBody
		if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
			reqLogger.Warn("provider returned invalid JSON on 2xx response; treating delivery as sent",
				slog.String("notification_id", n.ID.String()),
				slog.Int("http_status", status),
				slog.String("error", err.Error()),
			)
		} else if trimmed := strings.TrimSpace(parsed.MessageID); trimmed != "" {
			msgID = &trimmed
		}
	}

	return Result{
		Outcome:           OutcomeSent,
		HTTPStatus:        &status,
		ProviderMessageID: msgID,
		Duration:          elapsed,
	}
}

func (c *Client) classifyTransportError(err error, elapsed time.Duration) Result {
	var (
		certInvalid x509.CertificateInvalidError
		unknownAuth x509.UnknownAuthorityError
		hostErr     x509.HostnameError
		tlsHeader   tls.RecordHeaderError
	)
	if errors.As(err, &certInvalid) ||
		errors.As(err, &unknownAuth) ||
		errors.As(err, &hostErr) ||
		errors.As(err, &tlsHeader) {
		return Result{
			Outcome:  OutcomePermanent,
			Duration: elapsed,
			Err:      fmt.Errorf("provider TLS certificate error: %w", err),
		}
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return Result{
			Outcome:  OutcomeTransient,
			Duration: elapsed,
			Err:      fmt.Errorf("provider request timed out: %w", err),
		}
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return Result{
			Outcome:  OutcomeTransient,
			Duration: elapsed,
			Err:      fmt.Errorf("provider network timeout: %w", err),
		}
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) {
		return Result{
			Outcome:  OutcomeTransient,
			Duration: elapsed,
			Err:      fmt.Errorf("provider connection error: %w", err),
		}
	}

	var dnsErr *net.DNSError
	var opErr *net.OpError
	if errors.As(err, &dnsErr) || errors.As(err, &opErr) {
		return Result{
			Outcome:  OutcomeTransient,
			Duration: elapsed,
			Err:      fmt.Errorf("provider network error: %w", err),
		}
	}

	return Result{
		Outcome:  OutcomeTransient,
		Duration: elapsed,
		Err:      fmt.Errorf("provider transport error: %w", err),
	}
}

// maxRetryAfterSeconds is the largest whole-second Retry-After that fits in a time.Duration.
const maxRetryAfterSeconds = int(math.MaxInt64 / int64(time.Second))

func parseRetryAfter(raw string, now time.Time) *time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	if secs, err := strconv.Atoi(trimmed); err == nil {
		if secs < 0 {
			secs = 0
		}
		// Clamp before multiplying: fuzzing found "9227000000" overflowed time.Duration into a negative
		// delay. The retry policy caps the hint at RETRY_AFTER_MAX anyway.
		if secs > maxRetryAfterSeconds {
			secs = maxRetryAfterSeconds
		}
		d := time.Duration(secs) * time.Second
		return &d
	}
	if target, err := http.ParseTime(trimmed); err == nil {
		d := target.UTC().Sub(now.UTC())
		if d < 0 {
			d = 0
		}
		return &d
	}
	return nil
}

func readBounded(body io.ReadCloser) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	return io.ReadAll(io.LimitReader(body, maxResponseBodyBytes))
}
