package worker

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
)

const (
	defaultCBConsecutiveFail = 10
	defaultCBMinRequests     = 20
	defaultCBFailureRatio    = 0.5
	defaultCBInterval        = 60 * time.Second
	defaultCBOpenTimeout     = 30 * time.Second
	defaultCBHalfOpenMax     = 3
)

// transientBreakerError carries a transient provider Result back through gobreaker so only
// transient failures count against circuit health while permanent 4xx errors and 429 throttling
// (a healthy provider asking us to slow down) count as successes.
type transientBreakerError struct {
	res provider.Result
}

func (e transientBreakerError) Error() string {
	if e.res.Err != nil {
		return e.res.Err.Error()
	}
	return "transient provider error"
}

// Deliberately no Unwrap: a provider timeout wraps context.DeadlineExceeded, and exposing it would
// let IsExcluded drop exactly the slow-provider failures the breaker exists to detect.

// abortedCallError marks a Send aborted by the worker's own drain/shutdown cancellation, the only
// error class excluded from breaker counts.
type abortedCallError struct {
	err error
}

func (e abortedCallError) Error() string { return e.err.Error() }

func (e abortedCallError) Unwrap() error { return e.err }

// ChannelBreaker isolates delivery health per channel using an in-memory sony/gobreaker/v2 instance
// for each notification channel.
type ChannelBreaker struct {
	breakers map[domain.Channel]*gobreaker.CircuitBreaker[provider.Result]
}

// ChannelBreakerConfig configures the per-channel circuit breakers and optional state-change hook.
type ChannelBreakerConfig struct {
	Config        config.Config
	Logger        *slog.Logger
	OnStateChange func(ch domain.Channel, from, to string)
}

// NewChannelBreaker creates an isolated circuit breaker for sms, email, and push channels.
func NewChannelBreaker(cfg ChannelBreakerConfig) *ChannelBreaker {
	logger := cfg.Logger
	if logger == nil {
		logger = observability.NewLogger("info", os.Stdout)
	}

	consecutiveFail := cfg.Config.CBConsecutiveFail
	if consecutiveFail < 1 {
		consecutiveFail = defaultCBConsecutiveFail
	}
	minRequests := cfg.Config.CBMinRequests
	if minRequests < 1 {
		minRequests = defaultCBMinRequests
	}
	failureRatio := cfg.Config.CBFailureRatio
	if failureRatio <= 0 || failureRatio > 1 {
		failureRatio = defaultCBFailureRatio
	}
	interval := cfg.Config.CBInterval
	if interval <= 0 {
		interval = defaultCBInterval
	}
	openTimeout := cfg.Config.CBOpenTimeout
	if openTimeout <= 0 {
		openTimeout = defaultCBOpenTimeout
	}
	halfOpenMax := cfg.Config.CBHalfOpenMax
	if halfOpenMax < 1 {
		halfOpenMax = defaultCBHalfOpenMax
	}

	// #nosec G115 -- bounds checked above; safe conversion to uint32 for gobreaker settings.
	consecutiveLimit := uint32(consecutiveFail)
	// #nosec G115 -- bounds checked above; safe conversion to uint32 for gobreaker settings.
	minRequestLimit := uint32(minRequests)
	// #nosec G115 -- bounds checked above; safe conversion to uint32 for gobreaker settings.
	halfOpenLimit := uint32(halfOpenMax)

	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	breakers := make(map[domain.Channel]*gobreaker.CircuitBreaker[provider.Result], len(channels))

	for _, ch := range channels {
		channel := ch
		st := gobreaker.Settings{
			Name:        fmt.Sprintf("provider_%s", channel),
			MaxRequests: halfOpenLimit,
			Interval:    interval,
			Timeout:     openTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				if counts.ConsecutiveFailures >= consecutiveLimit {
					return true
				}
				var validRequests uint32
				if counts.Requests > counts.TotalExclusions {
					validRequests = counts.Requests - counts.TotalExclusions
				}
				if validRequests >= minRequestLimit &&
					float64(counts.TotalFailures)/float64(validRequests) > failureRatio {
					return true
				}
				return false
			},
			IsSuccessful: func(err error) bool {
				// Permanent 4xx rejections execute cleanly (err == nil) so bad payloads never trip the breaker.
				return err == nil
			},
			IsExcluded: func(err error) bool {
				// Only local drain aborts are excluded; transient provider outcomes (including HTTP
				// timeouts that wrap context.DeadlineExceeded) must always count as failures.
				var aborted abortedCallError
				return errors.As(err, &aborted)
			},
			OnStateChange: func(_ string, from, to gobreaker.State) {
				fromLabel := breakerStateLabel(from)
				toLabel := breakerStateLabel(to)
				logger.Warn("circuit breaker state changed",
					slog.String("channel", channel.String()),
					slog.String("from", fromLabel),
					slog.String("to", toLabel),
				)
				if cfg.OnStateChange != nil {
					cfg.OnStateChange(channel, fromLabel, toLabel)
				}
			},
		}
		breakers[channel] = gobreaker.NewCircuitBreaker[provider.Result](st)
	}

	return &ChannelBreaker{breakers: breakers}
}

// Execute runs fn through ch's circuit breaker and translates open/probe-saturated breaker states
// into ErrCircuitOpen without invoking fn.
func (b *ChannelBreaker) Execute(ch domain.Channel, fn func() (provider.Result, error)) (provider.Result, error) {
	cb, ok := b.breakers[ch]
	if !ok {
		return fn()
	}

	res, err := cb.Execute(func() (provider.Result, error) {
		out, callErr := fn()
		if callErr != nil {
			// Send only returns a non-nil error when the caller's own context was cancelled.
			return out, abortedCallError{err: callErr}
		}
		if out.Outcome == provider.OutcomeTransient || isProviderAuthRejection(out) {
			return out, transientBreakerError{res: out}
		}
		return out, nil
	})
	if err == nil {
		return res, nil
	}

	var transient transientBreakerError
	if errors.As(err, &transient) {
		return transient.res, nil
	}
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return provider.Result{}, ErrCircuitOpen
	}
	var aborted abortedCallError
	if errors.As(err, &aborted) {
		return res, aborted.err
	}
	return res, err
}

// isProviderAuthRejection reports a permanent 401/403/407: our credentials or the provider URL are
// wrong, which no payload change can fix and which affects every message on the channel. Counting it
// as a breaker failure opens the circuit after CB_CONSECUTIVE_FAILURES, so only that many messages
// reach the DLQ before consumers stop dequeuing, instead of the whole queue draining into it.
// The message outcome itself stays Permanent.
func isProviderAuthRejection(res provider.Result) bool {
	if res.Outcome != provider.OutcomePermanent || res.HTTPStatus == nil {
		return false
	}
	switch *res.HTTPStatus {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusProxyAuthRequired:
		return true
	}
	return false
}

// Open reports whether ch's breaker is open (not half-open). gobreaker moves an expired open state
// to half-open inside State, so consumers resume probing on their own after CB_OPEN_TIMEOUT.
func (b *ChannelBreaker) Open(ch domain.Channel) bool {
	return b.State(ch) == gobreaker.StateOpen
}

// State returns the current breaker state for ch (StateClosed if unknown).
func (b *ChannelBreaker) State(ch domain.Channel) gobreaker.State {
	cb, ok := b.breakers[ch]
	if !ok {
		return gobreaker.StateClosed
	}
	return cb.State()
}

func breakerStateLabel(st gobreaker.State) string {
	switch st {
	case gobreaker.StateClosed:
		return "closed"
	case gobreaker.StateHalfOpen:
		return "half_open"
	case gobreaker.StateOpen:
		return "open"
	default:
		return "unknown"
	}
}
