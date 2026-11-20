package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/worker"
)

func TestChannelBreakerPermanentErrorsIgnoredAndConsecutiveTransientTripsAndRecovers(t *testing.T) {
	t.Parallel()

	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.CBConsecutiveFail = 10
	cfg.CBMinRequests = 20
	cfg.CBFailureRatio = 0.5
	cfg.CBInterval = time.Minute
	cfg.CBOpenTimeout = 80 * time.Millisecond
	cfg.CBHalfOpenMax = 3

	var mu sync.Mutex
	var transitions []string
	cb := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
		Config: cfg,
		Logger: observability.NewLogger("error", io.Discard),
		OnStateChange: func(ch domain.Channel, from, to string) {
			mu.Lock()
			transitions = append(transitions, string(ch)+":"+from+"->"+to)
			mu.Unlock()
		},
	})

	// Permanent 4xx errors do not count as breaker failures.
	for i := 0; i < 15; i++ {
		res, err := cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomePermanent, Err: errors.New("400")}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, provider.OutcomePermanent, res.Outcome)
	}
	assert.Equal(t, gobreaker.StateClosed, cb.State(domain.ChannelSMS))

	// 9 transient errors keep circuit closed; 10th consecutive transient failure opens SMS only.
	for i := 0; i < 9; i++ {
		res, err := cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeTransient, Err: errors.New("503")}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, provider.OutcomeTransient, res.Outcome)
		assert.Equal(t, gobreaker.StateClosed, cb.State(domain.ChannelSMS))
	}
	res, err := cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
		return provider.Result{Outcome: provider.OutcomeTransient, Err: errors.New("503")}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, provider.OutcomeTransient, res.Outcome)
	assert.Equal(t, gobreaker.StateOpen, cb.State(domain.ChannelSMS))
	assert.Equal(t, gobreaker.StateClosed, cb.State(domain.ChannelEmail))

	// While SMS is open, provider closure is never invoked and ErrCircuitOpen is returned;
	// email channel continues sending normally.
	providerCalled := false
	_, err = cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
		providerCalled = true
		return provider.Result{Outcome: provider.OutcomeSent}, nil
	})
	require.ErrorIs(t, err, worker.ErrCircuitOpen)
	assert.False(t, providerCalled)

	emailRes, err := cb.Execute(domain.ChannelEmail, func() (provider.Result, error) {
		return provider.Result{Outcome: provider.OutcomeSent}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, provider.OutcomeSent, emailRes.Outcome)

	// After open timeout, 3 successful half-open probes close the circuit.
	assert.Eventually(t, func() bool {
		probeRes, probeErr := cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeSent}, nil
		})
		return probeErr == nil && probeRes.Outcome == provider.OutcomeSent
	}, time.Second, 20*time.Millisecond)

	for i := 0; i < 2; i++ {
		probeRes, probeErr := cb.Execute(domain.ChannelSMS, func() (provider.Result, error) {
			return provider.Result{Outcome: provider.OutcomeSent}, nil
		})
		require.NoError(t, probeErr)
		assert.Equal(t, provider.OutcomeSent, probeRes.Outcome)
	}
	assert.Equal(t, gobreaker.StateClosed, cb.State(domain.ChannelSMS))

	mu.Lock()
	assert.Equal(t, []string{
		"sms:closed->open",
		"sms:open->half_open",
		"sms:half_open->closed",
	}, transitions)
	mu.Unlock()
}

func TestChannelBreakerFailureRatioAndContextExclusion(t *testing.T) {
	t.Parallel()

	cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
	cfg.CBConsecutiveFail = 50
	cfg.CBMinRequests = 20
	cfg.CBFailureRatio = 0.5
	cfg.CBInterval = time.Minute
	cfg.CBOpenTimeout = 100 * time.Millisecond
	cfg.CBHalfOpenMax = 2

	cb := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
		Config: cfg,
		Logger: observability.NewLogger("error", io.Discard),
	})

	// Context cancellations during drain must never count toward circuit failures.
	for i := 0; i < 25; i++ {
		_, err := cb.Execute(domain.ChannelPush, func() (provider.Result, error) {
			return provider.Result{}, context.Canceled
		})
		require.ErrorIs(t, err, context.Canceled)
	}
	assert.Equal(t, gobreaker.StateClosed, cb.State(domain.ChannelPush))

	// Interleave 9 successes and 11 failures (= 20 requests, 55% failure ratio > 0.5) without 10 consecutive.
	for i := 0; i < 20; i++ {
		outcome := provider.OutcomeTransient
		if i%2 == 0 && i < 18 {
			outcome = provider.OutcomeSent
		}
		_, err := cb.Execute(domain.ChannelPush, func() (provider.Result, error) {
			return provider.Result{Outcome: outcome}, nil
		})
		require.NoError(t, err)
	}
	assert.Equal(t, gobreaker.StateOpen, cb.State(domain.ChannelPush))
}

// Regression: provider HTTP timeouts wrap context.DeadlineExceeded; they must still trip the breaker
// while the worker's own drain cancellation (Send returning an error) stays excluded.
func TestChannelBreakerProviderTimeoutsTripButDrainAbortsDoNot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		call      func() (provider.Result, error)
		wantState gobreaker.State
	}{
		{
			name: "provider timeout counts as failure",
			call: func() (provider.Result, error) {
				timeoutErr := fmt.Errorf("provider request timed out: %w",
					&url.Error{Op: "Post", URL: "http://mockprovider:8090/", Err: context.DeadlineExceeded})
				return provider.Result{Outcome: provider.OutcomeTransient, Err: timeoutErr}, nil
			},
			wantState: gobreaker.StateOpen,
		},
		{
			name: "drain cancellation is excluded",
			call: func() (provider.Result, error) {
				return provider.Result{}, context.Canceled
			},
			wantState: gobreaker.StateClosed,
		},
		{
			name: "drain deadline is excluded",
			call: func() (provider.Result, error) {
				return provider.Result{}, context.DeadlineExceeded
			},
			wantState: gobreaker.StateClosed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, _ := config.LoadFrom(func(string) (string, bool) { return "", false })
			cb := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
				Config: cfg,
				Logger: observability.NewLogger("error", io.Discard),
			})
			for i := 0; i < cfg.CBConsecutiveFail; i++ {
				res, err := cb.Execute(domain.ChannelSMS, tc.call)
				if tc.wantState == gobreaker.StateOpen {
					require.NoError(t, err)
					assert.Equal(t, provider.OutcomeTransient, res.Outcome)
				} else {
					require.Error(t, err)
					assert.False(t, errors.Is(err, worker.ErrCircuitOpen))
				}
			}
			assert.Equal(t, tc.wantState, cb.State(domain.ChannelSMS))
		})
	}
}
