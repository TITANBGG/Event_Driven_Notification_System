package worker

import (
	"math/rand/v2"
	"sync"
	"time"
)

// RetryOption customizes ExponentialRetryPolicy construction for deterministic tests.
type RetryOption func(*ExponentialRetryPolicy)

// WithRandSource overrides the pseudo-random generator used for full-jitter backoff and deferrals.
func WithRandSource(src rand.Source) RetryOption {
	return func(p *ExponentialRetryPolicy) {
		if src != nil {
			p.rng = rand.New(src)
		}
	}
}

// ExponentialRetryPolicy computes full-jitter exponential backoff and honors Retry-After bounds.
type ExponentialRetryPolicy struct {
	baseDelay     time.Duration
	capDelay      time.Duration
	minDelay      time.Duration
	retryAfterMax time.Duration

	mu  sync.Mutex
	rng *rand.Rand
}

// NewExponentialRetryPolicy creates a thread-safe retry delay calculator from retry timing bounds.
func NewExponentialRetryPolicy(baseDelay, capDelay, minDelay, retryAfterMax time.Duration, opts ...RetryOption) *ExponentialRetryPolicy {
	p := &ExponentialRetryPolicy{
		baseDelay:     baseDelay,
		capDelay:      capDelay,
		minDelay:      minDelay,
		retryAfterMax: retryAfterMax,
		rng:           rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// NextDelay returns max(minDelay, rand[0, min(cap, base * 2^(attempt-1)))) and honors retryAfter
// capped at retryAfterMax when provided.
func (p *ExponentialRetryPolicy) NextDelay(attempt int, retryAfter *time.Duration) time.Duration {
	expCap := p.exponentialCeiling(attempt)

	var delay time.Duration
	if expCap > 0 {
		delay = p.randomDuration(expCap)
	}
	if p.minDelay > 0 && delay < p.minDelay {
		delay = p.minDelay
	}
	if p.capDelay > 0 && delay > p.capDelay {
		delay = p.capDelay
	}

	if retryAfter != nil && *retryAfter > 0 {
		if *retryAfter > delay {
			delay = *retryAfter
		}
		if p.retryAfterMax > 0 && delay > p.retryAfterMax {
			delay = p.retryAfterMax
		}
	}
	return delay
}

// Jitter returns a uniform random duration in [0, maxJitter) using the policy's mutex-guarded RNG.
func (p *ExponentialRetryPolicy) Jitter(maxJitter time.Duration) time.Duration {
	if maxJitter <= 0 {
		return 0
	}
	return p.randomDuration(maxJitter)
}

func (p *ExponentialRetryPolicy) exponentialCeiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceiling := p.baseDelay
	if ceiling <= 0 {
		ceiling = p.minDelay
	}
	if ceiling <= 0 {
		return 0
	}
	for i := 1; i < attempt; i++ {
		if p.capDelay > 0 && ceiling >= p.capDelay {
			return p.capDelay
		}
		if p.capDelay > 0 && ceiling > p.capDelay/2 {
			return p.capDelay
		}
		// Prevent signed int64 overflow even if capDelay is disabled (<= 0).
		if ceiling > time.Duration(1<<62-1)/2 {
			return time.Duration(1<<62 - 1)
		}
		ceiling *= 2
	}
	if p.capDelay > 0 && ceiling > p.capDelay {
		return p.capDelay
	}
	return ceiling
}

func (p *ExponentialRetryPolicy) randomDuration(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.N(limit)
}
