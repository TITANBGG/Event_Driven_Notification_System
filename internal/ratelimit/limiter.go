package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
)

// OnRateLimitedFunc receives rate-limit backoff events so callers can record metrics without tight coupling.
type OnRateLimitedFunc func(channel domain.Channel, wait time.Duration)

// Option configures optional Limiter behavior.
type Option func(*Limiter)

// WithOnRateLimited installs a hook invoked whenever Wait is delayed by GCRA throttling.
func WithOnRateLimited(fn OnRateLimitedFunc) Option {
	return func(l *Limiter) {
		l.onRateLimited = fn
	}
}

// Limiter enforces per-channel GCRA rate limits across worker replicas backed by Redis.
type Limiter struct {
	rl            *redis_rate.Limiter
	limit         redis_rate.Limit
	onRateLimited OnRateLimitedFunc
}

// New builds a distributed GCRA rate limiter permitting ratePerSec requests/second per channel
// with configured burst size (Burst=1 spaces requests ~10ms apart at 100/s).
func New(rdb redis.UniversalClient, ratePerSec, burst int, opts ...Option) *Limiter {
	l := &Limiter{
		limit: redis_rate.Limit{
			Rate:   ratePerSec,
			Period: time.Second,
			Burst:  burst,
		},
	}
	if rdb != nil {
		l.rl = redis_rate.NewLimiter(rdb)
	}
	for _, opt := range opts {
		if opt != nil {
			opt(l)
		}
	}
	return l
}

// Allow consumes one token for ch if available, returning retryAfter when throttled.
// Fails closed by returning any Redis error rather than bypassing the provider rate limit.
func (l *Limiter) Allow(ctx context.Context, ch domain.Channel) (bool, time.Duration, error) {
	if l == nil || l.rl == nil {
		return false, 0, errors.New("rate limiter is not initialized")
	}
	if !ch.Valid() {
		return false, 0, fmt.Errorf("invalid channel %q", ch)
	}

	res, err := l.rl.Allow(ctx, queue.RateLimitKey(ch), l.limit)
	if err != nil {
		return false, 0, fmt.Errorf("checking rate limit for %s: %w", ch, err)
	}
	return res.Allowed > 0, res.RetryAfter, nil
}

// Wait blocks using ctx-aware timers until ch acquires a GCRA token, failing closed on Redis or context errors.
func (l *Limiter) Wait(ctx context.Context, ch domain.Channel) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		allowed, retryAfter, err := l.Allow(ctx, ch)
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}

		wait := retryAfter
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		if l.onRateLimited != nil {
			l.onRateLimited(ch, wait)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
