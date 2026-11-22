package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/TITANBGG/notification-system/internal/queue"
)

// KeyedLimiter is a GCRA limiter per arbitrary key (an API key name), shared by every API replica.
// It reuses the provider limiter's algorithm with a burst equal to the per-second rate, so a client
// may send a short burst but not exceed the rate over any second on average.
type KeyedLimiter struct {
	rl    *redis_rate.Limiter
	limit redis_rate.Limit
}

// NewKeyed returns a limiter allowing ratePerSec requests per second per key, or nil when
// ratePerSec <= 0 (disabled).
func NewKeyed(rdb redis.UniversalClient, ratePerSec int) *KeyedLimiter {
	if rdb == nil || ratePerSec <= 0 {
		return nil
	}
	return &KeyedLimiter{
		rl:    redis_rate.NewLimiter(rdb),
		limit: redis_rate.Limit{Rate: ratePerSec, Period: time.Second, Burst: ratePerSec},
	}
}

// AllowKey consumes one request for key. Errors are returned to the caller, which decides whether
// to fail open (the API) or closed.
func (l *KeyedLimiter) AllowKey(ctx context.Context, key string) (bool, time.Duration, error) {
	if l == nil || l.rl == nil {
		return false, 0, errors.New("keyed rate limiter is not initialized")
	}
	res, err := l.rl.Allow(ctx, queue.APIRateLimitKey(key), l.limit)
	if err != nil {
		return false, 0, fmt.Errorf("checking API rate limit: %w", err)
	}
	return res.Allowed > 0, res.RetryAfter, nil
}
