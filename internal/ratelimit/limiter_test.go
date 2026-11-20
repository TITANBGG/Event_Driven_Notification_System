package ratelimit_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/ratelimit"
)

func TestLimiterGCRA100PerSecondOverSimulated1Second(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()

	base := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	lim := ratelimit.New(rdb, 100, 1)

	allowed := 0
	for i := 0; i < 250; i++ {
		simNow := base.Add(time.Duration(i) * 4 * time.Millisecond)
		mr.SetTime(simNow)
		mr.FastForward(4 * time.Millisecond)

		ok, _, err := lim.Allow(t.Context(), domain.ChannelSMS)
		require.NoError(t, err)
		if ok {
			allowed++
		}
	}
	assert.LessOrEqual(t, allowed, 101)
	assert.GreaterOrEqual(t, allowed, 80, "expected discrete 4ms GCRA ticks over 1s to permit ~84 requests")
}

func TestLimiterWaitRespectsContextAndFailsClosedOnRedisError(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb := queue.NewClient(mr.Addr(), 4)
	defer func() { _ = rdb.Close() }()

	var hookCalls atomic.Int32
	lim := ratelimit.New(rdb, 10, 1, ratelimit.WithOnRateLimited(func(_ domain.Channel, _ time.Duration) {
		hookCalls.Add(1)
	}))

	// First token succeeds immediately.
	require.NoError(t, lim.Wait(t.Context(), domain.ChannelEmail))

	// Immediate second Wait with a short timeout is throttled, fires hook, and aborts on context timeout.
	waitCtx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	defer cancel()
	err := lim.Wait(waitCtx, domain.ChannelEmail)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, hookCalls.Load(), int32(1))

	// Redis down -> fails closed by returning an error instead of allowing sends.
	mr.Close()
	ok, _, allowErr := lim.Allow(t.Context(), domain.ChannelEmail)
	require.Error(t, allowErr)
	assert.False(t, ok)
}
