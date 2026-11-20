package worker_test

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/TITANBGG/notification-system/internal/worker"
)

func TestExponentialRetryPolicyNextDelay(t *testing.T) {
	t.Parallel()

	base := 2 * time.Second
	capDelay := 16 * time.Second
	minDelay := 200 * time.Millisecond
	retryAfterMax := 15 * time.Second

	policy := worker.NewExponentialRetryPolicy(
		base,
		capDelay,
		minDelay,
		retryAfterMax,
		worker.WithRandSource(rand.NewPCG(42, 99)),
	)

	// Verify 100 samples per attempt obey floor and exponential cap bounds.
	expectedCeilings := map[int]time.Duration{
		0:  2 * time.Second,
		1:  2 * time.Second,
		2:  4 * time.Second,
		3:  8 * time.Second,
		4:  16 * time.Second,
		5:  16 * time.Second,
		50: 16 * time.Second,
	}
	for attempt, ceiling := range expectedCeilings {
		for i := 0; i < 100; i++ {
			d := policy.NextDelay(attempt, nil)
			assert.GreaterOrEqual(t, d, minDelay, "attempt=%d", attempt)
			assert.LessOrEqual(t, d, ceiling, "attempt=%d", attempt)
		}
	}

	// Verify deterministic repeatability with identical PCG seed.
	pA := worker.NewExponentialRetryPolicy(base, capDelay, minDelay, retryAfterMax, worker.WithRandSource(rand.NewPCG(7, 11)))
	pB := worker.NewExponentialRetryPolicy(base, capDelay, minDelay, retryAfterMax, worker.WithRandSource(rand.NewPCG(7, 11)))
	for attempt := 1; attempt <= 6; attempt++ {
		assert.Equal(t, pA.NextDelay(attempt, nil), pB.NextDelay(attempt, nil))
	}

	// Verify Retry-After raises short delays and gets clamped by retryAfterMax.
	smallFloor := worker.NewExponentialRetryPolicy(100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, retryAfterMax, worker.WithRandSource(rand.NewPCG(1, 2)))
	raShort := 7 * time.Second
	assert.Equal(t, 7*time.Second, smallFloor.NextDelay(1, &raShort))

	raHuge := 45 * time.Second
	assert.Equal(t, retryAfterMax, smallFloor.NextDelay(1, &raHuge))

	// When exponential delay already exceeds Retry-After (and <= retryAfterMax), keep exponential delay.
	highFloor := worker.NewExponentialRetryPolicy(20*time.Second, 20*time.Second, 12*time.Second, 30*time.Second, worker.WithRandSource(rand.NewPCG(3, 4)))
	got := highFloor.NextDelay(1, &raShort)
	assert.GreaterOrEqual(t, got, 12*time.Second)
	assert.LessOrEqual(t, got, 20*time.Second)

	// Verify Jitter stays within [0, max).
	for i := 0; i < 50; i++ {
		j := policy.Jitter(5 * time.Second)
		assert.GreaterOrEqual(t, j, time.Duration(0))
		assert.Less(t, j, 5*time.Second)
	}
	assert.Equal(t, time.Duration(0), policy.Jitter(0))
}

func TestExponentialRetryPolicyConcurrentSafety(t *testing.T) {
	t.Parallel()

	policy := worker.NewExponentialRetryPolicy(2*time.Second, time.Minute, 200*time.Millisecond, 2*time.Minute)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ra := time.Duration(workerNum+1) * time.Second
				d := policy.NextDelay((i%6)+1, &ra)
				assert.GreaterOrEqual(t, d, 200*time.Millisecond)
				_ = policy.Jitter(5 * time.Second)
			}
		}(g)
	}
	wg.Wait()
}
