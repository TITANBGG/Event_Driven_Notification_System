package ratelimit_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// go-redis' pool redialer (ConnPool.tryDial) sleeps 1s between attempts and only then notices a
	// closed pool, so it may outlive a test that closed its client. It is the library's own
	// goroutine, not ours; ignoring it replaces a 1.1s sleep in every Redis-backed test.
	goleak.VerifyTestMain(m, goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).tryDial"))
}
