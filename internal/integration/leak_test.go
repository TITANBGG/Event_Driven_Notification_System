//go:build integration

package integration_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// testcontainers-go on Windows starts one permanent go-winio IOCP dispatcher via sync.Once
	// for the Docker named pipe transport (stack: syscall -> getQueuedCompletionStatus -> ioCompletionProcessor).
	goleak.VerifyTestMain(m, goleak.IgnoreAnyFunction("github.com/Microsoft/go-winio.ioCompletionProcessor"))
}
