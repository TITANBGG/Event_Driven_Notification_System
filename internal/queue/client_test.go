package queue_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/queue"
)

func TestNewClientWithPasswordAuthenticates(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cret-redis")

	anon := queue.NewClient(mr.Addr(), 2)
	t.Cleanup(func() { _ = anon.Close() })
	assert.Error(t, anon.Ping(t.Context()).Err(), "requirepass must refuse unauthenticated clients")

	authed := queue.NewClient(mr.Addr(), 2, queue.WithPassword("s3cret-redis"))
	t.Cleanup(func() { _ = authed.Close() })
	require.NoError(t, authed.Ping(t.Context()).Err())

	empty := queue.NewClient(mr.Addr(), 2, queue.WithPassword(""), nil)
	t.Cleanup(func() { _ = empty.Close() })
	assert.Error(t, empty.Ping(t.Context()).Err(), "an empty password sends no AUTH")
}
