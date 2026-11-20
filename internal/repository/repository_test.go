package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository/sqlc"
)

func TestMapError(t *testing.T) {
	t.Parallel()

	assert.NoError(t, MapError(nil))
	assert.ErrorIs(t, MapError(fmt.Errorf("wrapped: %w", pgx.ErrNoRows)), domain.ErrNotFound)

	dupErr := &pgconn.PgError{Code: "23505", ConstraintName: "idempotency_keys_pkey"}
	mapped := MapError(dupErr)
	assert.ErrorIs(t, mapped, domain.ErrDuplicateKey)

	otherErr := errors.New("connection refused")
	assert.Equal(t, otherErr, MapError(otherErr))

	assert.ErrorIs(t, mapFencedError(pgx.ErrNoRows), domain.ErrLeaseLost)
}

func TestNilStoreGuard(t *testing.T) {
	t.Parallel()

	s := New(nil)
	require.Error(t, s.Ping(t.Context()))
	require.Error(t, s.WithTx(t.Context(), func(_ context.Context, _ *Store) error { return nil }))
	_, err := s.GetNotification(t.Context(), uuid.New())
	require.Error(t, err)
}

func TestTimeAndDomainConversionsUTC(t *testing.T) {
	t.Parallel()

	loc := time.FixedZone("TRT", 3*3600)
	localTime := time.Date(2026, 11, 20, 20, 30, 0, 0, loc)
	utcPtr := normalizeNullUTC(&localTime)
	require.NotNil(t, utcPtr)
	assert.Equal(t, time.UTC, utcPtr.Location())
	assert.Equal(t, 17, utcPtr.Hour())

	assert.Nil(t, normalizeNullUTC(nil))

	id := uuid.New()
	httpCode := int32(202)
	attempt := toDomainAttempt(sqlc.DeliveryAttempt{
		ID:             9,
		NotificationID: id,
		AttemptNo:      1,
		Counted:        true,
		StartedAt:      localTime,
		DurationMs:     45,
		HttpStatus:     &httpCode,
		Outcome:        "sent",
	})
	assert.Equal(t, id, attempt.NotificationID)
	assert.Equal(t, domain.OutcomeSent, attempt.Outcome)
	assert.Equal(t, time.UTC, attempt.StartedAt.Location())
	assert.Equal(t, 17, attempt.StartedAt.Hour())
	require.NotNil(t, attempt.HTTPStatus)
	assert.Equal(t, 202, *attempt.HTTPStatus)
}
