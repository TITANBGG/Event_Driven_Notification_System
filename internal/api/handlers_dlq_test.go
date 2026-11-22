package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/domain"
)

func putFailed(store *fakeStore, ch domain.Channel, failedAt time.Time) uuid.UUID {
	id := uuid.Must(uuid.NewV7())
	lastErr := "provider rejected request with HTTP 401"
	store.SeedNotification(domain.Notification{
		ID: id, Channel: ch, Priority: domain.PriorityNormal, Status: domain.StatusFailed,
		Recipient: "+905551234567", Content: "x", Attempts: 1, MaxAttempts: 5, LastError: &lastErr,
		FailedAt: &failedAt, CreatedAt: failedAt, UpdatedAt: failedAt,
	})
	return id
}

func TestBulkRetryReplaysFailedNotificationsWithFilters(t *testing.T) {
	t.Parallel()
	h, store, enq := newTestRouter(t)
	base := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	smsOld := putFailed(store, domain.ChannelSMS, base)
	smsNew := putFailed(store, domain.ChannelSMS, base.Add(time.Hour))
	email := putFailed(store, domain.ChannelEmail, base)

	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/retry",
		`{"channel":"SMS","failedFrom":"2026-11-21T08:00:00Z","failedTo":"2026-11-21T09:30:00Z"}`, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp api.BulkRetryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.Retried)
	assert.Equal(t, []uuid.UUID{smsOld}, resp.NotificationIDs)

	got, err := store.GetNotification(t.Context(), smsOld)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, got.Status)
	assert.Zero(t, got.Attempts)
	assert.Nil(t, got.LastError)
	var queued []uuid.UUID
	for _, it := range enq.Items() {
		queued = append(queued, it.ID)
	}
	assert.Contains(t, queued, smsOld, "replayed rows are queued")

	for _, id := range []uuid.UUID{smsNew, email} {
		n, err := store.GetNotification(t.Context(), id)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusFailed, n.Status, "outside the filter")
	}

	// Empty body: default limit, every remaining failed row.
	rec = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/retry", `{}`, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 2, resp.Retried)

	// Nothing left to replay.
	rec = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/retry", `{"limit":5}`, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Zero(t, resp.Retried)
	assert.Empty(t, resp.NotificationIDs)
}

func TestBulkRetryValidatesFilters(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestRouter(t)

	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/retry",
		`{"channel":"fax","failedFrom":"yesterday","limit":5000}`, nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	for _, want := range []string{`"field":"channel"`, `"field":"failedFrom"`, `"field":"limit"`} {
		assert.Contains(t, rec.Body.String(), want)
	}

	rec = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/retry",
		`{"failedFrom":"2026-11-21T10:00:00Z","failedTo":"2026-11-21T09:00:00Z"}`, nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid_range")

	rec = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/retry", `{"unknown":1}`, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Item idempotency keys are claimed in one statement per batch, not one round trip per item.
func TestBatchItemKeysAreInsertedInOneCall(t *testing.T) {
	t.Parallel()
	h, store, _ := newTestRouter(t)
	body := `{"notifications":[` +
		`{"recipient":"+905551234561","channel":"sms","content":"a","idempotencyKey":"item-a"},` +
		`{"recipient":"+905551234562","channel":"sms","content":"b","idempotencyKey":"item-b"},` +
		`{"recipient":"+905551234563","channel":"sms","content":"c","idempotencyKey":"item-c"}]}`
	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", body, nil)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Equal(t, 1, store.BulkKeyInserts())

	// Reusing one of the keys rejects the whole new batch with the existing item reported.
	rec = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch",
		`{"notifications":[{"recipient":"+905551234564","channel":"sms","content":"d","idempotencyKey":"item-b"}]}`, nil)
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "item_idempotency_key_exists")
}
