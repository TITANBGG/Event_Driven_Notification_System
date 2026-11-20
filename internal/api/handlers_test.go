package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
)

func newTestRouter(t *testing.T, opts ...func(*config.Config)) (http.Handler, *fakeStore, *api.MemoryEnqueuer) {
	t.Helper()
	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	for _, fn := range opts {
		fn(&cfg)
	}

	store := newFakeStore()
	enq := api.NewMemoryEnqueuer()
	logger := observability.NewLogger("info", &bytes.Buffer{})
	r := api.NewRouter(api.Deps{
		Config:   cfg,
		Logger:   logger,
		Store:    store,
		Enqueuer: enq,
		Now:      func() time.Time { return time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC) },
	})
	return r, store, enq
}

func doJSONRequest(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
		if _, exists := headers["Content-Type"]; !exists {
			req.Header.Set("Content-Type", "application/json")
		}
	} else {
		req = httptest.NewRequestWithContext(t.Context(), method, path, nil)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateNotificationSuccessAndDTOExcludesInternalFields(t *testing.T) {
	t.Parallel()

	h, store, enq := newTestRouter(t)
	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"SMS","content":"Siparişiniz kargoya verildi","priority":"HIGH"}`,
		map[string]string{"X-Correlation-ID": "corr-create-1"},
	)

	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "corr-create-1", rec.Header().Get("X-Correlation-ID"))
	assert.Empty(t, rec.Header().Get("Idempotent-Replayed"))

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.Equal(t, "sms", raw["channel"])
	assert.Equal(t, "high", raw["priority"])
	assert.Equal(t, "pending", raw["status"])
	assert.Equal(t, float64(0), raw["attempts"])
	assert.Equal(t, float64(5), raw["maxAttempts"])
	// Verify internal persistence fields never leak into HTTP DTO responses.
	for _, forbidden := range []string{"processingStartedAt", "traceparent", "idempotencyKey", "deferrals"} {
		_, leaked := raw[forbidden]
		assert.False(t, leaked, "internal field %s must not be exposed in HTTP response", forbidden)
	}

	assert.Equal(t, 1, store.NotificationCount())
	require.Len(t, enq.Items(), 1)
	assert.Equal(t, "corr-create-1", enq.Items()[0].CorrelationID)
}

func TestCreateNotificationValidationMultipleFieldsAndTurkishSMS(t *testing.T) {
	t.Parallel()

	h, store, enq := newTestRouter(t)
	// Invalid E.164 + Turkish 'ş' (UCS-2) exceeding 10 segments (= 671 chars -> 11 segments).
	tooLongTurkish := strings.Repeat("ş", 671)
	payload := fmt.Sprintf(`{"recipient":"05551234567","channel":"sms","content":%q,"subject":"not-allowed"}`, tooLongTurkish)

	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", payload, map[string]string{
		"X-Correlation-ID": "corr-val-1",
	})

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Equal(t, "corr-val-1", rec.Header().Get("X-Correlation-ID"))

	var errResp api.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "validation_failed", errResp.Error.Code)
	assert.Equal(t, "corr-val-1", errResp.CorrelationID)

	byField := make(map[string]domain.FieldError)
	for _, d := range errResp.Error.Details {
		byField[d.Field] = d
	}
	assert.Equal(t, "invalid_e164", byField["recipient"].Code)
	assert.Equal(t, "too_many_segments", byField["content"].Code)
	assert.Contains(t, byField["content"].Message, "UCS-2")
	assert.Equal(t, "unsupported_field", byField["subject"].Code)

	assert.Equal(t, 0, store.NotificationCount())
	assert.Empty(t, enq.Items())
}

func TestCreateNotificationPhase3FeatureGatesAndUnknownFields(t *testing.T) {
	t.Parallel()

	h, _, _ := newTestRouter(t)

	// Unknown JSON field rejected with 400 (DisallowUnknownFields).
	recUnknown := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"hi","typoField":true}`,
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, recUnknown.Code)
	assert.Contains(t, recUnknown.Body.String(), "malformed_json")

	// Missing template returns 422 template_not_found.
	recMissingTmpl := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","templateId":"01936f7e-0000-7000-8000-000000000001"}`,
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recMissingTmpl.Code)
	var errResp api.ErrorResponse
	require.NoError(t, json.Unmarshal(recMissingTmpl.Body.Bytes(), &errResp))
	assert.Equal(t, "template_not_found", errResp.Error.Code)
}

func TestTemplatesCRUDPreviewSoftDeleteAndRenderAtCreate(t *testing.T) {
	t.Parallel()

	h, store, enq := newTestRouter(t)

	// 1. Create SMS template with Turkish body and placeholders {{name}} and {{code}}.
	recCreate := doJSONRequest(t, h, http.MethodPost, "/api/v1/templates",
		`{"name":"otp_tr","channel":"sms","body":"Merhaba {{name}}, doğrulama kodunuz: {{code}}","variables":["name","code"]}`,
		nil,
	)
	require.Equal(t, http.StatusCreated, recCreate.Code)
	var tmpl api.TemplateResponse
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &tmpl))
	assert.Equal(t, "otp_tr", tmpl.Name)
	assert.Equal(t, "sms", tmpl.Channel)
	assert.Equal(t, []string{"name", "code"}, tmpl.Variables)

	// Duplicate active template name returns 409 template_name_exists.
	recDupName := doJSONRequest(t, h, http.MethodPost, "/api/v1/templates",
		`{"name":"otp_tr","channel":"sms","body":"Hi {{name}}","variables":["name"]}`,
		nil,
	)
	require.Equal(t, http.StatusConflict, recDupName.Code)
	assert.Contains(t, recDupName.Body.String(), "template_name_exists")

	// 2. Preview template with extra unused variable -> returns rendered content, UCS-2 SMS info, and unusedVariables.
	recPreview := doJSONRequest(t, h, http.MethodPost, "/api/v1/templates/"+tmpl.ID.String()+"/preview",
		`{"variables":{"name":"Ayşe","code":4821,"unusedKey":"extra"}}`,
		nil,
	)
	require.Equal(t, http.StatusOK, recPreview.Code)
	var preview api.TemplatePreviewResponse
	require.NoError(t, json.Unmarshal(recPreview.Body.Bytes(), &preview))
	assert.Equal(t, "Merhaba Ayşe, doğrulama kodunuz: 4821", preview.Content)
	require.NotNil(t, preview.SMS)
	assert.Equal(t, "UCS-2", preview.SMS.Encoding)
	assert.Equal(t, 1, preview.SMS.Segments)
	assert.Equal(t, []string{"unusedKey"}, preview.UnusedVariables)

	// 3. Reject notification specifying both content and templateId, or neither, or mismatched channel.
	recBoth := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":"direct","templateId":%q,"templateVariables":{"name":"Ayşe","code":"1234"}}`, tmpl.ID),
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recBoth.Code)
	assert.Contains(t, recBoth.Body.String(), "content_and_template_conflict")

	recNeither := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms"}`,
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recNeither.Code)

	recWrongCh := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		fmt.Sprintf(`{"recipient":"a@example.com","channel":"email","templateId":%q,"templateVariables":{"name":"Ayşe","code":"1234"}}`, tmpl.ID),
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recWrongCh.Code)
	assert.Contains(t, recWrongCh.Body.String(), "template_channel_mismatch")

	// 4. Rendered SMS exceeding 10 segments (> 670 UCS-2 code units) fails with 422 too_many_segments.
	recTooLong := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","templateId":%q,"templateVariables":{"name":%q,"code":"1234"}}`, tmpl.ID, strings.Repeat("ş", 660)),
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recTooLong.Code)
	assert.Contains(t, recTooLong.Body.String(), "too_many_segments")

	// 5. Batch create with personalized SMS templates (render-at-create).
	batchBody := fmt.Sprintf(`{"notifications":[
		{"recipient":"+905551111111","channel":"sms","templateId":%q,"templateVariables":{"name":"Ayşe","code":"4821"}},
		{"recipient":"+905552222222","channel":"sms","templateId":%q,"templateVariables":{"name":"Mehmet","code":"9035"}}
	]}`, tmpl.ID, tmpl.ID)
	recBatch := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", batchBody, nil)
	require.Equal(t, http.StatusAccepted, recBatch.Code)
	var batchResp api.BatchCreateResponse
	require.NoError(t, json.Unmarshal(recBatch.Body.Bytes(), &batchResp))
	require.Len(t, batchResp.NotificationIDs, 2)
	assert.Len(t, enq.Items(), 2)

	n0, err := store.GetNotification(t.Context(), batchResp.NotificationIDs[0])
	require.NoError(t, err)
	assert.Equal(t, "Merhaba Ayşe, doğrulama kodunuz: 4821", n0.Content)
	require.NotNil(t, n0.TemplateID)
	assert.Equal(t, tmpl.ID, *n0.TemplateID)

	// 6. PUT /api/v1/templates/{id} updates template definition, and does NOT alter already-created notifications.
	recPut := doJSONRequest(t, h, http.MethodPut, "/api/v1/templates/"+tmpl.ID.String(),
		`{"name":"otp_tr","channel":"sms","body":"YENI METIN: {{name}} - {{code}}","variables":["name","code"]}`,
		nil,
	)
	require.Equal(t, http.StatusOK, recPut.Code)
	n0AfterPut, err := store.GetNotification(t.Context(), batchResp.NotificationIDs[0])
	require.NoError(t, err)
	assert.Equal(t, "Merhaba Ayşe, doğrulama kodunuz: 4821", n0AfterPut.Content, "render-at-create must keep queued notification immutable after template PUT")

	// 7. GET /api/v1/templates?channel=sms lists active template.
	recList := doJSONRequest(t, h, http.MethodGet, "/api/v1/templates?channel=sms", "", nil)
	require.Equal(t, http.StatusOK, recList.Code)
	var listResp api.TemplateListResponse
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	require.Len(t, listResp.Data, 1)
	assert.Equal(t, tmpl.ID, listResp.Data[0].ID)

	// 8. DELETE /api/v1/templates/{id} soft-deletes (204) -> subsequent GET returns 404 -> create notification returns 422 template_not_found, while existing notification keeps templateId and content.
	recDel := doJSONRequest(t, h, http.MethodDelete, "/api/v1/templates/"+tmpl.ID.String(), "", nil)
	require.Equal(t, http.StatusNoContent, recDel.Code)

	recGetDeleted := doJSONRequest(t, h, http.MethodGet, "/api/v1/templates/"+tmpl.ID.String(), "", nil)
	require.Equal(t, http.StatusNotFound, recGetDeleted.Code)

	recUseDeleted := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","templateId":%q,"templateVariables":{"name":"Ali","code":"0000"}}`, tmpl.ID),
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recUseDeleted.Code)
	assert.Contains(t, recUseDeleted.Body.String(), "template_not_found")

	recGetExistingNotif := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications/"+batchResp.NotificationIDs[0].String(), "", nil)
	require.Equal(t, http.StatusOK, recGetExistingNotif.Code)
	var existingDetail api.NotificationDetailResponse
	require.NoError(t, json.Unmarshal(recGetExistingNotif.Body.Bytes(), &existingDetail))
	assert.Equal(t, "Merhaba Ayşe, doğrulama kodunuz: 4821", existingDetail.Content)
	require.NotNil(t, existingDetail.TemplateID)
	assert.Equal(t, tmpl.ID, *existingDetail.TemplateID)
}

func TestScheduledNotificationsValidationImmediateThresholdAndCancel(t *testing.T) {
	t.Parallel()

	// Fixed test clock in newTestRouter: 2026-11-21T10:00:00Z, ClockSkewTolerance=2s, ScheduleMaxAhead=720h (30d).
	h, store, enq := newTestRouter(t)

	// 1. Past beyond clock skew (-3s) -> 422 scheduled_in_past.
	recPast := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"past","scheduledAt":"2026-11-21T09:59:57Z"}`,
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recPast.Code)
	assert.Contains(t, recPast.Body.String(), "scheduled_in_past")

	// 2. Beyond 30 days (720h + 1m) -> 422 scheduled_too_far.
	recTooFar := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"far","scheduledAt":"2026-12-21T10:01:00Z"}`,
		nil,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recTooFar.Code)
	assert.Contains(t, recTooFar.Body.String(), "scheduled_too_far")

	// 3. Within clock skew (-1s) or <= now + 1s (+1s) -> treated as immediate (status="pending", scheduledAt=null, Enqueue immediate).
	for _, ts := range []string{"2026-11-21T09:59:59Z", "2026-11-21T10:00:01Z"} {
		recImm := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
			fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":"immediate","scheduledAt":%q}`, ts),
			nil,
		)
		require.Equal(t, http.StatusAccepted, recImm.Code)
		var resp api.NotificationResponse
		require.NoError(t, json.Unmarshal(recImm.Body.Bytes(), &resp))
		assert.Equal(t, "pending", resp.Status)
		assert.Nil(t, resp.ScheduledAt)
	}
	require.Len(t, enq.Items(), 2)
	assert.Nil(t, enq.Items()[0].ScheduledAt)
	assert.Nil(t, enq.Items()[1].ScheduledAt)

	// 4. Future scheduledAt with non-UTC offset (+03:00 -> 2026-11-21T10:00:30Z) -> status="scheduled", stored in UTC, routed with ScheduledAt.
	recSched := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"future 30s","scheduledAt":"2026-11-21T13:00:30+03:00"}`,
		map[string]string{"Idempotency-Key": "sched-tz-1"},
	)
	require.Equal(t, http.StatusAccepted, recSched.Code)
	var schedResp api.NotificationResponse
	require.NoError(t, json.Unmarshal(recSched.Body.Bytes(), &schedResp))
	assert.Equal(t, "scheduled", schedResp.Status)
	require.NotNil(t, schedResp.ScheduledAt)
	assert.Equal(t, time.Date(2026, 11, 21, 10, 0, 30, 0, time.UTC), *schedResp.ScheduledAt)

	// Idempotent replay with equivalent UTC timestamp ("2026-11-21T10:00:30Z") matches canonical hash.
	recReplay := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"future 30s","scheduledAt":"2026-11-21T10:00:30Z"}`,
		map[string]string{"Idempotency-Key": "sched-tz-1"},
	)
	require.Equal(t, http.StatusAccepted, recReplay.Code)
	assert.Equal(t, "true", recReplay.Header().Get("Idempotent-Replayed"))

	items := enq.Items()
	require.Len(t, items, 3)
	require.NotNil(t, items[2].ScheduledAt)
	assert.Equal(t, time.Date(2026, 11, 21, 10, 0, 30, 0, time.UTC), *items[2].ScheduledAt)

	// 5. Batch create with mixed immediate and scheduled items.
	recBatch := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch",
		`{"notifications":[
			{"recipient":"+905551234501","channel":"sms","content":"imm","scheduledAt":"2026-11-21T10:00:00.500Z"},
			{"recipient":"+905551234502","channel":"sms","content":"sched","scheduledAt":"2026-11-21T10:05:00Z"}
		]}`,
		nil,
	)
	require.Equal(t, http.StatusAccepted, recBatch.Code)

	// 6. List filter status=scheduled returns the 2 scheduled notifications.
	recList := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications?status=scheduled", "", nil)
	require.Equal(t, http.StatusOK, recList.Code)
	var listResp api.NotificationListResponse
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	require.Len(t, listResp.Data, 2)
	for _, n := range listResp.Data {
		assert.Equal(t, "scheduled", n.Status)
		assert.NotNil(t, n.ScheduledAt)
	}

	// 7. Cancel scheduled notification transitions scheduled -> cancelled and invokes best-effort CancelDelayed.
	recCancel := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/"+schedResp.ID.String()+"/cancel", "", nil)
	require.Equal(t, http.StatusOK, recCancel.Code)
	var cancelResp api.NotificationResponse
	require.NoError(t, json.Unmarshal(recCancel.Body.Bytes(), &cancelResp))
	assert.Equal(t, "cancelled", cancelResp.Status)
	assert.Contains(t, enq.CancelledIDs(), schedResp.ID)
	dbRow, err := store.GetNotification(t.Context(), schedResp.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, dbRow.Status)
}

func TestCreateNotificationDBFirstOnEnqueueFailure(t *testing.T) {
	t.Parallel()

	h, store, enq := newTestRouter(t)
	enq.SetError(errors.New("redis unavailable"))

	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"durable even without redis"}`,
		nil,
	)
	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, 1, store.NotificationCount())
}

func TestCreateNotificationIdempotentReplayConflictAndConcurrentDuplicate(t *testing.T) {
	t.Parallel()

	h, store, enq := newTestRouter(t)

	// Replay with equivalent canonical fields ("SMS" vs "sms", omitted priority vs "normal").
	rec1 := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"SMS","content":"order ready"}`,
		map[string]string{"Idempotency-Key": "order-42"},
	)
	require.Equal(t, http.StatusAccepted, rec1.Code)
	assert.Empty(t, rec1.Header().Get("Idempotent-Replayed"))

	rec2 := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"priority":"normal","content":"order ready","channel":"sms","recipient":"+905551234567"}`,
		map[string]string{"Idempotency-Key": "order-42"},
	)
	require.Equal(t, http.StatusAccepted, rec2.Code)
	assert.Equal(t, "true", rec2.Header().Get("Idempotent-Replayed"))

	var n1, n2 api.NotificationResponse
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &n1))
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &n2))
	assert.Equal(t, n1.ID, n2.ID)
	assert.Equal(t, 1, store.NotificationCount())
	assert.Len(t, enq.Items(), 1)

	// Same key + different payload -> 409 idempotency_conflict.
	recConflict := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"DIFFERENT BODY"}`,
		map[string]string{"Idempotency-Key": "order-42"},
	)
	require.Equal(t, http.StatusConflict, recConflict.Code)
	assert.Contains(t, recConflict.Body.String(), "idempotency_conflict")

	// Concurrent duplicate requests with two goroutines produce 1 DB row and 1 replay.
	h2, store2, enq2 := newTestRouter(t)
	ctx2 := t.Context()
	start := make(chan struct{})
	var wg sync.WaitGroup
	recs := make([]*httptest.ResponseRecorder, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			req := httptest.NewRequestWithContext(ctx2, http.MethodPost, "/api/v1/notifications",
				strings.NewReader(`{"recipient":"+905551234567","channel":"sms","content":"concurrent"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "race-key-1")
			r := httptest.NewRecorder()
			h2.ServeHTTP(r, req)
			recs[idx] = r
		}(i)
	}
	close(start)
	wg.Wait()

	require.Equal(t, http.StatusAccepted, recs[0].Code)
	require.Equal(t, http.StatusAccepted, recs[1].Code)

	var cn0, cn1 api.NotificationResponse
	require.NoError(t, json.Unmarshal(recs[0].Body.Bytes(), &cn0))
	require.NoError(t, json.Unmarshal(recs[1].Body.Bytes(), &cn1))
	assert.Equal(t, cn0.ID, cn1.ID)
	assert.Equal(t, 1, store2.NotificationCount())
	assert.Len(t, enq2.Items(), 1)

	replayedCount := 0
	for _, r := range recs {
		if r.Header().Get("Idempotent-Replayed") == "true" {
			replayedCount++
		}
	}
	assert.Equal(t, 1, replayedCount)
}

func TestBatchCreateValidationLimitsDuplicatesConflictsAndReplay(t *testing.T) {
	t.Parallel()

	t.Run("batch over 1000 items returns 422 batch_too_large", func(t *testing.T) {
		t.Parallel()
		h, _, _ := newTestRouter(t)

		items := make([]string, 1001)
		for i := range items {
			items[i] = `{"recipient":"+905551234567","channel":"sms","content":"hi"}`
		}
		body := `{"notifications":[` + strings.Join(items, ",") + `]}`
		rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", body, nil)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "batch_too_large")
	})

	t.Run("batch body over limit returns 413 payload_too_large", func(t *testing.T) {
		t.Parallel()
		// Exercise the middleware limit directly using config overrides; 16 MB default is enforced identically.
		h, _, _ := newTestRouter(t, func(cfg *config.Config) {
			cfg.BatchMaxBodyBytes = 1024
		})
		large := `{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"` + strings.Repeat("x", 1500) + `"}]}`
		rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", large, nil)
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		assert.Contains(t, rec.Body.String(), "payload_too_large")
		assert.Contains(t, rec.Body.String(), "split batch")
	})

	t.Run("16 MB boundary enforces batch max body bytes", func(t *testing.T) {
		t.Parallel()
		h, _, _ := newTestRouter(t)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/notifications/batch", http.NoBody)
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = 16*1024*1024 + 1
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		assert.Contains(t, rec.Body.String(), "payload_too_large")
	})

	t.Run("duplicate item keys inside batch returns 422 duplicate_idempotency_key", func(t *testing.T) {
		t.Parallel()
		h, store, _ := newTestRouter(t)
		body := `{"notifications":[
			{"recipient":"+905551234567","channel":"sms","content":"a","idempotencyKey":"dup-k"},
			{"recipient":"+905551234568","channel":"sms","content":"b","idempotencyKey":"unique-k"},
			{"recipient":"+905551234569","channel":"sms","content":"c","idempotencyKey":"dup-k"}
		]}`
		rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", body, nil)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "duplicate_idempotency_key")
		assert.Contains(t, rec.Body.String(), "notifications[0].idempotencyKey")
		assert.Contains(t, rec.Body.String(), "notifications[2].idempotencyKey")
		assert.Equal(t, 0, store.NotificationCount())
	})

	t.Run("existing item key rejects whole batch with 409 item_idempotency_key_exists", func(t *testing.T) {
		t.Parallel()
		h, store, _ := newTestRouter(t)
		existingID, err := uuid.NewV7()
		require.NoError(t, err)
		store.SeedIdempotencyKey(domain.IdempotencyKey{
			Key:          "already-used-item",
			RequestHash:  "abc",
			ResourceType: domain.ResourceNotification,
			ResourceID:   existingID,
			ExpiresAt:    time.Now().UTC().Add(time.Hour),
		})

		body := `{"notifications":[
			{"recipient":"+905551234567","channel":"sms","content":"a","idempotencyKey":"fresh-k"},
			{"recipient":"+905551234568","channel":"sms","content":"b","idempotencyKey":"already-used-item"}
		]}`
		rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", body, nil)
		require.Equal(t, http.StatusConflict, rec.Code)

		var errResp api.ErrorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "item_idempotency_key_exists", errResp.Error.Code)
		require.Len(t, errResp.Error.Details, 1)
		assert.Equal(t, "notifications[1].idempotencyKey", errResp.Error.Details[0].Field)
		assert.Contains(t, errResp.Error.Details[0].Message, existingID.String())
		// All-or-nothing guarantee: fresh-k notification was NOT persisted.
		assert.Equal(t, 0, store.NotificationCount())
	})

	t.Run("batch create, get summary, cancel, and batch idempotent replay", func(t *testing.T) {
		t.Parallel()
		h, store, enq := newTestRouter(t)
		body := `{"notifications":[
			{"recipient":"+905551234567","channel":"sms","content":"one","idempotencyKey":"item-a"},
			{"recipient":"user@example.com","channel":"email","subject":"Hello","content":"two"}
		]}`
		rec1 := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", body, map[string]string{
			"Idempotency-Key": "batch-key-1",
		})
		require.Equal(t, http.StatusAccepted, rec1.Code)
		assert.Empty(t, rec1.Header().Get("Idempotent-Replayed"))

		var b1 api.BatchCreateResponse
		require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &b1))
		assert.Equal(t, 2, b1.Count)
		require.Len(t, b1.NotificationIDs, 2)
		assert.Len(t, enq.Items(), 2)

		// Replaying batch key must return original batchId + notificationIds even when items have keys.
		recReplay := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/batch", body, map[string]string{
			"Idempotency-Key": "batch-key-1",
		})
		require.Equal(t, http.StatusAccepted, recReplay.Code)
		assert.Equal(t, "true", recReplay.Header().Get("Idempotent-Replayed"))
		var bReplay api.BatchCreateResponse
		require.NoError(t, json.Unmarshal(recReplay.Body.Bytes(), &bReplay))
		assert.Equal(t, b1.BatchID, bReplay.BatchID)
		assert.Equal(t, b1.NotificationIDs, bReplay.NotificationIDs)
		assert.Len(t, enq.Items(), 2)

		// Mark item[0] as sent so batch cancel yields cancelled=1, notCancellable=1.
		n0, err := store.GetNotification(t.Context(), b1.NotificationIDs[0])
		require.NoError(t, err)
		n0.Status = domain.StatusSent
		store.SeedNotification(*n0)

		getBatchRec := doJSONRequest(t, h, http.MethodGet, "/api/v1/batches/"+b1.BatchID.String(), "", nil)
		require.Equal(t, http.StatusOK, getBatchRec.Code)
		var detail api.BatchDetailResponse
		require.NoError(t, json.Unmarshal(getBatchRec.Body.Bytes(), &detail))
		assert.Equal(t, int64(1), detail.CountsByStatus["sent"])
		assert.Equal(t, int64(1), detail.CountsByStatus["pending"])
		assert.Len(t, detail.Items, 2)

		cancelBatchRec := doJSONRequest(t, h, http.MethodPost, "/api/v1/batches/"+b1.BatchID.String()+"/cancel", "", nil)
		require.Equal(t, http.StatusOK, cancelBatchRec.Code)
		var cancelResp api.BatchCancelResponse
		require.NoError(t, json.Unmarshal(cancelBatchRec.Body.Bytes(), &cancelResp))
		assert.Equal(t, 1, cancelResp.Cancelled)
		assert.Equal(t, 1, cancelResp.NotCancellable)
	})
}

func TestCancelGetAndRetryEndpoints(t *testing.T) {
	t.Parallel()

	h, store, enq := newTestRouter(t)

	// Invalid UUID -> 400.
	recBadUUID := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications/not-a-uuid", "", nil)
	assert.Equal(t, http.StatusBadRequest, recBadUUID.Code)
	assert.Contains(t, recBadUUID.Body.String(), "invalid_uuid")

	// Cancel missing ID -> 404.
	missingID, err := uuid.NewV7()
	require.NoError(t, err)
	recMissing := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/"+missingID.String()+"/cancel", "", nil)
	assert.Equal(t, http.StatusNotFound, recMissing.Code)

	// Seed sent notification -> cancel returns 409 not_cancellable with current status.
	sentID, err := uuid.NewV7()
	require.NoError(t, err)
	now := time.Now().UTC()
	store.SeedNotification(domain.Notification{
		ID:          sentID,
		Channel:     domain.ChannelSMS,
		Priority:    domain.PriorityNormal,
		Status:      domain.StatusSent,
		Recipient:   "+905551234567",
		Content:     "delivered",
		Attempts:    1,
		MaxAttempts: 5,
		CreatedAt:   now,
		UpdatedAt:   now,
		SentAt:      &now,
	})
	http202 := http.StatusAccepted
	store.SeedAttempt(domain.DeliveryAttempt{
		ID:             1,
		NotificationID: sentID,
		AttemptNo:      1,
		Counted:        true,
		StartedAt:      now,
		DurationMS:     45,
		HTTPStatus:     &http202,
		Outcome:        domain.OutcomeSent,
	})

	getRec := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications/"+sentID.String(), "", nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	var detail api.NotificationDetailResponse
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &detail))
	assert.Equal(t, "sent", detail.Status)
	require.Len(t, detail.DeliveryAttempts, 1)
	assert.Equal(t, 45, detail.DeliveryAttempts[0].DurationMS)

	cancelSentRec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/"+sentID.String()+"/cancel", "", nil)
	require.Equal(t, http.StatusConflict, cancelSentRec.Code)
	var cancelErr api.ErrorResponse
	require.NoError(t, json.Unmarshal(cancelSentRec.Body.Bytes(), &cancelErr))
	assert.Equal(t, "not_cancellable", cancelErr.Error.Code)
	require.Len(t, cancelErr.Error.Details, 1)
	assert.Equal(t, "current_status", cancelErr.Error.Details[0].Code)
	assert.Equal(t, "sent", cancelErr.Error.Details[0].Message)

	// Retry non-failed -> 409 not_retryable.
	retrySentRec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/"+sentID.String()+"/retry", "", nil)
	require.Equal(t, http.StatusConflict, retrySentRec.Code)
	assert.Contains(t, retrySentRec.Body.String(), "not_retryable")

	// Retry failed -> 200 pending (resets attempts=0) + enqueued.
	failedID, err := uuid.NewV7()
	require.NoError(t, err)
	lastErr := "provider returned 500"
	store.SeedNotification(domain.Notification{
		ID:          failedID,
		Channel:     domain.ChannelEmail,
		Priority:    domain.PriorityHigh,
		Status:      domain.StatusFailed,
		Recipient:   "a@example.com",
		Content:     "retry me",
		Attempts:    5,
		MaxAttempts: 5,
		Deferrals:   2,
		LastError:   &lastErr,
		CreatedAt:   now,
		UpdatedAt:   now,
		FailedAt:    &now,
	})
	retryFailedRec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/"+failedID.String()+"/retry", "", nil)
	require.Equal(t, http.StatusOK, retryFailedRec.Code)
	var retried api.NotificationResponse
	require.NoError(t, json.Unmarshal(retryFailedRec.Body.Bytes(), &retried))
	assert.Equal(t, "pending", retried.Status)
	assert.Equal(t, 0, retried.Attempts)
	assert.Nil(t, retried.LastError)
	require.Len(t, enq.Items(), 1)
	assert.Equal(t, failedID, enq.Items()[0].ID)

	// Cancel newly pending notification -> 200 cancelled.
	cancelOKRec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications/"+failedID.String()+"/cancel", "", nil)
	require.Equal(t, http.StatusOK, cancelOKRec.Code)
	var cancelled api.NotificationResponse
	require.NoError(t, json.Unmarshal(cancelOKRec.Body.Bytes(), &cancelled))
	assert.Equal(t, "cancelled", cancelled.Status)
}

func TestListNotificationsCursorPagination3PagesAndInvalidCursor(t *testing.T) {
	t.Parallel()

	h, store, _ := newTestRouter(t)

	// Seed 5 matching SMS notifications across distinct timestamps for deterministic 3-page pagination (limit=2 -> 2+2+1).
	base := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	expectedIDsDesc := make([]uuid.UUID, 5)
	for i := 0; i < 5; i++ {
		id, err := uuid.NewV7()
		require.NoError(t, err)
		ts := base.Add(time.Duration(i) * time.Minute)
		store.SeedNotification(domain.Notification{
			ID:          id,
			Channel:     domain.ChannelSMS,
			Priority:    domain.PriorityNormal,
			Status:      domain.StatusPending,
			Recipient:   "+905551234567",
			Content:     fmt.Sprintf("msg-%d", i),
			MaxAttempts: 5,
			CreatedAt:   ts,
			UpdatedAt:   ts,
		})
		expectedIDsDesc[4-i] = id
	}
	// Seed one non-matching email notification to verify filter correctness.
	emailID, err := uuid.NewV7()
	require.NoError(t, err)
	store.SeedNotification(domain.Notification{
		ID:          emailID,
		Channel:     domain.ChannelEmail,
		Priority:    domain.PriorityLow,
		Status:      domain.StatusSent,
		Recipient:   "other@example.com",
		Content:     "ignore",
		MaxAttempts: 5,
		CreatedAt:   base.Add(10 * time.Minute),
		UpdatedAt:   base.Add(10 * time.Minute),
	})

	var collected []uuid.UUID
	var cursorParam string
	for pageNum := 0; pageNum < 3; pageNum++ {
		u := "/api/v1/notifications?status=pending,retrying&channel=sms&limit=2"
		if cursorParam != "" {
			u += "&cursor=" + cursorParam
		}
		rec := doJSONRequest(t, h, http.MethodGet, u, "", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var page api.NotificationListResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
		for _, item := range page.Data {
			collected = append(collected, item.ID)
		}
		if pageNum < 2 {
			require.NotNil(t, page.NextCursor, "page %d must return nextCursor", pageNum+1)
			cursorParam = *page.NextCursor
		} else {
			assert.Nil(t, page.NextCursor, "final page must have nextCursor=null")
		}
	}
	assert.Equal(t, expectedIDsDesc, collected)

	// Invalid cursor -> 400 invalid_cursor.
	recBadCursor := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications?cursor=not-valid-base64!!!", "", nil)
	assert.Equal(t, http.StatusBadRequest, recBadCursor.Code)
	assert.Contains(t, recBadCursor.Body.String(), "invalid_cursor")
}

func TestCorrelationIDEchoHeaderAndErrorBodyAndCORSPreflight(t *testing.T) {
	t.Parallel()

	h, _, _ := newTestRouter(t)

	// Explicit correlation ID echoed in both header and JSON error body.
	rec := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications/invalid", "", map[string]string{
		"X-Correlation-ID": "client-trace-99",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "client-trace-99", rec.Header().Get("X-Correlation-ID"))
	var errResp api.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "client-trace-99", errResp.CorrelationID)

	// CORS preflight through full router returns 204 with configured Swagger origin and exposed headers.
	reqPre := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/api/v1/notifications", nil)
	reqPre.Header.Set("Origin", "http://localhost:8081")
	reqPre.Header.Set("Access-Control-Request-Method", "POST")
	recPre := httptest.NewRecorder()
	h.ServeHTTP(recPre, reqPre)
	assert.Equal(t, http.StatusNoContent, recPre.Code)
	assert.Equal(t, "http://localhost:8081", recPre.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "Idempotent-Replayed, X-Correlation-ID", recPre.Header().Get("Access-Control-Expose-Headers"))
}

func TestCreateNotificationRejectsBodyIdempotencyKey(t *testing.T) {
	t.Parallel()

	h, store, _ := newTestRouter(t)
	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"hi","idempotencyKey":"body-only-key"}`,
		nil,
	)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	var errResp api.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	require.Len(t, errResp.Error.Details, 1)
	assert.Equal(t, "idempotencyKey", errResp.Error.Details[0].Field)
	assert.Equal(t, "unsupported_field", errResp.Error.Details[0].Code)
	assert.Equal(t, 0, store.NotificationCount())
}

func TestDatabaseUnavailableReturns503(t *testing.T) {
	t.Parallel()

	t.Run("pgconn SQLSTATE 57P03 maps to 503", func(t *testing.T) {
		t.Parallel()
		h, store, _ := newTestRouter(t)
		store.SetForcedError(&pgconn.PgError{Code: "57P03", Message: "the database system is starting up"})

		rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications",
			`{"recipient":"+905551234567","channel":"sms","content":"hi"}`,
			nil,
		)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Contains(t, rec.Body.String(), "service_unavailable")
		assert.NotContains(t, rec.Body.String(), "starting up")
	})

	t.Run("net.OpError maps to 503 on read", func(t *testing.T) {
		t.Parallel()
		h, store, _ := newTestRouter(t)
		store.SetForcedError(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})

		id, err := uuid.NewV7()
		require.NoError(t, err)
		rec := doJSONRequest(t, h, http.MethodGet, "/api/v1/notifications/"+id.String(), "", nil)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Contains(t, rec.Body.String(), "service_unavailable")
		assert.NotContains(t, rec.Body.String(), "connection refused")
	})
}
