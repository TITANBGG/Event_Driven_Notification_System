package provider

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
)

func testNotification() domain.Notification {
	subj := "Subject"
	title := "Title"
	cid := "corr-provider-1"
	return domain.Notification{
		ID:            uuid.Must(uuid.NewV7()),
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityHigh,
		Status:        domain.StatusProcessing,
		Recipient:     "+905551234567",
		Subject:       &subj,
		Title:         &title,
		Content:       "hello world",
		CorrelationID: &cid,
	}
}

func TestSendPayloadContractAndHeaders(t *testing.T) {
	t.Parallel()

	n := testNotification()

	t.Run("default omits subject and title and sends EXT-1 body", func(t *testing.T) {
		t.Parallel()
		var gotBody map[string]any
		var gotIdemKey, gotCorrID, gotCT string

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotIdemKey = r.Header.Get("Idempotency-Key")
			gotCorrID = r.Header.Get("X-Correlation-ID")
			gotCT = r.Header.Get("Content-Type")
			raw, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(raw, &gotBody))
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"messageId":"msg-1","status":"accepted","timestamp":"2026-11-21T10:00:00Z"}`))
		}))
		defer ts.Close()

		c := NewClient(ClientConfig{URL: ts.URL, Timeout: 2 * time.Second, ExtendedFields: false, Logger: observability.NewLogger("error", io.Discard)})
		res, err := c.Send(t.Context(), n)
		require.NoError(t, err)
		assert.Equal(t, OutcomeSent, res.Outcome)
		require.NotNil(t, res.ProviderMessageID)
		assert.Equal(t, "msg-1", *res.ProviderMessageID)

		assert.Equal(t, n.ID.String(), gotIdemKey)
		assert.Equal(t, "corr-provider-1", gotCorrID)
		assert.Equal(t, "application/json", gotCT)
		assert.Len(t, gotBody, 3)
		assert.Equal(t, "+905551234567", gotBody["to"])
		assert.Equal(t, "sms", gotBody["channel"])
		assert.Equal(t, "hello world", gotBody["content"])
	})

	t.Run("extended fields flag includes optional subject and title", func(t *testing.T) {
		t.Parallel()
		var gotBody map[string]any
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(raw, &gotBody))
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"messageId":"msg-2","status":"accepted","timestamp":"2026-11-21T10:00:00Z"}`))
		}))
		defer ts.Close()

		c := NewClient(ClientConfig{URL: ts.URL, Timeout: 2 * time.Second, ExtendedFields: true, Logger: observability.NewLogger("error", io.Discard)})
		res, err := c.Send(t.Context(), n)
		require.NoError(t, err)
		assert.Equal(t, OutcomeSent, res.Outcome)
		assert.Equal(t, "Subject", gotBody["subject"])
		assert.Equal(t, "Title", gotBody["title"])
	})
}

func TestSendStatusClassificationMatrix(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	httpDatePlus12 := fixedNow.Add(12 * time.Second).Format(http.TimeFormat)

	tests := []struct {
		name           string
		status         int
		headers        map[string]string
		body           string
		wantOutcome    OutcomeKind
		wantMsgID      bool
		wantRetryAfter *time.Duration
	}{
		{name: "202 accepted with messageId", status: 202, body: `{"messageId":"id-202","status":"accepted"}`, wantOutcome: OutcomeSent, wantMsgID: true},
		{name: "200 ok tolerated as sent", status: 200, body: `{"messageId":"id-200"}`, wantOutcome: OutcomeSent, wantMsgID: true},
		{name: "201 created tolerated as sent", status: 201, body: `{"messageId":"id-201"}`, wantOutcome: OutcomeSent, wantMsgID: true},
		{name: "202 empty body still sent with nil messageId", status: 202, body: ``, wantOutcome: OutcomeSent, wantMsgID: false},
		{name: "301 redirect not followed is permanent", status: 301, headers: map[string]string{"Location": "http://example.com/new"}, wantOutcome: OutcomePermanent},
		{name: "400 permanent", status: 400, wantOutcome: OutcomePermanent},
		{name: "401 permanent", status: 401, wantOutcome: OutcomePermanent},
		{name: "403 permanent", status: 403, wantOutcome: OutcomePermanent},
		{name: "404 permanent", status: 404, wantOutcome: OutcomePermanent},
		{name: "408 transient", status: 408, wantOutcome: OutcomeTransient},
		{name: "409 permanent", status: 409, wantOutcome: OutcomePermanent},
		{name: "413 permanent", status: 413, wantOutcome: OutcomePermanent},
		{name: "422 permanent", status: 422, wantOutcome: OutcomePermanent},
		{name: "425 transient", status: 425, wantOutcome: OutcomeTransient},
		{name: "429 without Retry-After", status: 429, wantOutcome: OutcomeThrottled},
		{name: "429 Retry-After seconds", status: 429, headers: map[string]string{"Retry-After": "7"}, wantOutcome: OutcomeThrottled, wantRetryAfter: ptrDuration(7 * time.Second)},
		{name: "429 Retry-After HTTP date", status: 429, headers: map[string]string{"Retry-After": httpDatePlus12}, wantOutcome: OutcomeThrottled, wantRetryAfter: ptrDuration(12 * time.Second)},
		{name: "500 transient", status: 500, wantOutcome: OutcomeTransient},
		{name: "501 transient", status: 501, wantOutcome: OutcomeTransient},
		{name: "503 transient with Retry-After", status: 503, headers: map[string]string{"Retry-After": "5"}, wantOutcome: OutcomeTransient, wantRetryAfter: ptrDuration(5 * time.Second)},
	}

	n := testNotification()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			c := NewClient(ClientConfig{URL: ts.URL, Timeout: 2 * time.Second, Logger: observability.NewLogger("error", &bytes.Buffer{})})
			c.nowFn = func() time.Time { return fixedNow }

			res, err := c.Send(t.Context(), n)
			require.NoError(t, err)
			assert.Equal(t, tc.wantOutcome, res.Outcome)
			require.NotNil(t, res.HTTPStatus)
			assert.Equal(t, tc.status, *res.HTTPStatus)
			if tc.wantMsgID {
				assert.NotNil(t, res.ProviderMessageID)
			} else {
				assert.Nil(t, res.ProviderMessageID)
			}
			if tc.wantRetryAfter != nil {
				require.NotNil(t, res.RetryAfter)
				assert.Equal(t, *tc.wantRetryAfter, *res.RetryAfter)
			} else {
				assert.Nil(t, res.RetryAfter)
			}
		})
	}
}

func TestSendTransportTimeoutConnectionRefusedAndInvalidURL(t *testing.T) {
	t.Parallel()

	n := testNotification()

	t.Run("request timeout triggers transient", func(t *testing.T) {
		t.Parallel()
		done := make(chan struct{})
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-done:
			}
			w.WriteHeader(http.StatusAccepted)
		}))
		defer func() {
			close(done)
			ts.Close()
		}()

		c := NewClient(ClientConfig{URL: ts.URL, Timeout: 25 * time.Millisecond, Logger: observability.NewLogger("error", io.Discard)})
		res, err := c.Send(t.Context(), n)
		require.NoError(t, err)
		assert.Equal(t, OutcomeTransient, res.Outcome)
		assert.Nil(t, res.HTTPStatus)
	})

	t.Run("connection refused triggers transient", func(t *testing.T) {
		t.Parallel()
		ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		closedURL := ts.URL
		ts.Close()

		c := NewClient(ClientConfig{URL: closedURL, Timeout: time.Second, Logger: observability.NewLogger("error", io.Discard)})
		res, err := c.Send(t.Context(), n)
		require.NoError(t, err)
		assert.Equal(t, OutcomeTransient, res.Outcome)
	})

	t.Run("invalid URL triggers permanent", func(t *testing.T) {
		t.Parallel()
		c := NewClient(ClientConfig{URL: "://not-a-valid-url", Timeout: time.Second, Logger: observability.NewLogger("error", io.Discard)})
		res, err := c.Send(t.Context(), n)
		require.NoError(t, err)
		assert.Equal(t, OutcomePermanent, res.Outcome)
	})

	t.Run("bad TLS cert triggers permanent", func(t *testing.T) {
		t.Parallel()
		ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		defer ts.Close()

		c := NewClient(ClientConfig{URL: ts.URL, Timeout: time.Second, Logger: observability.NewLogger("error", io.Discard)})
		res, err := c.Send(t.Context(), n)
		require.NoError(t, err)
		assert.Equal(t, OutcomePermanent, res.Outcome)
	})
}

func ptrDuration(d time.Duration) *time.Duration {
	return &d
}
