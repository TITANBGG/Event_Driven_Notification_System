package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/observability"
)

const validSMSPayload = `{"to":"+905551234567","channel":"sms","content":"Your message"}`

func TestMockProviderAcceptedContractAndStats(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 11, 20, 19, 55, 0, 0, time.UTC)
	var logBuf bytes.Buffer
	ms := newMockServer(mockConfig{Seed: 42}, observability.NewLogger("info", &logBuf), func() time.Time {
		return fixedNow
	})
	ts := httptest.NewServer(ms.routes())
	defer ts.Close()

	for i := 0; i < 2; i++ {
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			ts.URL+"/",
			bytes.NewBufferString(validSMSPayload),
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "notif-1")
		req.Header.Set("X-Correlation-ID", "corr-1")

		resp, err := ts.Client().Do(req)
		require.NoError(t, err)

		var body acceptedResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		require.NoError(t, resp.Body.Close())

		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
		assert.NotEmpty(t, body.MessageID)
		assert.Equal(t, "accepted", body.Status)
		assert.Equal(t, fixedNow.Format(time.RFC3339), body.Timestamp)
	}

	logged := logBuf.String()
	assert.NotContains(t, logged, "+905551234567", "recipient PII must be masked in logs")
	assert.NotContains(t, logged, "Your message", "message content must never be logged")
	assert.Contains(t, logged, "+90555***4567")

	statsReq, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/stats", nil)
	require.NoError(t, err)
	statsResp, err := ts.Client().Do(statsReq)
	require.NoError(t, err)
	var stats statsResponse
	require.NoError(t, json.NewDecoder(statsResp.Body).Decode(&stats))
	require.NoError(t, statsResp.Body.Close())

	assert.Equal(t, 2, stats.Received)
	assert.Equal(t, 2, stats.ByKey["notif-1"])
	assert.Len(t, stats.Timestamps, 2)

	resetReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/reset", nil)
	require.NoError(t, err)
	resetResp, err := ts.Client().Do(resetReq)
	require.NoError(t, err)
	require.NoError(t, resetResp.Body.Close())
	assert.Equal(t, http.StatusOK, resetResp.StatusCode)

	statsReq2, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/stats", nil)
	require.NoError(t, err)
	statsResp2, err := ts.Client().Do(statsReq2)
	require.NoError(t, err)
	var statsAfter statsResponse
	require.NoError(t, json.NewDecoder(statsResp2.Body).Decode(&statsAfter))
	require.NoError(t, statsResp2.Body.Close())
	assert.Equal(t, 0, statsAfter.Received)
	assert.Empty(t, statsAfter.ByKey)
}

func TestMockProviderPayloadValidation(t *testing.T) {
	t.Parallel()

	ms := newMockServer(mockConfig{Seed: 1}, observability.NewLogger("error", io.Discard), nil)
	ts := httptest.NewServer(ms.routes())
	t.Cleanup(ts.Close)

	badPayloads := []struct {
		name string
		body string
	}{
		{name: "empty json object", body: `{}`},
		{name: "missing content", body: `{"to":"+905551234567","channel":"sms"}`},
		{name: "unknown channel", body: `{"to":"+905551234567","channel":"fax","content":"hi"}`},
		{name: "unknown field", body: `{"to":"+905551234567","channel":"sms","content":"hi","extra":1}`},
		{name: "malformed json", body: `{not json`},
	}

	t.Run("cases", func(t *testing.T) {
		for _, tc := range badPayloads {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewBufferString(tc.body))
				require.NoError(t, err)
				resp, err := ts.Client().Do(req)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			})
		}
	})

	// Invalid payloads must not inflate /stats counters.
	statsReq, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/stats", nil)
	require.NoError(t, err)
	statsResp, err := ts.Client().Do(statsReq)
	require.NoError(t, err)
	var stats statsResponse
	require.NoError(t, json.NewDecoder(statsResp.Body).Decode(&stats))
	require.NoError(t, statsResp.Body.Close())
	assert.Equal(t, 0, stats.Received)
}

func TestMockProviderFaultInjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		cfg            mockConfig
		wantStatus     int
		wantRetryAfter string
	}{
		{
			name:       "permanent 400",
			cfg:        mockConfig{PermanentRate: 1.0, Seed: 1},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:           "rate limit 429",
			cfg:            mockConfig{Rate429: 1.0, Seed: 1},
			wantStatus:     http.StatusTooManyRequests,
			wantRetryAfter: "1",
		},
		{
			name:       "transient 500",
			cfg:        mockConfig{FailureRate: 1.0, Seed: 1},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ms := newMockServer(tc.cfg, observability.NewLogger("error", io.Discard), nil)
			ts := httptest.NewServer(ms.routes())
			defer ts.Close()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewBufferString(validSMSPayload))
			require.NoError(t, err)
			resp, err := ts.Client().Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			if tc.wantRetryAfter != "" {
				assert.Equal(t, tc.wantRetryAfter, resp.Header.Get("Retry-After"))
			}
		})
	}
}

func TestMockProviderTimestampsBoundedAndRaceFree(t *testing.T) {
	t.Parallel()

	tick := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	ms := newMockServer(mockConfig{Seed: 7, LatencyMS: 1, LatencyJitterMS: 2}, observability.NewLogger("error", io.Discard), func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		tick = tick.Add(time.Millisecond)
		return tick
	})
	ms.maxTimestamps = 10
	ts := httptest.NewServer(ms.routes())
	defer ts.Close()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewBufferString(validSMSPayload))
			if !assert.NoError(t, err) {
				return
			}
			req.Header.Set("Idempotency-Key", "k")
			resp, err := ts.Client().Do(req)
			if assert.NoError(t, err) {
				assert.Equal(t, http.StatusAccepted, resp.StatusCode)
				assert.NoError(t, resp.Body.Close())
			}
		}()
	}
	wg.Wait()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/stats", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	var stats statsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 16, stats.Received)
	assert.Equal(t, 16, stats.ByKey["k"])
	require.Len(t, stats.Timestamps, 10, "timestamps buffer must stay bounded to maxTimestamps")
	for i := 1; i < len(stats.Timestamps); i++ {
		assert.False(t, stats.Timestamps[i].Before(stats.Timestamps[i-1]), "timestamps from ring buffer must stay chronological")
	}
}

func TestMockProviderRingBufferEvictionOrder(t *testing.T) {
	t.Parallel()

	t.Run("retains newest timestamps in chronological order on wraparound", func(t *testing.T) {
		t.Parallel()
		tick := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)
		ms := newMockServer(mockConfig{Seed: 1}, observability.NewLogger("error", io.Discard), func() time.Time {
			tick = tick.Add(time.Second)
			return tick
		})
		ms.maxTimestamps = 4

		// Alternate recordAndDecide (+1,+3,+5,...) and acceptedResponse formatting (+2,+4,+6,...).
		var expected []time.Time
		for i := 0; i < 6; i++ {
			_, _ = ms.recordAndDecide("k", 0)
			_ = ms.nowFn()
			expected = append(expected, tick.Add(-time.Second))
		}

		ts := httptest.NewServer(ms.routes())
		t.Cleanup(ts.Close)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/stats", nil)
		require.NoError(t, err)
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		var stats statsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
		require.NoError(t, resp.Body.Close())

		assert.Equal(t, 6, stats.Received)
		assert.Equal(t, expected[2:], stats.Timestamps)
	})
}
