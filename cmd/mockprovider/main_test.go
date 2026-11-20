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

func TestMockProviderAcceptedContractAndStats(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 11, 20, 19, 55, 0, 0, time.UTC)
	ms := newMockServer(mockConfig{Seed: 42}, observability.NewLogger("error", io.Discard), func() time.Time {
		return fixedNow
	})
	ts := httptest.NewServer(ms.routes())
	defer ts.Close()

	for i := 0; i < 2; i++ {
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			ts.URL+"/",
			bytes.NewBufferString(`{"to":"+905551234567","channel":"sms","content":"hi"}`),
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

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewBufferString(`{}`))
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

func TestMockProviderConcurrentRaceFree(t *testing.T) {
	t.Parallel()

	ms := newMockServer(mockConfig{Seed: 7, LatencyMS: 1, LatencyJitterMS: 2}, observability.NewLogger("error", io.Discard), nil)
	ts := httptest.NewServer(ms.routes())
	defer ts.Close()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewBufferString(`{"to":"+905551234567"}`))
			if !assert.NoError(t, err) {
				return
			}
			req.Header.Set("Idempotency-Key", "k")
			resp, err := ts.Client().Do(req)
			if assert.NoError(t, err) {
				assert.Equal(t, http.StatusAccepted, resp.StatusCode)
				assert.NoError(t, resp.Body.Close())
			}
			_ = idx
		}(g)
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
}
