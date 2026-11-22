package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPriorityMixParsingAndDistribution(t *testing.T) {
	t.Parallel()

	w, err := parsePriorityMix("20:60:20")
	require.NoError(t, err)

	counts := map[string]int{}
	for i := 0; i < 100; i++ {
		counts[selectPriority(w, i)]++
	}
	assert.Equal(t, 20, counts["high"])
	assert.Equal(t, 60, counts["normal"])
	assert.Equal(t, 20, counts["low"])

	_, err = parsePriorityMix("50:50:10")
	assert.Error(t, err)
	_, err = parsePriorityMix("20:80")
	assert.Error(t, err)
}

func TestPercentileFloat(t *testing.T) {
	t.Parallel()

	samples := []float64{50, 10, 40, 20, 30}
	assert.InDelta(t, 30.0, percentileFloat(samples, 0.50), 0.01)
	assert.InDelta(t, 48.0, percentileFloat(samples, 0.95), 0.01)
	assert.Equal(t, 0.0, percentileFloat(nil, 0.50))
}

func TestRunLoadgenEndToEnd(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		batches = map[string]int{}
		polls   = map[string]int{}
	)
	seq := 0

	const apiKey = "loadgen-test-key-0123456789"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/notifications/batch":
			var req struct {
				Notifications []json.RawMessage `json:"notifications"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			mu.Lock()
			seq++
			bid := fmt.Sprintf("batch-%d", seq)
			batches[bid] = len(req.Notifications)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"batchId":%q,"count":%d,"notificationIds":[]}`, bid, len(req.Notifications))

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/batches/"):
			bid := strings.TrimPrefix(r.URL.Path, "/api/v1/batches/")
			mu.Lock()
			total := batches[bid]
			polls[bid]++
			done := polls[bid] >= 2
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			statusKey := "pending"
			if done {
				statusKey = "sent"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"batchId":        bid,
				"total":          total,
				"countsByStatus": map[string]int{statusKey: total},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	var out bytes.Buffer
	res, err := runLoadgen(t.Context(), loadConfig{
		baseURL:     srv.URL,
		total:       25,
		batchSize:   10,
		concurrency: 2,
		channel:     "mixed",
		priorityMix: "20:60:20",
		pollTimeout: 5 * time.Second,
		pollEvery:   10 * time.Millisecond,
		apiKey:      apiKey,
	}, &out)
	require.NoError(t, err)
	assert.Equal(t, 25, res.Submitted)
	assert.Len(t, res.BatchLatencies, 3)
	assert.Equal(t, int64(25), res.FinalStatuses["sent"])
	assert.Contains(t, out.String(), "created=25")
	assert.Contains(t, out.String(), "completed=25")
}

func TestChannelMixDistributionIsExactPerHundredAndDecorrelated(t *testing.T) {
	t.Parallel()

	cw, err := parseChannelMix("50:30:20")
	require.NoError(t, err)
	pw, err := parsePriorityMix("20:60:20")
	require.NoError(t, err)

	counts := map[string]int{}
	highByChannel := map[string]int{}
	for seq := 0; seq < 100; seq++ {
		ch, chErr := selectChannel("mixed", cw, seq)
		require.NoError(t, chErr)
		counts[ch]++
		if selectPriority(pw, seq) == "high" {
			highByChannel[ch]++
		}
	}
	assert.Equal(t, map[string]int{"sms": 50, "email": 30, "push": 20}, counts)
	// With a plain seq%100 slot all 20 "high" items would land on sms; the stride spreads them.
	assert.Less(t, highByChannel["sms"], 20)
	assert.Positive(t, highByChannel["email"]+highByChannel["push"])

	ch, err := selectChannel("Email", cw, 7)
	require.NoError(t, err)
	assert.Equal(t, "email", ch)
	_, err = selectChannel("fax", cw, 0)
	assert.Error(t, err)
	_, err = parseChannelMix("50:50:1")
	assert.Error(t, err)
}

func TestChannelStatEffectiveRate(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	assert.InDelta(t, 100.0, channelStat{Sent: 1001, FirstSent: start, LastSent: start.Add(10 * time.Second)}.EffectiveRate(), 0.001)
	assert.Zero(t, channelStat{Sent: 1, FirstSent: start, LastSent: start}.EffectiveRate())
}

func TestCollectChannelStatsFollowsCursorPages(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{"channel": "sms", "sentAt": base},
					{"channel": "email", "sentAt": base.Add(time.Second)},
					{"channel": "push", "sentAt": nil},
				},
				"nextCursor": "page/2+",
			})
			return
		}
		assert.Equal(t, "page/2+", r.URL.Query().Get("cursor"), "cursor must be URL-escaped and round-trip intact")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items":      []map[string]any{{"channel": "sms", "sentAt": base.Add(2 * time.Second)}},
			"nextCursor": nil,
		})
	}))
	defer srv.Close()

	stats, err := collectChannelStats(t.Context(), srv.Client(), srv.URL, []string{"b1"})
	require.NoError(t, err)
	assert.Equal(t, 2, stats["sms"].Sent)
	assert.Equal(t, 2*time.Second, stats["sms"].LastSent.Sub(stats["sms"].FirstSent))
	assert.Equal(t, 1, stats["email"].Sent)
	assert.NotContains(t, stats, "push", "unsent items are not counted")
}
