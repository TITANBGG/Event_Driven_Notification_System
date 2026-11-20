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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}, &out)
	require.NoError(t, err)
	assert.Equal(t, 25, res.Submitted)
	assert.Len(t, res.BatchLatencies, 3)
	assert.Equal(t, int64(25), res.FinalStatuses["sent"])
	assert.Contains(t, out.String(), "created=25")
	assert.Contains(t, out.String(), "completed=25")
}
