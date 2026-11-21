//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/worker"
)

// postRaw is a goroutine-safe POST helper: unlike doJSON it never calls require (t.FailNow must
// only run on the test goroutine), so concurrent callers report errors back instead.
func postRaw(ctx context.Context, url string, headers map[string]string, body any) (int, http.Header, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, respBody, err
}

func smsBatchItems(n, recipientBase int, label string) []map[string]any {
	items := make([]map[string]any, n)
	for i := range n {
		items[i] = map[string]any{
			"recipient": fmt.Sprintf("+90555%07d", recipientBase+i),
			"channel":   "sms",
			"priority":  "normal",
			"content":   fmt.Sprintf("%s %d", label, i),
		}
	}
	return items
}

func (inf *testInfra) countByStatus(t *testing.T, batchID uuid.UUID, status string) int {
	t.Helper()
	var n int
	require.NoError(t, inf.pool.QueryRow(t.Context(),
		"SELECT count(*) FROM notifications WHERE batch_id = $1 AND status = $2", batchID, status).Scan(&n))
	return n
}

func runExtendedScenarios(t *testing.T, inf *testInfra) {
	t.Helper()

	t.Run("n_circuit_breaker_opens_on_503_outage_without_burning_attempts_then_recovers", func(t *testing.T) {
		inf.resetState(t)
		inf.provider.setForceStatus(http.StatusServiceUnavailable)

		stopWorker := inf.startWorkerWithDeps(t, func(c *config.Config) {
			c.WorkerConcurrency = 2
			c.CBConsecutiveFail = 3
			c.CBMinRequests = 1000 // isolate the consecutive-failure trip rule
			c.CBOpenTimeout = 300 * time.Millisecond
			c.CBHalfOpenMax = 1
		}, 500, 10, func(d *worker.Deps) {
			// Zero jitter: circuit deferrals land exactly CB_OPEN_TIMEOUT later (production adds up to 5s).
			d.Jitter = func(time.Duration) time.Duration { return 0 }
		})
		defer stopWorker()

		var batch api.BatchCreateResponse
		code, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil,
			map[string]any{"notifications": smsBatchItems(10, 8100000, "Outage")}, &batch)
		require.Equal(t, http.StatusAccepted, code)

		// The breaker must open: deferrals are recorded as circuit_open attempts that do not count.
		require.Eventually(t, func() bool {
			var deferred int
			_ = inf.pool.QueryRow(t.Context(), `SELECT count(*) FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
				WHERE n.batch_id = $1 AND a.outcome = 'circuit_open'`, batch.BatchID).Scan(&deferred)
			return deferred >= 10
		}, 10*time.Second, 25*time.Millisecond)

		var countedCircuitOpen int
		require.NoError(t, inf.pool.QueryRow(t.Context(), `SELECT count(*) FROM delivery_attempts
			WHERE outcome = 'circuit_open' AND counted`).Scan(&countedCircuitOpen))
		assert.Zero(t, countedCircuitOpen, "circuit deferrals must never consume an attempt")
		assert.Zero(t, inf.countByStatus(t, batch.BatchID, "failed"), "an outage shorter than the deferral budget must not dead-letter anything")
		outageRequests := inf.provider.requestCount()
		assert.Less(t, outageRequests, 10*inf.cfg.MaxAttempts, "open breaker must shield the provider (saw %d requests)", outageRequests)

		inf.provider.setForceStatus(0)

		require.Eventually(t, func() bool {
			return inf.countByStatus(t, batch.BatchID, "sent") == 10
		}, 15*time.Second, 50*time.Millisecond)

		// attempts == number of counted attempt rows, and never reached MAX_ATTEMPTS.
		rows, err := inf.pool.Query(t.Context(), `SELECT n.attempts, count(a.id) FILTER (WHERE a.counted)
			FROM notifications n LEFT JOIN delivery_attempts a ON a.notification_id = n.id
			WHERE n.batch_id = $1 GROUP BY n.id, n.attempts`, batch.BatchID)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var attempts, counted int
			require.NoError(t, rows.Scan(&attempts, &counted))
			assert.Equal(t, counted, attempts)
			assert.Less(t, attempts, inf.cfg.MaxAttempts)
		}
		require.NoError(t, rows.Err())
		t.Logf("circuit breaker: %d provider requests during outage for 10 notifications", outageRequests)
	})

	t.Run("o_worker_crash_mid_batch_recovered_by_reconciler_with_bounded_duplicates", func(t *testing.T) {
		inf.resetState(t)
		ctx := t.Context()

		var batch api.BatchCreateResponse
		code, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil,
			map[string]any{"notifications": smsBatchItems(30, 8200000, "Crash")}, &batch)
		require.Equal(t, http.StatusAccepted, code)

		// Reproduce exactly what a SIGKILLed worker leaves behind: it LMOVEd 10 items into its inflight
		// list and claimed them in Postgres; 5 provider calls were accepted but the process died before
		// MarkSent/Ack. No graceful requeue runs and its heartbeat disappears.
		const crashed = "worker-crashed"
		require.NoError(t, inf.q.Heartbeat(ctx, crashed, time.Minute))
		var claimed []uuid.UUID
		for range 10 {
			meta, ok, err := inf.q.Dequeue(ctx, domain.ChannelSMS, crashed)
			require.NoError(t, err)
			require.True(t, ok)
			n, ok, err := inf.repo.Claim(ctx, meta.ID, inf.cfg.ClockSkewTolerance)
			require.NoError(t, err)
			require.True(t, ok)
			claimed = append(claimed, n.ID)
		}
		preSent := claimed[:5]
		for _, id := range preSent {
			n, err := inf.repo.GetNotification(ctx, id)
			require.NoError(t, err)
			status, _, _, err := postRaw(ctx, inf.provider.srv.URL, map[string]string{"Idempotency-Key": id.String()},
				map[string]string{"to": n.Recipient, "channel": string(n.Channel), "content": n.Content})
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, status)
		}
		require.NoError(t, inf.q.DeleteHeartbeat(ctx, crashed))

		stopWorker := inf.startWorker(t, func(c *config.Config) {
			c.WorkerID = "worker-survivor"
			c.VisibilityTimeout = time.Second
		}, 500, 10)
		defer stopWorker()

		require.Eventually(t, func() bool {
			return inf.countByStatus(t, batch.BatchID, "sent") == 30
		}, 20*time.Second, 50*time.Millisecond)

		orphanLeft, err := inf.rdb.Exists(ctx, queue.InflightKey(crashed)).Result()
		require.NoError(t, err)
		assert.Zero(t, orphanLeft, "reconciler step (d) must drain the dead worker's inflight list")

		var abandoned int
		require.NoError(t, inf.pool.QueryRow(ctx, "SELECT count(*) FROM delivery_attempts WHERE outcome = 'abandoned'").Scan(&abandoned))
		assert.Equal(t, 10, abandoned, "every row stuck in processing is reclaimed exactly once by step (c)")

		preSentSet := make(map[uuid.UUID]bool, len(preSent))
		for _, id := range preSent {
			preSentSet[id] = true
		}
		duplicates := 0
		for _, id := range batch.NotificationIDs {
			calls := inf.provider.callsForID(id.String())
			if preSentSet[id] {
				assert.Len(t, calls, 2, "accepted-before-crash send is redelivered (at-least-once)")
				for _, c := range calls {
					assert.Equal(t, id.String(), c.IdempotencyID, "redelivery reuses the same Idempotency-Key for provider dedupe")
				}
				duplicates += len(calls) - 1
				continue
			}
			assert.Len(t, calls, 1, "notification %s must be sent exactly once", id)
		}
		assert.Equal(t, len(preSent), duplicates, "duplicates are bounded by the sends in flight at crash time")
		t.Logf("worker crash: 30/30 sent, %d abandoned rows reclaimed, %d duplicate provider calls", abandoned, duplicates)
	})

	t.Run("p_idempotency_20_concurrent_posts_same_key_create_exactly_one_row", func(t *testing.T) {
		inf.resetState(t)
		body := map[string]any{"recipient": "+905558300001", "channel": "sms", "content": "Concurrent idempotent create"}

		type result struct {
			code     int
			id       string
			replayed bool
			err      error
		}
		const callers = 20
		results := make([]result, callers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				code, hdr, raw, err := postRaw(t.Context(), inf.apiSrv.URL+"/api/v1/notifications",
					map[string]string{"Idempotency-Key": "concurrent-key-1"}, body)
				r := result{code: code, err: err}
				if err == nil {
					var resp api.NotificationResponse
					r.err = json.Unmarshal(raw, &resp)
					r.id = resp.ID.String()
					r.replayed = hdr.Get("Idempotent-Replayed") == "true"
				}
				results[i] = r
			}()
		}
		close(start)
		wg.Wait()

		ids := make(map[string]int)
		fresh := 0
		for _, r := range results {
			require.NoError(t, r.err)
			assert.Equal(t, http.StatusAccepted, r.code)
			ids[r.id]++
			if !r.replayed {
				fresh++
			}
		}
		assert.Len(t, ids, 1, "every caller must observe the same notification id")
		assert.Equal(t, 1, fresh, "exactly one request creates; the other 19 are replays")

		var rows int
		require.NoError(t, inf.pool.QueryRow(t.Context(), "SELECT count(*) FROM notifications").Scan(&rows))
		assert.Equal(t, 1, rows)
	})

	t.Run("q_manual_retry_failed_to_sent", func(t *testing.T) {
		inf.resetState(t)
		const recipient = "+905558400001"
		inf.provider.setSequence(recipient, []int{http.StatusBadRequest})
		stopWorker := inf.startWorker(t, nil, 500, 10)
		defer stopWorker()

		var created api.NotificationResponse
		code, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil,
			map[string]any{"recipient": recipient, "channel": "sms", "content": "Manual DLQ replay"}, &created)
		require.Equal(t, http.StatusAccepted, code)

		detailURL := inf.apiSrv.URL + "/api/v1/notifications/" + created.ID.String()
		var detail api.NotificationDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, detailURL, nil, nil, &detail)
			return c == http.StatusOK && detail.Status == "failed"
		}, 10*time.Second, 25*time.Millisecond)
		dlq, err := inf.rdb.LRange(t.Context(), queue.DLQListKey, 0, -1).Result()
		require.NoError(t, err)
		assert.Contains(t, dlq, created.ID.String())

		var retried api.NotificationResponse
		code, _ = doJSON(t, http.MethodPost, detailURL+"/retry", nil, nil, &retried)
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, "pending", string(retried.Status))
		assert.Zero(t, retried.Attempts, "manual retry resets the attempt budget")
		assert.Nil(t, retried.LastError)
		dlq, err = inf.rdb.LRange(t.Context(), queue.DLQListKey, 0, -1).Result()
		require.NoError(t, err)
		assert.NotContains(t, dlq, created.ID.String(), "a replayed notification leaves the DLQ list")

		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, detailURL, nil, nil, &detail)
			return c == http.StatusOK && detail.Status == "sent"
		}, 10*time.Second, 25*time.Millisecond)
		assert.Equal(t, 1, detail.Attempts)
		assert.Len(t, detail.DeliveryAttempts, 2, "history keeps the permanent failure and the successful replay")

		code, _ = doJSON(t, http.MethodPost, detailURL+"/retry", nil, nil, nil)
		assert.Equal(t, http.StatusConflict, code, "only failed notifications can be retried")
	})

	// The metrics queries were rewritten for speed (one scan per outcome, array percentiles); this pins
	// their results on rows with known timestamps so any later rewrite must keep the same answers.
	t.Run("r_metrics_windows_and_percentiles_from_known_rows", func(t *testing.T) {
		inf.resetState(t)
		ctx := t.Context()
		// sms sent at now-s, created k seconds earlier (end-to-end k s), provider call k*100 ms.
		_, err := inf.pool.Exec(ctx, `
			WITH n AS (
			  INSERT INTO notifications (id, channel, priority, status, recipient, content, max_attempts, created_at, sent_at)
			  SELECT gen_random_uuid(), 'sms', 'normal', 'sent', '+905550000000', 'metrics', 5,
			         now() - make_interval(secs => v.s + v.k), now() - make_interval(secs => v.s)
			  FROM (VALUES (30,1),(30,2),(180,3),(180,4),(180,5),(600,6),(600,7),(600,8),(600,9),(1200,10)) AS v(s, k)
			  RETURNING id, created_at, sent_at
			)
			INSERT INTO delivery_attempts (notification_id, attempt_no, counted, started_at, duration_ms, outcome)
			SELECT id, 1, true, sent_at, (EXTRACT(EPOCH FROM sent_at - created_at) * 100)::int, 'sent' FROM n`)
		require.NoError(t, err)
		_, err = inf.pool.Exec(ctx, `
			INSERT INTO notifications (id, channel, priority, status, recipient, content, max_attempts, created_at, failed_at)
			SELECT gen_random_uuid(), 'email', 'normal', 'failed', 'a@example.com', 'metrics', 5, now() - make_interval(secs => s), now() - make_interval(secs => s)
			FROM (VALUES (30),(240),(240)) AS v(s)`)
		require.NoError(t, err)
		_, err = inf.pool.Exec(ctx, `
			INSERT INTO notifications (id, channel, priority, status, recipient, content, max_attempts)
			SELECT gen_random_uuid(), 'push', 'normal', 'retrying', 'token', 'metrics', 5 FROM generate_series(1, 2)`)
		require.NoError(t, err)
		// Must not move the provider percentiles: a non-counted attempt now and a counted one 20 minutes ago.
		_, err = inf.pool.Exec(ctx, `
			INSERT INTO delivery_attempts (notification_id, attempt_no, counted, started_at, duration_ms, outcome)
			SELECT id, 2, c, now() - make_interval(secs => s), 99999, o
			FROM (SELECT id FROM notifications WHERE channel = 'sms' LIMIT 1) n,
			     (VALUES (false, 0, 'circuit_open'), (true, 1200, 'sent')) AS v(c, s, o)`)
		require.NoError(t, err)

		rows, err := inf.repo.MetricsWindowCounts(ctx)
		require.NoError(t, err)
		require.Len(t, rows, 9, "3 windows x 3 channels, all present even when zero")
		got := map[string][3]int64{}
		for _, r := range rows {
			got[r.Window+"/"+string(r.Channel)] = [3]int64{r.SentCount, r.FailedCount, r.RetryingCount}
		}
		want := map[string][3]int64{
			"1m/sms": {2, 0, 0}, "5m/sms": {5, 0, 0}, "15m/sms": {9, 0, 0},
			"1m/email": {0, 1, 0}, "5m/email": {0, 3, 0}, "15m/email": {0, 3, 0},
			"1m/push": {0, 0, 2}, "5m/push": {0, 0, 2}, "15m/push": {0, 0, 2},
		}
		assert.Equal(t, want, got)
		assert.Equal(t, "1m", rows[0].Window, "windows stay in chronological order")
		assert.Equal(t, "15m", rows[8].Window)

		lat, err := inf.repo.LatencyPercentiles(ctx)
		require.NoError(t, err)
		// 9 values 1000..9000 ms: p50 = 5000, p95 = 8000 + 0.6*1000, p99 = 8000 + 0.92*1000 (linear interpolation).
		assert.InDelta(t, 5000, lat.EndToEndP50MS, 1)
		assert.InDelta(t, 8600, lat.EndToEndP95MS, 1)
		assert.InDelta(t, 8920, lat.EndToEndP99MS, 1)
		assert.InDelta(t, 500, lat.ProviderP50MS, 0.01)
		assert.InDelta(t, 860, lat.ProviderP95MS, 0.01)
		assert.InDelta(t, 892, lat.ProviderP99MS, 0.01)
	})
}
