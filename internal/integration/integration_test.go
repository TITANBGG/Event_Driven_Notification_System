//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/provider"
	"github.com/TITANBGG/notification-system/internal/queue"
	"github.com/TITANBGG/notification-system/internal/ratelimit"
	"github.com/TITANBGG/notification-system/internal/repository"
	"github.com/TITANBGG/notification-system/internal/worker"
	"github.com/TITANBGG/notification-system/migrations"
)

type recordedProviderCall struct {
	Body          map[string]any
	RawBody       string
	IdempotencyID string
	CorrelationID string
	ReceivedAt    time.Time
}

type mockProviderServer struct {
	srv *httptest.Server

	mu         sync.Mutex
	calls      []recordedProviderCall
	byID       map[string][]recordedProviderCall
	seqByRecip map[string][]int
	delay      time.Duration
}

func newMockProviderServer(t *testing.T) *mockProviderServer {
	t.Helper()
	m := &mockProviderServer{
		byID:       make(map[string][]recordedProviderCall),
		seqByRecip: make(map[string][]int),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockProviderServer) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	recip, _ := body["to"].(string)
	idemID := r.Header.Get("Idempotency-Key")
	corrID := r.Header.Get("X-Correlation-ID")

	m.mu.Lock()
	delay := m.delay
	status := http.StatusAccepted
	if seq, ok := m.seqByRecip[recip]; ok && len(seq) > 0 {
		status = seq[0]
		m.seqByRecip[recip] = seq[1:]
	}
	call := recordedProviderCall{
		Body:          body,
		RawBody:       strings.TrimSpace(string(raw)),
		IdempotencyID: idemID,
		CorrelationID: corrID,
		ReceivedAt:    time.Now().UTC(),
	}
	if status >= 200 && status < 300 {
		m.calls = append(m.calls, call)
		m.byID[idemID] = append(m.byID[idemID], call)
	}
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status >= 200 && status < 300 {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"messageId": "prov-" + idemID,
			"status":    "accepted",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	} else {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("simulated status %d", status),
		})
	}
}

func (m *mockProviderServer) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = nil
	m.byID = make(map[string][]recordedProviderCall)
	m.seqByRecip = make(map[string][]int)
	m.delay = 0
}

func (m *mockProviderServer) setSequence(recipient string, statuses []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]int, len(statuses))
	copy(cp, statuses)
	m.seqByRecip[recipient] = cp
}

func (m *mockProviderServer) setDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
}

func (m *mockProviderServer) callsForID(id string) []recordedProviderCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]recordedProviderCall, len(m.byID[id]))
	copy(cp, m.byID[id])
	return cp
}

func (m *mockProviderServer) allCalls() []recordedProviderCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]recordedProviderCall, len(m.calls))
	copy(cp, m.calls)
	return cp
}

type queueEnqueuer struct {
	q *queue.Queue
}

func (e queueEnqueuer) Enqueue(ctx context.Context, items []api.QueueItem) error {
	var (
		immediate []queue.Meta
		scheduled []queue.Meta
		dueAts    []time.Time
	)
	for _, it := range items {
		meta := queue.Meta{
			ID:            it.ID,
			Channel:       it.Channel,
			Priority:      it.Priority,
			CorrelationID: it.CorrelationID,
			TraceParent:   it.TraceParent,
		}
		if it.ScheduledAt != nil {
			scheduled = append(scheduled, meta)
			dueAts = append(dueAts, it.ScheduledAt.UTC())
		} else {
			immediate = append(immediate, meta)
		}
	}
	var errs []error
	if len(immediate) > 0 {
		if err := e.q.Enqueue(ctx, immediate...); err != nil {
			errs = append(errs, err)
		}
	}
	if len(scheduled) > 0 {
		if err := e.q.ScheduleBatch(ctx, scheduled, dueAts); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e queueEnqueuer) CancelDelayed(ctx context.Context, ids ...uuid.UUID) error {
	return e.q.CancelDelayed(ctx, ids...)
}

type testInfra struct {
	dbURL     string
	redisAddr string
	pool      *pgxpool.Pool
	rdb       *redis.Client
	repo      *repository.Store
	q         *queue.Queue
	provider  *mockProviderServer
	apiSrv    *httptest.Server
	cfg       config.Config
}

func startTestInfra(t *testing.T) *testInfra {
	t.Helper()
	ctx := t.Context()

	dbURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	redisAddr := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))

	if dbURL == "" {
		pgCtr, err := tcpostgres.Run(
			ctx,
			"postgres:16-alpine",
			tcpostgres.WithDatabase("notif_test"),
			tcpostgres.WithUsername("notif"),
			tcpostgres.WithPassword("notif"),
			tcpostgres.BasicWaitStrategies(),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = pgCtr.Terminate(context.Background()) })

		connStr, err := pgCtr.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
		dbURL = connStr
	}

	if redisAddr == "" {
		rdCtr, err := tcredis.Run(ctx, "redis:7-alpine")
		require.NoError(t, err)
		t.Cleanup(func() { _ = rdCtr.Terminate(context.Background()) })

		ep, err := rdCtr.Endpoint(ctx, "")
		require.NoError(t, err)
		redisAddr = ep
	}

	applyMigrations(t, dbURL)

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	rdb := queue.NewClient(redisAddr, 60)
	t.Cleanup(func() { _ = rdb.Close() })

	repo := repository.New(pool)
	q := queue.New(rdb)
	mockProv := newMockProviderServer(t)

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	cfg.DatabaseURL = dbURL
	cfg.RedisAddr = redisAddr
	cfg.ProviderURL = mockProv.srv.URL
	cfg.WorkerConcurrency = 6
	cfg.DelayedMoverIntv = 25 * time.Millisecond
	cfg.ReconcileInterval = 60 * time.Millisecond
	cfg.PendingStaleAfter = 2 * time.Second
	cfg.HeartbeatInterval = 100 * time.Millisecond
	cfg.HeartbeatTTL = 2 * time.Second
	cfg.RetryMin = 15 * time.Millisecond
	cfg.RetryBase = 25 * time.Millisecond
	cfg.RetryCap = 80 * time.Millisecond
	cfg.RetryAfterMax = time.Second

	logger := observability.NewLogger("error", io.Discard)
	router := api.NewRouter(api.Deps{
		Config:         cfg,
		Logger:         logger,
		Store:          api.NewRepositoryStore(repo),
		Enqueuer:       queueEnqueuer{q: q},
		QueueInspector: q,
		Metrics:        observability.NewAPIMetrics(),
	})
	apiSrv := httptest.NewServer(router)
	t.Cleanup(apiSrv.Close)

	return &testInfra{
		dbURL:     dbURL,
		redisAddr: redisAddr,
		pool:      pool,
		rdb:       rdb,
		repo:      repo,
		q:         q,
		provider:  mockProv,
		apiSrv:    apiSrv,
		cfg:       cfg,
	}
}

func applyMigrations(t *testing.T, dbURL string) {
	t.Helper()
	src, err := iofs.New(migrations.FS, ".")
	require.NoError(t, err)

	m, err := migrate.NewWithSourceInstance("iofs", src, dbURL)
	require.NoError(t, err)
	defer func() {
		_, _ = m.Close()
	}()

	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		require.NoError(t, err)
	}
}

func (inf *testInfra) resetState(t *testing.T) {
	t.Helper()
	_, err := inf.pool.Exec(t.Context(), "TRUNCATE TABLE delivery_attempts, notifications, batches, templates, idempotency_keys RESTART IDENTITY CASCADE")
	require.NoError(t, err)
	require.NoError(t, inf.rdb.FlushDB(t.Context()).Err())
	inf.provider.reset()
}

func (inf *testInfra) startWorker(t *testing.T, cfgOverride func(*config.Config), ratePerSec, burst int) func() {
	t.Helper()
	cfg := inf.cfg
	if cfgOverride != nil {
		cfgOverride(&cfg)
	}
	logger := observability.NewLogger("error", io.Discard)
	limiter := ratelimit.New(inf.rdb, ratePerSec, burst)
	client := provider.NewClient(provider.ClientConfig{
		URL:          cfg.ProviderURL,
		Timeout:      cfg.ProviderTimeout,
		MaxIdleConns: 64,
		Logger:       logger,
	})
	policy := worker.NewExponentialRetryPolicy(cfg.RetryBase, cfg.RetryCap, cfg.RetryMin, cfg.RetryAfterMax)
	breaker := worker.NewChannelBreaker(worker.ChannelBreakerConfig{
		Config: cfg,
		Logger: logger,
	})
	rec, err := worker.NewReconciler(worker.ReconcilerDeps{
		Config: cfg,
		Logger: logger,
		Store:  inf.repo,
		Queue:  inf.q,
	})
	require.NoError(t, err)

	w, err := worker.New(worker.Deps{
		Config:      cfg,
		Logger:      logger,
		Store:       inf.repo,
		Queue:       inf.q,
		RateLimiter: limiter,
		Sender:      client,
		RetryPolicy: policy,
		Breaker:     breaker,
		Reconciler:  rec,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			require.NoError(t, <-done)
		})
	}
	t.Cleanup(stop)
	return stop
}

func doJSON(t *testing.T, method, url string, headers map[string]string, reqBody any, out any) (int, http.Header) {
	t.Helper()
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, bodyReader)
	require.NoError(t, err)
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	rawResp, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if out != nil && len(rawResp) > 0 {
		require.NoError(t, json.Unmarshal(rawResp, out), "body=%s", string(rawResp))
	}
	return resp.StatusCode, resp.Header
}

func TestCoreIntegration(t *testing.T) {
	inf := startTestInfra(t)

	t.Run("a_single_notification_end_to_end", func(t *testing.T) {
		inf.resetState(t)
		stopWorker := inf.startWorker(t, nil, 500, 10)
		defer stopWorker()

		var created api.NotificationResponse
		code, hdr := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", map[string]string{
			"X-Correlation-ID": "corr-int-a1",
		}, map[string]any{
			"recipient": "+905551234567",
			"channel":   "sms",
			"content":   "Hello integration test",
			"priority":  "high",
		}, &created)
		require.Equal(t, http.StatusAccepted, code)
		assert.Equal(t, "corr-int-a1", hdr.Get("X-Correlation-ID"))

		var detail api.NotificationDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+created.ID.String(), nil, nil, &detail)
			return c == http.StatusOK && detail.Status == "sent"
		}, 10*time.Second, 25*time.Millisecond)

		assert.Equal(t, 1, detail.Attempts)
		require.Len(t, detail.DeliveryAttempts, 1)
		assert.Equal(t, "sent", detail.DeliveryAttempts[0].Outcome)

		calls := inf.provider.callsForID(created.ID.String())
		require.Len(t, calls, 1)
		assert.Equal(t, created.ID.String(), calls[0].IdempotencyID)
		assert.Equal(t, "corr-int-a1", calls[0].CorrelationID)
		assert.Equal(t, map[string]any{
			"to":      "+905551234567",
			"channel": "sms",
			"content": "Hello integration test",
		}, calls[0].Body)
	})

	t.Run("b_idempotent_create_replay_and_conflict", func(t *testing.T) {
		inf.resetState(t)

		payload := map[string]any{
			"recipient": "+905551234567",
			"channel":   "sms",
			"content":   "Idempotent message",
			"priority":  "normal",
		}
		var first, second api.NotificationResponse
		code1, hdr1 := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", map[string]string{
			"Idempotency-Key": "idem-key-b1",
		}, payload, &first)
		require.Equal(t, http.StatusAccepted, code1)
		assert.Empty(t, hdr1.Get("Idempotent-Replayed"))

		code2, hdr2 := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", map[string]string{
			"Idempotency-Key": "idem-key-b1",
		}, payload, &second)
		require.Equal(t, http.StatusAccepted, code2)
		assert.Equal(t, "true", hdr2.Get("Idempotent-Replayed"))
		assert.Equal(t, first.ID, second.ID)

		var rowCount int
		require.NoError(t, inf.pool.QueryRow(t.Context(), "SELECT count(*) FROM notifications").Scan(&rowCount))
		assert.Equal(t, 1, rowCount)

		code3, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", map[string]string{
			"Idempotency-Key": "idem-key-b1",
		}, map[string]any{
			"recipient": "+905551234567",
			"channel":   "sms",
			"content":   "Different payload",
			"priority":  "normal",
		}, nil)
		assert.Equal(t, http.StatusConflict, code3)
	})

	t.Run("c_batch_1000_and_batch_key_replay", func(t *testing.T) {
		inf.resetState(t)
		stopWorker := inf.startWorker(t, func(c *config.Config) {
			c.WorkerConcurrency = 15
		}, 5000, 100)
		defer stopWorker()

		items := make([]map[string]any, 1000)
		for i := 0; i < 1000; i++ {
			ch := []string{"sms", "email", "push"}[i%3]
			var recip string
			switch ch {
			case "sms":
				recip = fmt.Sprintf("+90555%07d", 1000000+i)
			case "email":
				recip = fmt.Sprintf("batch-%d@example.com", i)
			default:
				recip = fmt.Sprintf("push-token-%d", i)
			}
			items[i] = map[string]any{
				"recipient": recip,
				"channel":   ch,
				"priority":  "normal",
				"content":   fmt.Sprintf("Batch item %d", i),
			}
		}

		var created, replayed api.BatchCreateResponse
		code1, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", map[string]string{
			"Idempotency-Key": "batch-1000-idem",
		}, map[string]any{"notifications": items}, &created)
		require.Equal(t, http.StatusAccepted, code1)
		assert.Equal(t, 1000, created.Count)

		code2, hdr2 := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", map[string]string{
			"Idempotency-Key": "batch-1000-idem",
		}, map[string]any{"notifications": items}, &replayed)
		require.Equal(t, http.StatusAccepted, code2)
		assert.Equal(t, "true", hdr2.Get("Idempotent-Replayed"))
		assert.Equal(t, created.BatchID, replayed.BatchID)

		var summary api.BatchDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/batches/"+created.BatchID.String()+"?limit=1", nil, nil, &summary)
			return c == http.StatusOK && summary.CountsByStatus["sent"] == 1000
		}, 25*time.Second, 100*time.Millisecond)
		assert.Equal(t, 1000, summary.Total)
	})

	t.Run("d_cancel_race_no_cancelled_notification_reaches_provider", func(t *testing.T) {
		inf.resetState(t)
		inf.provider.setDelay(12 * time.Millisecond)
		stopWorker := inf.startWorker(t, func(c *config.Config) {
			c.WorkerConcurrency = 3
		}, 200, 5)
		defer stopWorker()

		items := make([]map[string]any, 200)
		for i := 0; i < 200; i++ {
			items[i] = map[string]any{
				"recipient": fmt.Sprintf("+90555%07d", 2000000+i),
				"channel":   "sms",
				"priority":  "normal",
				"content":   fmt.Sprintf("Race item %d", i),
			}
		}
		var batch api.BatchCreateResponse
		code, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil, map[string]any{
			"notifications": items,
		}, &batch)
		require.Equal(t, http.StatusAccepted, code)

		var (
			mu           sync.Mutex
			cancelledIDs []uuid.UUID
			wg           sync.WaitGroup
		)
		for i := 0; i < 100; i++ {
			id := batch.NotificationIDs[i]
			wg.Add(1)
			go func(target uuid.UUID) {
				defer wg.Done()
				c, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/"+target.String()+"/cancel", nil, nil, nil)
				if c == http.StatusOK {
					mu.Lock()
					cancelledIDs = append(cancelledIDs, target)
					mu.Unlock()
				}
			}(id)
		}
		wg.Wait()

		var summary api.BatchDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/batches/"+batch.BatchID.String()+"?limit=1", nil, nil, &summary)
			if c != http.StatusOK {
				return false
			}
			return summary.CountsByStatus["sent"]+summary.CountsByStatus["cancelled"] == 200
		}, 20*time.Second, 50*time.Millisecond)

		for _, cid := range cancelledIDs {
			assert.Empty(t, inf.provider.callsForID(cid.String()), "cancelled notification %s must never reach provider", cid)
		}
	})

	t.Run("e_retry_500_500_202_and_permanent_400_to_dlq", func(t *testing.T) {
		inf.resetState(t)
		inf.provider.setSequence("+905553000001", []int{500, 500, 202})
		inf.provider.setSequence("+905553000002", []int{400})

		stopWorker := inf.startWorker(t, nil, 500, 10)
		defer stopWorker()

		var n1, n2 api.NotificationResponse
		code1, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil, map[string]any{
			"recipient": "+905553000001",
			"channel":   "sms",
			"content":   "Retry sequence",
		}, &n1)
		require.Equal(t, http.StatusAccepted, code1)

		code2, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil, map[string]any{
			"recipient": "+905553000002",
			"channel":   "sms",
			"content":   "Permanent 400",
		}, &n2)
		require.Equal(t, http.StatusAccepted, code2)

		var d1, d2 api.NotificationDetailResponse
		require.Eventually(t, func() bool {
			_, _ = doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+n1.ID.String(), nil, nil, &d1)
			_, _ = doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+n2.ID.String(), nil, nil, &d2)
			return d1.Status == "sent" && d2.Status == "failed"
		}, 10*time.Second, 30*time.Millisecond)

		assert.Equal(t, 3, d1.Attempts)
		assert.Len(t, d1.DeliveryAttempts, 3)

		assert.Equal(t, 1, d2.Attempts)
		assert.Len(t, d2.DeliveryAttempts, 1)
		dlqItems, err := inf.rdb.LRange(t.Context(), queue.DLQListKey, 0, -1).Result()
		require.NoError(t, err)
		assert.Contains(t, dlqItems, n2.ID.String())
	})

	t.Run("f_priority_high_completes_ahead_of_low", func(t *testing.T) {
		inf.resetState(t)

		lowItems := make([]map[string]any, 300)
		for i := 0; i < 300; i++ {
			lowItems[i] = map[string]any{
				"recipient": fmt.Sprintf("+90555%07d", 4000000+i),
				"channel":   "sms",
				"priority":  "low",
				"content":   fmt.Sprintf("Low %d", i),
			}
		}
		highItems := make([]map[string]any, 30)
		for i := 0; i < 30; i++ {
			highItems[i] = map[string]any{
				"recipient": fmt.Sprintf("+90555%07d", 4500000+i),
				"channel":   "sms",
				"priority":  "high",
				"content":   fmt.Sprintf("High %d", i),
			}
		}

		var lowBatch, highBatch api.BatchCreateResponse
		c1, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil, map[string]any{"notifications": lowItems}, &lowBatch)
		require.Equal(t, http.StatusAccepted, c1)
		c2, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil, map[string]any{"notifications": highItems}, &highBatch)
		require.Equal(t, http.StatusAccepted, c2)

		highSet := make(map[string]bool, len(highBatch.NotificationIDs))
		for _, id := range highBatch.NotificationIDs {
			highSet[id.String()] = true
		}

		stopWorker := inf.startWorker(t, func(c *config.Config) {
			c.WorkerConcurrency = 1
		}, 5000, 100)
		defer stopWorker()

		require.Eventually(t, func() bool {
			return len(inf.provider.allCalls()) == 330
		}, 15*time.Second, 50*time.Millisecond)

		calls := inf.provider.allCalls()
		var highPosSum, lowPosSum float64
		var highCount, lowCount int
		for idx, call := range calls {
			if highSet[call.IdempotencyID] {
				highPosSum += float64(idx)
				highCount++
			} else {
				lowPosSum += float64(idx)
				lowCount++
			}
		}
		require.Equal(t, 30, highCount)
		require.Equal(t, 300, lowCount)
		meanHigh := highPosSum / float64(highCount)
		meanLow := lowPosSum / float64(lowCount)
		assert.Less(t, meanHigh, meanLow, "expected meanHigh (%.1f) < meanLow (%.1f)", meanHigh, meanLow)
	})

	t.Run("g_distributed_rate_limit_500_sms_across_two_workers", func(t *testing.T) {
		inf.resetState(t)

		items := make([]map[string]any, 500)
		for i := 0; i < 500; i++ {
			items[i] = map[string]any{
				"recipient": fmt.Sprintf("+90555%07d", 5000000+i),
				"channel":   "sms",
				"priority":  "normal",
				"content":   fmt.Sprintf("Rate limit item %d", i),
			}
		}
		var batch api.BatchCreateResponse
		code, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil, map[string]any{"notifications": items}, &batch)
		require.Equal(t, http.StatusAccepted, code)

		stopW1 := inf.startWorker(t, func(c *config.Config) {
			c.WorkerID = "worker-int-1"
			c.WorkerConcurrency = 10
		}, 100, 1)
		defer stopW1()

		stopW2 := inf.startWorker(t, func(c *config.Config) {
			c.WorkerID = "worker-int-2"
			c.WorkerConcurrency = 10
		}, 100, 1)
		defer stopW2()

		require.Eventually(t, func() bool {
			return len(inf.provider.allCalls()) == 500
		}, 20*time.Second, 50*time.Millisecond)

		calls := inf.provider.allCalls()
		first := calls[0].ReceivedAt
		last := calls[len(calls)-1].ReceivedAt
		totalSpan := last.Sub(first)
		assert.GreaterOrEqual(t, totalSpan, 4850*time.Millisecond, "500 messages at 100/s Burst=1 must take >= ~4.9s, took %s", totalSpan)

		// Verify no 1-second sliding window [t_i, t_i + 1s) contains more than 101 requests.
		maxInWindow := 0
		right := 0
		for left := 0; left < len(calls); left++ {
			windowEnd := calls[left].ReceivedAt.Add(time.Second)
			for right < len(calls) && calls[right].ReceivedAt.Before(windowEnd) {
				right++
			}
			if count := right - left; count > maxInWindow {
				maxInWindow = count
			}
		}
		assert.LessOrEqual(t, maxInWindow, 101, "sliding 1s window exceeded 101 requests: %d", maxInWindow)
	})

	t.Run("h_reconciler_repairs_untracked_pending_and_simulated_redis_loss", func(t *testing.T) {
		inf.resetState(t)

		// 1. Insert a pending row and a due-soon scheduled row directly into Postgres without Redis enqueue/schedule.
		directID := uuid.Must(uuid.NewV7())
		_, err := inf.repo.InsertNotification(t.Context(), domain.Notification{
			ID:          directID,
			Channel:     domain.ChannelSMS,
			Priority:    domain.PriorityHigh,
			Status:      domain.StatusPending,
			Recipient:   "+905556000001",
			Content:     "Direct DB row without enqueue",
			MaxAttempts: 5,
		})
		require.NoError(t, err)

		schedLostID := uuid.Must(uuid.NewV7())
		schedDueAt := time.Now().UTC().Add(350 * time.Millisecond)
		_, err = inf.repo.InsertNotification(t.Context(), domain.Notification{
			ID:          schedLostID,
			Channel:     domain.ChannelSMS,
			Priority:    domain.PriorityHigh,
			Status:      domain.StatusScheduled,
			ScheduledAt: &schedDueAt,
			Recipient:   "+905556000002",
			Content:     "Scheduled row lost from Redis",
			MaxAttempts: 5,
		})
		require.NoError(t, err)

		// Configure first 5 batch items to hit transient 500 first (entering status='retrying'),
		// and add a brief provider delay so other items remain pending when Redis keys are wiped mid-run.
		for i := 0; i < 5; i++ {
			inf.provider.setSequence(fmt.Sprintf("+90555%07d", 6100000+i), []int{500, 202})
		}
		inf.provider.setDelay(20 * time.Millisecond)

		stopWorker := inf.startWorker(t, func(c *config.Config) {
			c.WorkerConcurrency = 2
			c.RetryMin = 400 * time.Millisecond
			c.RetryBase = 450 * time.Millisecond
			c.RetryCap = 600 * time.Millisecond
			c.PendingStaleAfter = 150 * time.Millisecond
			c.ReconcileInterval = 50 * time.Millisecond
		}, 500, 10)
		defer stopWorker()

		// 2. Submit a 20-item batch while the worker is actively running.
		items := make([]map[string]any, 20)
		for i := 0; i < 20; i++ {
			items[i] = map[string]any{
				"recipient": fmt.Sprintf("+90555%07d", 6100000+i),
				"channel":   "sms",
				"priority":  "normal",
				"content":   fmt.Sprintf("Lost from redis mid-run %d", i),
			}
		}
		var batch api.BatchCreateResponse
		code, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil, map[string]any{"notifications": items}, &batch)
		require.Equal(t, http.StatusAccepted, code)

		// Wait until at least 3 notifications have transitioned to status='retrying' mid-run.
		require.Eventually(t, func() bool {
			var retryingCount int
			_ = inf.pool.QueryRow(t.Context(),
				"SELECT count(*) FROM notifications WHERE batch_id = $1 AND status = 'retrying'",
				batch.BatchID,
			).Scan(&retryingCount)
			return retryingCount >= 3
		}, 5*time.Second, 15*time.Millisecond)

		// Simulate mid-run Redis data loss by deleting {notif}:msg, {notif}:delayed, and SMS priority queues
		// while the worker is running. Reconciler step (a) must recover pending rows and step (b) must recover retrying rows.
		require.NoError(t, inf.rdb.Del(
			t.Context(),
			queue.MsgHashKey,
			queue.DelayedZSetKey,
			queue.Key(domain.ChannelSMS, domain.PriorityHigh),
			queue.Key(domain.ChannelSMS, domain.PriorityNormal),
			queue.Key(domain.ChannelSMS, domain.PriorityLow),
		).Err())

		var detail api.NotificationDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+directID.String(), nil, nil, &detail)
			return c == http.StatusOK && detail.Status == "sent"
		}, 10*time.Second, 30*time.Millisecond)

		var schedDetail api.NotificationDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+schedLostID.String(), nil, nil, &schedDetail)
			return c == http.StatusOK && schedDetail.Status == "sent"
		}, 10*time.Second, 30*time.Millisecond)

		var summary api.BatchDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/batches/"+batch.BatchID.String()+"?limit=1", nil, nil, &summary)
			return c == http.StatusOK && summary.CountsByStatus["sent"] == 20
		}, 12*time.Second, 40*time.Millisecond)
	})

	t.Run("i_list_filters_and_three_page_keyset_pagination", func(t *testing.T) {
		inf.resetState(t)

		tBefore := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
		for i := 0; i < 5; i++ {
			var created api.NotificationResponse
			c, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil, map[string]any{
				"recipient": fmt.Sprintf("+90555%07d", 7000000+i),
				"channel":   "sms",
				"priority":  "high",
				"content":   fmt.Sprintf("Page item %d", i),
			}, &created)
			require.Equal(t, http.StatusAccepted, c)
		}
		// Add 2 email items that must be excluded by channel=sms filter.
		for i := 0; i < 2; i++ {
			c, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil, map[string]any{
				"recipient": fmt.Sprintf("filter-%d@example.com", i),
				"channel":   "email",
				"priority":  "low",
				"content":   "Email item",
			}, nil)
			require.Equal(t, http.StatusAccepted, c)
		}
		tAfter := time.Now().UTC().Add(time.Minute).Format(time.RFC3339)

		seen := make(map[uuid.UUID]bool)
		var cursor string
		pageSizes := []int{2, 2, 1}
		for pageIdx, expectedLen := range pageSizes {
			url := fmt.Sprintf("%s/api/v1/notifications?channel=sms&status=pending&priority=high&from=%s&to=%s&limit=2",
				inf.apiSrv.URL, tBefore, tAfter)
			if cursor != "" {
				url += "&cursor=" + cursor
			}
			var page api.NotificationListResponse
			c, _ := doJSON(t, http.MethodGet, url, nil, nil, &page)
			require.Equal(t, http.StatusOK, c, "page=%d", pageIdx)
			require.Len(t, page.Data, expectedLen, "page=%d", pageIdx)
			for _, item := range page.Data {
				assert.False(t, seen[item.ID], "duplicate item %s across pages", item.ID)
				seen[item.ID] = true
				assert.Equal(t, "sms", item.Channel)
				assert.Equal(t, "high", item.Priority)
			}
			if pageIdx < len(pageSizes)-1 {
				require.NotNil(t, page.NextCursor)
				cursor = *page.NextCursor
			} else {
				assert.Nil(t, page.NextCursor)
			}
		}
		assert.Len(t, seen, 5)
	})

	t.Run("j_scheduled_due_in_2s_sent_after_due_and_cancelled_never_sent", func(t *testing.T) {
		inf.resetState(t)
		stopWorker := inf.startWorker(t, nil, 500, 10)
		defer stopWorker()

		dueAt := time.Now().UTC().Add(2 * time.Second).Truncate(time.Millisecond)
		var scheduled, toCancel api.NotificationResponse
		code1, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil, map[string]any{
			"recipient":   "+905558000001",
			"channel":     "sms",
			"priority":    "high",
			"content":     "Scheduled in 2s",
			"scheduledAt": dueAt.Format(time.RFC3339Nano),
		}, &scheduled)
		require.Equal(t, http.StatusAccepted, code1)
		assert.Equal(t, "scheduled", scheduled.Status)
		require.NotNil(t, scheduled.ScheduledAt)

		code2, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications", nil, map[string]any{
			"recipient":   "+905558000002",
			"channel":     "sms",
			"priority":    "high",
			"content":     "Scheduled then cancelled",
			"scheduledAt": dueAt.Format(time.RFC3339Nano),
		}, &toCancel)
		require.Equal(t, http.StatusAccepted, code2)
		assert.Equal(t, "scheduled", toCancel.Status)

		// Cancel the second scheduled notification before due time.
		var cancelledResp api.NotificationResponse
		cancelCode, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/"+toCancel.ID.String()+"/cancel", nil, nil, &cancelledResp)
		require.Equal(t, http.StatusOK, cancelCode)
		assert.Equal(t, "cancelled", cancelledResp.Status)

		// Verify 800ms after creation (well before the 2s due time) that neither notification has been sent.
		time.Sleep(800 * time.Millisecond)
		var earlyCheck api.NotificationDetailResponse
		cEarly, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+scheduled.ID.String(), nil, nil, &earlyCheck)
		require.Equal(t, http.StatusOK, cEarly)
		assert.Equal(t, "scheduled", earlyCheck.Status, "notification must remain scheduled before due time")
		assert.Empty(t, inf.provider.callsForID(scheduled.ID.String()), "provider must not be called before due time")

		// Wait until the first scheduled notification becomes sent after dueAt.
		var finalDetail api.NotificationDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/notifications/"+scheduled.ID.String(), nil, nil, &finalDetail)
			return c == http.StatusOK && finalDetail.Status == "sent"
		}, 6*time.Second, 40*time.Millisecond)

		calls := inf.provider.callsForID(scheduled.ID.String())
		require.Len(t, calls, 1)
		// Allow at most 100ms tolerance for sub-millisecond timer jitter, proving it was never sent early.
		assert.False(t, calls[0].ReceivedAt.Before(dueAt.Add(-100*time.Millisecond)),
			"provider call at %s occurred before dueAt %s", calls[0].ReceivedAt, dueAt)

		// Confirm the cancelled scheduled notification was never sent to the provider.
		assert.Empty(t, inf.provider.callsForID(toCancel.ID.String()), "cancelled scheduled notification must never reach provider")
	})

	t.Run("k_template_create_preview_and_batch_personalized_sms_delivery", func(t *testing.T) {
		inf.resetState(t)
		stopWorker := inf.startWorker(t, nil, 500, 10)
		defer stopWorker()

		// 1. Create SMS template with Turkish body.
		var tmpl api.TemplateResponse
		cTmpl, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/templates", nil, map[string]any{
			"name":      "otp_sms_tr",
			"channel":   "sms",
			"body":      "Merhaba {{name}}, doğrulama kodunuz: {{code}}",
			"variables": []string{"name", "code"},
		}, &tmpl)
		require.Equal(t, http.StatusCreated, cTmpl)

		// 2. Preview template -> UCS-2 encoding, 1 segment, unusedVariables reported.
		var preview api.TemplatePreviewResponse
		cPrev, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/templates/"+tmpl.ID.String()+"/preview", nil, map[string]any{
			"variables": map[string]any{
				"name":        "Ayşe",
				"code":        "4821",
				"unusedField": true,
			},
		}, &preview)
		require.Equal(t, http.StatusOK, cPrev)
		assert.Equal(t, "Merhaba Ayşe, doğrulama kodunuz: 4821", preview.Content)
		require.NotNil(t, preview.SMS)
		assert.Equal(t, "UCS-2", preview.SMS.Encoding)
		assert.Equal(t, 1, preview.SMS.Segments)
		assert.Equal(t, []string{"unusedField"}, preview.UnusedVariables)

		// 3. Submit a batch of 3 personalized SMS notifications using templateId + templateVariables.
		var batch api.BatchCreateResponse
		cBatch, _ := doJSON(t, http.MethodPost, inf.apiSrv.URL+"/api/v1/notifications/batch", nil, map[string]any{
			"notifications": []map[string]any{
				{
					"recipient":         "+905551111111",
					"channel":           "sms",
					"templateId":        tmpl.ID.String(),
					"templateVariables": map[string]any{"name": "Ayşe", "code": "4821"},
				},
				{
					"recipient":         "+905552222222",
					"channel":           "sms",
					"templateId":        tmpl.ID.String(),
					"templateVariables": map[string]any{"name": "Mehmet", "code": "9035"},
				},
				{
					"recipient":         "+905553333333",
					"channel":           "sms",
					"templateId":        tmpl.ID.String(),
					"templateVariables": map[string]any{"name": "Zeynep", "code": 7142},
				},
			},
		}, &batch)
		require.Equal(t, http.StatusAccepted, cBatch)
		require.Len(t, batch.NotificationIDs, 3)

		var summary api.BatchDetailResponse
		require.Eventually(t, func() bool {
			c, _ := doJSON(t, http.MethodGet, inf.apiSrv.URL+"/api/v1/batches/"+batch.BatchID.String()+"?limit=10", nil, nil, &summary)
			return c == http.StatusOK && summary.CountsByStatus["sent"] == 3
		}, 10*time.Second, 30*time.Millisecond)

		expectedBodies := map[string]string{
			batch.NotificationIDs[0].String(): "Merhaba Ayşe, doğrulama kodunuz: 4821",
			batch.NotificationIDs[1].String(): "Merhaba Mehmet, doğrulama kodunuz: 9035",
			batch.NotificationIDs[2].String(): "Merhaba Zeynep, doğrulama kodunuz: 7142",
		}
		for idStr, wantBody := range expectedBodies {
			calls := inf.provider.callsForID(idStr)
			require.Len(t, calls, 1)
			assert.Equal(t, wantBody, calls[0].Body["content"])
		}
	})
}
