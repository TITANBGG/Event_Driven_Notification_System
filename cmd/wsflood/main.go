// Package main implements cmd/wsflood, a live WebSocket flood, slow-consumer isolation,
// and API boundary-value stress verifier against a running Notification System API.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

type floodConfig struct {
	baseURL        string
	maxClients     int
	fastClients    int
	slowClients    int
	floodBatches   int
	batchSize      int
	dialWorkers    int
	drainTimeout   time.Duration
	skipBoundaries bool
	skipWS         bool
}

type apiMetricsSample struct {
	Goroutines float64
	OpenFDs    float64
	HeapBytes  float64
}

type wsEventMsg struct {
	Type           string `json:"type"`
	NotificationID string `json:"notificationId"`
	Status         string `json:"status"`
	Batch          *struct {
		BatchID        string           `json:"batchId"`
		Total          int              `json:"total"`
		CountsByStatus map[string]int64 `json:"countsByStatus"`
	} `json:"batch,omitempty"`
}

type boundaryCase struct {
	name          string
	method        string
	path          string
	body          []byte
	headers       map[string]string
	wantStatus    int
	wantCode      string
	cancelCreated bool
}

func main() {
	cfg := floodConfig{}
	flag.StringVar(&cfg.baseURL, "url", "http://localhost:8080", "API base URL")
	flag.IntVar(&cfg.maxClients, "max-clients", 1000, "Expected MAX_WS_CLIENTS saturation limit")
	flag.IntVar(&cfg.fastClients, "fast-clients", 400, "Fast reading WS clients during slow-consumer flood")
	flag.IntVar(&cfg.slowClients, "slow-clients", 50, "Non-reading slow WS clients during flood")
	flag.IntVar(&cfg.floodBatches, "flood-batches", 4, "Number of 1000-item SMS batches in slow-consumer flood")
	flag.IntVar(&cfg.batchSize, "batch-size", 1000, "Notifications per flood batch")
	flag.IntVar(&cfg.dialWorkers, "dial-workers", 50, "Concurrent WebSocket dial goroutines")
	flag.DurationVar(&cfg.drainTimeout, "drain-timeout", 180*time.Second, "Timeout waiting for flood batches to complete")
	flag.BoolVar(&cfg.skipBoundaries, "skip-boundaries", false, "Skip HTTP/SMS/Email/Push/WS boundary checks")
	flag.BoolVar(&cfg.skipWS, "skip-ws", false, "Skip WebSocket flood scenarios")
	holdFor := flag.Duration("hold", 0, "Instead of the suite: keep -hold-clients readers subscribed this long (soak load)")
	holdClients := flag.Int("hold-clients", 50, "Readers for -hold, spread over sms/email/push")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *holdFor > 0 {
		if err := runWSHold(ctx, cfg.baseURL, *holdClients, cfg.dialWorkers, *holdFor, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "wsflood -hold failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := runSuite(ctx, cfg, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "wsflood failed: %v\n", err)
		os.Exit(1)
	}
}

func runSuite(ctx context.Context, cfg floodConfig, out io.Writer) error {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          200,
			MaxIdleConnsPerHost:   200,
			IdleConnTimeout:       60 * time.Second,
			ExpectContinueTimeout: 2 * time.Second,
		},
	}

	if err := waitReady(ctx, httpClient, cfg.baseURL); err != nil {
		return err
	}

	var failures int
	if !cfg.skipBoundaries {
		bFail, err := runBoundarySuite(ctx, httpClient, cfg.baseURL, out)
		if err != nil {
			return err
		}
		failures += bFail
	}

	if !cfg.skipWS {
		if err := runWSCapacityAndReuse(ctx, httpClient, cfg, out); err != nil {
			_, _ = fmt.Fprintf(out, "FAIL [WS Capacity & Reuse]: %v\n", err)
			failures++
		}
		if err := runWSSlowConsumerIsolation(ctx, httpClient, cfg, out); err != nil {
			_, _ = fmt.Fprintf(out, "FAIL [WS Slow-Consumer Isolation]: %v\n", err)
			failures++
		}
		if err := runWSBatchSnapshotAndLiveStream(ctx, httpClient, cfg.baseURL, out); err != nil {
			_, _ = fmt.Fprintf(out, "FAIL [WS Batch Snapshot & Live Stream]: %v\n", err)
			failures++
		}
	}

	if failures > 0 {
		return fmt.Errorf("%d suite check(s) failed", failures)
	}
	_, _ = fmt.Fprintln(out, "ALL STEP 7 BOUNDARY & WEBSOCKET FLOOD CHECKS PASSED")
	return nil
}

func waitReady(ctx context.Context, hc *http.Client, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/health/ready", nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("health/ready failed: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health/ready returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func buildBoundaryCases(now time.Time) []boundaryCase {
	base := strings.TrimRight("TAG", "")
	_ = base

	validSMS := func(content, suffix string) []byte {
		body, _ := json.Marshal(map[string]any{
			"recipient": "+90555" + suffix,
			"channel":   "sms",
			"content":   content,
		})
		return body
	}

	batchBody := func(count int) []byte {
		var buf bytes.Buffer
		buf.WriteString(`{"notifications":[`)
		for i := 0; i < count; i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			fmt.Fprintf(&buf, `{"recipient":"+90555%07d","channel":"sms","content":"boundary %d"}`, 7000000+(i%1000000), i)
		}
		buf.WriteString(`]}`)
		return buf.Bytes()
	}

	// Single request body limit = 524288 bytes (512 KB). Keep email content inside 102400B while
	// pushing total JSON size above 524288B using whitespace padding so BodyLimit returns 413.
	over512KB := append(validSMS("ok", "1000001"), bytes.Repeat([]byte{' '}, 524288)...)
	over16MB := bytes.Repeat([]byte{' '}, 16*1024*1024+64)
	over16MB[0] = '{'
	over16MB[len(over16MB)-1] = '}'

	emailBody := func(subj, content string) []byte {
		b, _ := json.Marshal(map[string]any{
			"recipient": "boundary@example.com",
			"channel":   "email",
			"subject":   subj,
			"content":   content,
		})
		return b
	}

	pushBody := func(token, title, content string) []byte {
		b, _ := json.Marshal(map[string]any{
			"recipient": token,
			"channel":   "push",
			"title":     title,
			"content":   content,
		})
		return b
	}

	// Push payload limit check measures {"to":...,"channel":"push","content":...}.
	// With 3800-char token + 2000-char content -> exceeds 4096 serialized bytes while both fields stay below 4096/2000 runes.
	pushPayloadOver4096 := pushBody(strings.Repeat("t", 3000), "ok", strings.Repeat("c", 1200))

	schedBody := func(ts time.Time) []byte {
		b, _ := json.Marshal(map[string]any{
			"recipient":   "+905551000099",
			"channel":     "sms",
			"content":     "scheduled boundary",
			"scheduledAt": ts.UTC().Format(time.RFC3339),
		})
		return b
	}

	singleWithBodyIdemKey := []byte(`{"recipient":"+905551000088","channel":"sms","content":"hi","idempotencyKey":"bad"}`)
	invalidUTF8Body := []byte("{\"recipient\":\"+905551000077\",\"channel\":\"sms\",\"content\":\"\xff\xfe\"}")

	tag := fmt.Sprintf("b%d", now.UnixNano()%1000000)

	return []boundaryCase{
		{name: "batch 0 items -> 422", method: http.MethodPost, path: "/api/v1/notifications/batch", body: batchBody(0), wantStatus: http.StatusUnprocessableEntity},
		{name: "batch 1000 items -> 202", method: http.MethodPost, path: "/api/v1/notifications/batch", body: batchBody(1000), wantStatus: http.StatusAccepted},
		{name: "batch 1001 items -> 422 batch_too_large", method: http.MethodPost, path: "/api/v1/notifications/batch", body: batchBody(1001), wantStatus: http.StatusUnprocessableEntity, wantCode: "batch_too_large"},

		{name: "single body > 512KB -> 413 payload_too_large", method: http.MethodPost, path: "/api/v1/notifications", body: over512KB, wantStatus: http.StatusRequestEntityTooLarge, wantCode: "payload_too_large"},
		{name: "batch body > 16MB -> 413 payload_too_large", method: http.MethodPost, path: "/api/v1/notifications/batch", body: over16MB, headers: map[string]string{"Expect": "100-continue"}, wantStatus: http.StatusRequestEntityTooLarge, wantCode: "payload_too_large"},

		{name: "SMS GSM-7 exact 1530 septets (10 seg) -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("a", 1530), "1000002"), wantStatus: http.StatusAccepted},
		{name: "SMS GSM-7 1531 septets (11 seg) -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("a", 1531), "1000003"), wantStatus: http.StatusUnprocessableEntity},
		// 153 is odd and a 2-septet GSM-7 ext char (€) cannot split across segments -> max 76/segment (760 total = 1520 septets).
		{name: "SMS GSM-7 ext 760x€ (max 10 seg, no ESC split) -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("€", 760), "1000004"), wantStatus: http.StatusAccepted},
		{name: "SMS GSM-7 ext 761x€ (11 seg, no ESC split) -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("€", 761), "1000005"), wantStatus: http.StatusUnprocessableEntity},
		{name: "SMS UCS-2 Turkish 670 chars (10 seg) -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("ş", 670), "1000006"), wantStatus: http.StatusAccepted},
		{name: "SMS UCS-2 Turkish 671 chars (11 seg) -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("ş", 671), "1000007"), wantStatus: http.StatusUnprocessableEntity},
		// 67 is odd and a 2-unit UTF-16 surrogate pair (🚀) cannot split -> max 33/segment (330 total = 660 UTF-16 units).
		{name: "SMS Emoji 330x🚀 (max 10 seg, no surrogate split) -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("🚀", 330), "1000008"), wantStatus: http.StatusAccepted},
		{name: "SMS Emoji 331x🚀 (11 seg, no surrogate split) -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS(strings.Repeat("🚀", 331), "1000009"), wantStatus: http.StatusUnprocessableEntity},

		{name: "Email subject 200 runes & content 102400B -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: emailBody(strings.Repeat("ö", 200), strings.Repeat("x", 102400)), wantStatus: http.StatusAccepted},
		{name: "Email subject 201 runes -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: emailBody(strings.Repeat("ö", 201), "ok"), wantStatus: http.StatusUnprocessableEntity},
		{name: "Email subject CR/LF -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: emailBody("bad\r\nsubj", "ok"), wantStatus: http.StatusUnprocessableEntity},
		{name: "Email content 102401B -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: emailBody("ok", strings.Repeat("x", 102401)), wantStatus: http.StatusUnprocessableEntity},

		{name: "Push title 100 runes & content 2000 runes -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: pushBody("tok-valid", strings.Repeat("t", 100), strings.Repeat("c", 2000)), wantStatus: http.StatusAccepted},
		{name: "Push title 101 runes -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: pushBody("tok-valid", strings.Repeat("t", 101), "ok"), wantStatus: http.StatusUnprocessableEntity},
		{name: "Push content 2001 runes -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: pushBody("tok-valid", "ok", strings.Repeat("c", 2001)), wantStatus: http.StatusUnprocessableEntity},
		{name: "Push token whitespace -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: pushBody("tok bad", "ok", "ok"), wantStatus: http.StatusUnprocessableEntity},
		{name: "Push token 4097 runes -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: pushBody(strings.Repeat("k", 4097), "ok", "ok"), wantStatus: http.StatusUnprocessableEntity},
		{name: "Push JSON payload > 4096B -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: pushPayloadOver4096, wantStatus: http.StatusUnprocessableEntity},

		{name: "Schedule +719h59m -> 202 (and cancelled)", method: http.MethodPost, path: "/api/v1/notifications", body: schedBody(now.Add(719*time.Hour + 59*time.Minute)), wantStatus: http.StatusAccepted, cancelCreated: true},
		{name: "Schedule +720h05m -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: schedBody(now.Add(720*time.Hour + 5*time.Minute)), wantStatus: http.StatusUnprocessableEntity},
		{name: "Schedule -10s (past skew) -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: schedBody(now.Add(-10 * time.Second)), wantStatus: http.StatusUnprocessableEntity},

		{name: "Idempotency-Key 255 ASCII chars -> 202", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS("idem 255", "1000010"), headers: map[string]string{"Idempotency-Key": tag + strings.Repeat("k", 255-len(tag))}, wantStatus: http.StatusAccepted},
		{name: "Idempotency-Key 256 chars -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS("idem 256", "1000011"), headers: map[string]string{"Idempotency-Key": strings.Repeat("k", 256)}, wantStatus: http.StatusUnprocessableEntity},
		{name: "Idempotency-Key non-ASCII -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: validSMS("idem utf8", "1000012"), headers: map[string]string{"Idempotency-Key": "bad-key-ğ"}, wantStatus: http.StatusUnprocessableEntity},
		{name: "Single body idempotencyKey rejected -> 422", method: http.MethodPost, path: "/api/v1/notifications", body: singleWithBodyIdemKey, wantStatus: http.StatusUnprocessableEntity},
		{name: "Invalid UTF-8 body -> 400", method: http.MethodPost, path: "/api/v1/notifications", body: invalidUTF8Body, wantStatus: http.StatusBadRequest},

		{name: "List limit=200 -> 200", method: http.MethodGet, path: "/api/v1/notifications?limit=200", wantStatus: http.StatusOK},
		{name: "List limit=201 -> 422", method: http.MethodGet, path: "/api/v1/notifications?limit=201", wantStatus: http.StatusUnprocessableEntity},
		{name: "List limit=0 -> 422", method: http.MethodGet, path: "/api/v1/notifications?limit=0", wantStatus: http.StatusUnprocessableEntity},
		{name: "List bad cursor -> 400", method: http.MethodGet, path: "/api/v1/notifications?cursor=not-valid-base64", wantStatus: http.StatusBadRequest, wantCode: "invalid_cursor"},

		{name: "WS missing filter -> 400 missing_ws_filter", method: http.MethodGet, path: "/api/v1/ws", wantStatus: http.StatusBadRequest, wantCode: "missing_ws_filter"},
		{name: "WS invalid UUID -> 400 invalid_uuid", method: http.MethodGet, path: "/api/v1/ws?notificationId=bad", wantStatus: http.StatusBadRequest, wantCode: "invalid_uuid"},
		{name: "WS invalid channel -> 400 invalid_channel", method: http.MethodGet, path: "/api/v1/ws?channel=fax", wantStatus: http.StatusBadRequest, wantCode: "invalid_channel"},
	}
}

func runBoundarySuite(ctx context.Context, hc *http.Client, baseURL string, out io.Writer) (int, error) {
	_, _ = fmt.Fprintln(out, "=== Stage 1: API & WebSocket Boundary Matrix ===")
	cases := buildBoundaryCases(time.Now().UTC())
	failures := 0

	for _, tc := range cases {
		code, body, err := doRequest(ctx, hc, tc.method, strings.TrimRight(baseURL, "/")+tc.path, tc.body, tc.headers)
		if err != nil {
			_, _ = fmt.Fprintf(out, "FAIL  %-50s request error: %v\n", tc.name, err)
			failures++
			continue
		}
		if code != tc.wantStatus {
			_, _ = fmt.Fprintf(out, "FAIL  %-50s got HTTP %d want %d body=%s\n", tc.name, code, tc.wantStatus, truncate(string(body), 200))
			failures++
			continue
		}
		if tc.wantCode != "" && !strings.Contains(string(body), tc.wantCode) {
			_, _ = fmt.Fprintf(out, "FAIL  %-50s missing code %q in body=%s\n", tc.name, tc.wantCode, truncate(string(body), 200))
			failures++
			continue
		}
		if tc.cancelCreated && code == http.StatusAccepted {
			var created struct {
				ID string `json:"id"`
			}
			if jErr := json.Unmarshal(body, &created); jErr != nil || created.ID == "" {
				_, _ = fmt.Fprintf(out, "FAIL  %-50s failed parsing created id for cancel cleanup\n", tc.name)
				failures++
				continue
			}
			cancelCode, cancelBody, cErr := doRequest(ctx, hc, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/notifications/"+created.ID+"/cancel", nil, nil)
			if cErr != nil || cancelCode != http.StatusOK {
				_, _ = fmt.Fprintf(out, "FAIL  %-50s cancel cleanup failed HTTP=%d err=%v body=%s\n", tc.name, cancelCode, cErr, truncate(string(cancelBody), 120))
				failures++
				continue
			}
		}
		_, _ = fmt.Fprintf(out, "PASS  %-50s HTTP %d\n", tc.name, code)
	}

	// Disallowed Origin WebSocket handshake check.
	wsURL := toWSURL(baseURL, "/api/v1/ws?channel=sms")
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, resp, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
	})
	cancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		_ = conn.CloseNow()
		_, _ = fmt.Fprintf(out, "FAIL  %-50s expected disallowed Origin rejection, got 101\n", "WS disallowed Origin -> 403")
		failures++
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		_, _ = fmt.Fprintf(out, "FAIL  %-50s got HTTP %d want 403 (%v)\n", "WS disallowed Origin -> 403", status, err)
		failures++
	} else {
		_, _ = fmt.Fprintf(out, "PASS  %-50s HTTP %d\n", "WS disallowed Origin -> 403", resp.StatusCode)
	}

	return failures, nil
}

func runWSCapacityAndReuse(ctx context.Context, hc *http.Client, cfg floodConfig, out io.Writer) error {
	_, _ = fmt.Fprintf(out, "\n=== Stage 2: WS Capacity Saturation (%d clients + overflow + slot reuse) ===\n", cfg.maxClients)
	baseSample, err := fetchAPIMetrics(ctx, hc, cfg.baseURL)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "Baseline API metrics: goroutines=%.0f open_fds=%.0f heap_mb=%.2f\n",
		baseSample.Goroutines, baseSample.OpenFDs, baseSample.HeapBytes/(1024*1024))

	wsURL := toWSURL(cfg.baseURL, "/api/v1/ws?channel=sms")
	conns, err := dialWSBatch(ctx, wsURL, cfg.maxClients, cfg.dialWorkers, 0)
	if err != nil {
		closeAllWS(conns)
		return fmt.Errorf("dialing %d WS clients: %w", cfg.maxClients, err)
	}
	defer closeAllWS(conns)

	// Start background readers for all open connections so Ping/Pong keepalives work cleanly.
	readCtx, cancelReads := context.WithCancel(ctx)
	defer cancelReads()
	var readerWg sync.WaitGroup
	for _, c := range conns {
		conn := c
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				if _, _, rErr := conn.Read(readCtx); rErr != nil {
					return
				}
			}
		}()
	}

	peakSample, err := fetchAPIMetrics(ctx, hc, cfg.baseURL)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "At %d live WS clients: goroutines=%.0f open_fds=%.0f heap_mb=%.2f\n",
		len(conns), peakSample.Goroutines, peakSample.OpenFDs, peakSample.HeapBytes/(1024*1024))

	// Attempt 10 overflow connections -> all must fail with 503 too_many_ws_clients.
	for i := 0; i < 10; i++ {
		dCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		extraConn, resp, dErr := websocket.Dial(dCtx, wsURL, nil)
		cancel()
		var body []byte
		status := 0
		if resp != nil {
			status = resp.StatusCode
			if resp.Body != nil {
				body, _ = io.ReadAll(io.LimitReader(resp.Body, 4096))
				_ = resp.Body.Close()
			}
		}
		if dErr == nil {
			_ = extraConn.CloseNow()
			return fmt.Errorf("overflow client #%d was unexpectedly accepted above MAX_WS_CLIENTS=%d", i+1, cfg.maxClients)
		}
		if status != http.StatusServiceUnavailable || !strings.Contains(string(body), "too_many_ws_clients") {
			return fmt.Errorf("overflow client #%d got status=%d body=%s, want 503 too_many_ws_clients", i+1, status, string(body))
		}
	}
	_, _ = fmt.Fprintf(out, "PASS  10 overflow WS clients rejected with 503 too_many_ws_clients\n")

	// Close 200 clients and verify 200 new clients can take those freed slots.
	reuseCount := min(200, len(conns))
	for i := 0; i < reuseCount; i++ {
		_ = conns[i].Close(websocket.StatusNormalClosure, "slot reuse test")
		conns[i] = nil
	}

	var replacement []*websocket.Conn
	deadline := time.Now().Add(5 * time.Second)
	for len(replacement) < reuseCount && time.Now().Before(deadline) {
		need := reuseCount - len(replacement)
		batch, bErr := dialWSBatch(ctx, wsURL, need, cfg.dialWorkers, 0)
		replacement = append(replacement, batch...)
		if bErr != nil {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if len(replacement) != reuseCount {
		closeAllWS(replacement)
		return fmt.Errorf("slot reuse: opened %d/%d replacement clients after closing %d", len(replacement), reuseCount, reuseCount)
	}
	_, _ = fmt.Fprintf(out, "PASS  Slot reuse: closed %d clients -> reopened %d/%d replacement clients cleanly\n", reuseCount, len(replacement), reuseCount)

	closeAllWS(replacement)
	cancelReads()
	closeAllWS(conns)
	readerWg.Wait()

	// Brief settle window before checking post-close goroutine & FD recovery.
	time.Sleep(2 * time.Second)
	afterSample, err := fetchAPIMetrics(ctx, hc, cfg.baseURL)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "After closing all WS clients: goroutines=%.0f open_fds=%.0f heap_mb=%.2f\n",
		afterSample.Goroutines, afterSample.OpenFDs, afterSample.HeapBytes/(1024*1024))
	if afterSample.Goroutines > baseSample.Goroutines+30 {
		return fmt.Errorf("possible API goroutine leak after WS close: baseline=%.0f after=%.0f", baseSample.Goroutines, afterSample.Goroutines)
	}
	if afterSample.OpenFDs > baseSample.OpenFDs+20 {
		return fmt.Errorf("possible API FD leak after WS close: baseline=%.0f after=%.0f", baseSample.OpenFDs, afterSample.OpenFDs)
	}
	return nil
}

func runWSSlowConsumerIsolation(ctx context.Context, hc *http.Client, cfg floodConfig, out io.Writer) error {
	totalEvents := cfg.floodBatches * cfg.batchSize
	_, _ = fmt.Fprintf(out, "\n=== Stage 3: Slow-Consumer Isolation (%d fast + %d stalled slow clients, %d SMS events) ===\n",
		cfg.fastClients, cfg.slowClients, totalEvents)

	wsURL := toWSURL(cfg.baseURL, "/api/v1/ws?channel=sms")
	fastConns, err := dialWSBatch(ctx, wsURL, cfg.fastClients, cfg.dialWorkers, 0)
	if err != nil {
		closeAllWS(fastConns)
		return fmt.Errorf("dialing fast clients: %w", err)
	}
	defer closeAllWS(fastConns)

	// Shrink slow clients' TCP receive buffer (1024 B) and never call Read(), forcing server-side
	// write backpressure -> the Hub queue grows past its soft limit with no progress -> server drops slow clients without blocking the Hub.
	slowConns, err := dialWSBatch(ctx, wsURL, cfg.slowClients, cfg.dialWorkers, 1024)
	if err != nil {
		closeAllWS(slowConns)
		return fmt.Errorf("dialing slow clients: %w", err)
	}
	defer closeAllWS(slowConns)

	fastSent := make([]atomic.Int64, len(fastConns))
	var fastDrops atomic.Int64
	readCtx, cancelFastReads := context.WithCancel(ctx)
	defer cancelFastReads()
	var fastWg sync.WaitGroup
	for i, c := range fastConns {
		idx, conn := i, c
		fastWg.Add(1)
		go func() {
			defer fastWg.Done()
			for {
				_, raw, rErr := conn.Read(readCtx)
				if rErr != nil {
					if readCtx.Err() == nil {
						fastDrops.Add(1)
					}
					return
				}
				var ev wsEventMsg
				if json.Unmarshal(raw, &ev) == nil && ev.Status == "sent" {
					fastSent[idx].Add(1)
				}
			}
		}()
	}

	// Give Hub Run loop a moment to finish all registrations before sending batches.
	time.Sleep(500 * time.Millisecond)

	batchIDs := make([]string, 0, cfg.floodBatches)
	for b := 0; b < cfg.floodBatches; b++ {
		id, bErr := submitSMSBatch(ctx, hc, cfg.baseURL, cfg.batchSize, fmt.Sprintf("wsflood-%d-%d", time.Now().UnixNano(), b))
		if bErr != nil {
			return fmt.Errorf("submitting flood batch %d: %w", b, bErr)
		}
		batchIDs = append(batchIDs, id)
		// Space 1,000-item batch submissions slightly so Hub clients flush each burst cleanly
		// while stalled slow consumers still hit backpressure and disconnect.
		if b+1 < cfg.floodBatches {
			time.Sleep(500 * time.Millisecond)
		}
	}

	// Verify /health/ready stays responsive while slow consumers are hitting 5s write timeouts and dropping.
	var maxHealthMS int64
	stopProbe := make(chan struct{})
	var probeWg sync.WaitGroup
	probeWg.Add(1)
	go func() {
		defer probeWg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopProbe:
				return
			case <-t.C:
				t0 := time.Now()
				_ = waitReady(ctx, hc, cfg.baseURL)
				if ms := time.Since(t0).Milliseconds(); ms > maxHealthMS {
					atomic.StoreInt64(&maxHealthMS, ms)
				}
			}
		}
	}()

	if err := waitBatchesTerminal(ctx, hc, cfg.baseURL, batchIDs, cfg.drainTimeout); err != nil {
		close(stopProbe)
		probeWg.Wait()
		return err
	}
	close(stopProbe)
	probeWg.Wait()

	// Brief grace period for Redis Pub/Sub -> API Hub -> fast WS clients to finish flushing final sent frames.
	time.Sleep(2 * time.Second)
	cancelFastReads()
	closeAllWS(fastConns)
	fastWg.Wait()

	minFastSent := int64(totalEvents + 1)
	for i := range fastSent {
		if v := fastSent[i].Load(); v < minFastSent {
			minFastSent = v
		}
	}

	// Now drain slow sockets briefly to observe whether the server dropped them and with which close code.
	droppedSlow := 0
	close1008 := 0
	closeOther := 0
	for _, sc := range slowConns {
		probeCtx, cancel := context.WithTimeout(ctx, 7*time.Second)
		dropped := false
		for {
			_, _, rErr := sc.Read(probeCtx)
			if rErr != nil {
				var ce websocket.CloseError
				if errors.As(rErr, &ce) {
					dropped = true
					if ce.Code == websocket.StatusPolicyViolation {
						close1008++
					} else {
						closeOther++
					}
				} else if !errors.Is(rErr, context.DeadlineExceeded) {
					dropped = true
					closeOther++
				}
				break
			}
		}
		cancel()
		if dropped {
			droppedSlow++
		}
	}

	_, _ = fmt.Fprintf(out, "Results: fastClients=%d droppedFast=%d minFastSentEvents=%d/%d | slowClients=%d droppedSlow=%d (1008=%d, otherClose=%d) | maxHealthLatency=%dms\n",
		cfg.fastClients, fastDrops.Load(), minFastSent, totalEvents,
		cfg.slowClients, droppedSlow, close1008, closeOther, atomic.LoadInt64(&maxHealthMS))

	if fastDrops.Load() > 0 {
		return fmt.Errorf("%d fast healthy WS client(s) dropped during flood", fastDrops.Load())
	}
	if minFastSent < int64(totalEvents) {
		return fmt.Errorf("fast clients missed live sent events: min=%d want=%d", minFastSent, totalEvents)
	}
	if droppedSlow < cfg.slowClients {
		return fmt.Errorf("only %d/%d stalled slow clients were disconnected by server", droppedSlow, cfg.slowClients)
	}
	return nil
}

func runWSBatchSnapshotAndLiveStream(ctx context.Context, hc *http.Client, baseURL string, out io.Writer) error {
	const count = 1000
	const watchers = 20
	_, _ = fmt.Fprintf(out, "\n=== Stage 4: Batch Snapshot + Live Stream (%d watchers, %d SMS) ===\n", watchers, count)

	batchID, err := submitSMSBatch(ctx, hc, baseURL, count, fmt.Sprintf("wssnap-%d", time.Now().UnixNano()))
	if err != nil {
		return err
	}
	wsURL := toWSURL(baseURL, "/api/v1/ws?batchId="+url.QueryEscape(batchID))
	conns, err := dialWSBatch(ctx, wsURL, watchers, watchers, 0)
	if err != nil {
		closeAllWS(conns)
		return err
	}
	defer closeAllWS(conns)

	var snapOK atomic.Int64
	var errCount atomic.Int64
	var wg sync.WaitGroup
	sentSets := make([]int, len(conns))

	for i, c := range conns {
		idx, conn := i, c
		wg.Add(1)
		go func() {
			defer wg.Done()
			rCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			seenSent := make(map[string]struct{}, count)
			var snapSent int64
			gotSnap := false
			for !gotSnap || snapSent+int64(len(seenSent)) < count {
				_, raw, rErr := conn.Read(rCtx)
				if rErr != nil {
					errCount.Add(1)
					break
				}
				var msg wsEventMsg
				if json.Unmarshal(raw, &msg) != nil {
					continue
				}
				if msg.Type == "snapshot" && msg.Batch != nil && msg.Batch.BatchID == batchID && msg.Batch.Total == count {
					gotSnap = true
					snapSent = msg.Batch.CountsByStatus["sent"]
					continue
				}
				if msg.Type == "event" && msg.Status == "sent" && msg.NotificationID != "" {
					seenSent[msg.NotificationID] = struct{}{}
				}
			}
			if gotSnap {
				snapOK.Add(1)
			}
			sentSets[idx] = int(snapSent) + len(seenSent)
		}()
	}

	if err := waitBatchesTerminal(ctx, hc, baseURL, []string{batchID}, 60*time.Second); err != nil {
		return err
	}
	wg.Wait()

	minSent := count
	for _, n := range sentSets {
		if n < minSent {
			minSent = n
		}
	}
	_, _ = fmt.Fprintf(out, "PASS  Watchers=%d snapshotsOK=%d/%d minLiveSentEvents=%d/%d errors=%d\n",
		watchers, snapOK.Load(), watchers, minSent, count, errCount.Load())

	if snapOK.Load() != watchers || minSent < count || errCount.Load() > 0 {
		return fmt.Errorf("batch snapshot/stream mismatch: snaps=%d/%d minSent=%d/%d errs=%d",
			snapOK.Load(), watchers, minSent, count, errCount.Load())
	}
	return nil
}

func dialWSBatch(ctx context.Context, wsURL string, n, workers, recvBuf int) ([]*websocket.Conn, error) {
	if n <= 0 {
		return nil, nil
	}
	workers = max(1, min(workers, n))

	dialClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(dCtx context.Context, network, addr string) (net.Conn, error) {
				c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(dCtx, network, addr)
				if err != nil {
					return nil, err
				}
				if recvBuf > 0 {
					if tc, ok := c.(*net.TCPConn); ok {
						_ = tc.SetReadBuffer(recvBuf)
					}
				}
				return c, nil
			},
		},
	}

	conns := make([]*websocket.Conn, n)
	var mu sync.Mutex
	var firstErr error
	nextIdx := atomic.Int64{}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(nextIdx.Add(1) - 1)
				if idx >= n {
					return
				}
				dCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				conn, resp, err := websocket.Dial(dCtx, wsURL, &websocket.DialOptions{HTTPClient: dialClient})
				cancel()
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("client #%d: %w", idx, err)
					}
					mu.Unlock()
					return
				}
				conn.SetReadLimit(1 << 20)
				conns[idx] = conn
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		closeAllWS(conns)
		return nil, firstErr
	}
	return conns, nil
}

func closeAllWS(conns []*websocket.Conn) {
	for i, c := range conns {
		if c != nil {
			_ = c.CloseNow()
			conns[i] = nil
		}
	}
}

func submitSMSBatch(ctx context.Context, hc *http.Client, baseURL string, count int, tag string) (string, error) {
	var buf bytes.Buffer
	buf.WriteString(`{"notifications":[`)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"recipient":"+90555%07d","channel":"sms","priority":"high","content":"%s-%d"}`, 8000000+(i%1000000), tag, i)
	}
	buf.WriteString(`]}`)

	code, body, err := doRequest(ctx, hc, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/notifications/batch", buf.Bytes(), nil)
	if err != nil {
		return "", err
	}
	if code != http.StatusAccepted {
		return "", fmt.Errorf("batch POST returned HTTP %d: %s", code, truncate(string(body), 200))
	}
	var resp struct {
		BatchID string `json:"batchId"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.BatchID == "" {
		return "", fmt.Errorf("invalid batchId response: %s", truncate(string(body), 200))
	}
	return resp.BatchID, nil
}

func waitBatchesTerminal(ctx context.Context, hc *http.Client, baseURL string, batchIDs []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allDone := true
		for _, id := range batchIDs {
			code, body, err := doRequest(ctx, hc, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/batches/"+id+"?limit=1", nil, nil)
			if err != nil || code != http.StatusOK {
				allDone = false
				break
			}
			var b struct {
				Total          int              `json:"total"`
				CountsByStatus map[string]int64 `json:"countsByStatus"`
			}
			if json.Unmarshal(body, &b) != nil {
				allDone = false
				break
			}
			term := b.CountsByStatus["sent"] + b.CountsByStatus["failed"] + b.CountsByStatus["cancelled"]
			if int(term) < b.Total {
				allDone = false
				break
			}
		}
		if allDone {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s waiting for %d batch(es) to become terminal", timeout, len(batchIDs))
}

func fetchAPIMetrics(ctx context.Context, hc *http.Client, baseURL string) (apiMetricsSample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/metrics", nil)
	if err != nil {
		return apiMetricsSample{}, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return apiMetricsSample{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return apiMetricsSample{}, fmt.Errorf("/metrics returned HTTP %d", resp.StatusCode)
	}
	return parsePrometheusSample(resp.Body)
}

func parsePrometheusSample(r io.Reader) (apiMetricsSample, error) {
	var s apiMetricsSample
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "go_goroutines":
			s.Goroutines = val
		case "process_open_fds":
			s.OpenFDs = val
		case "go_memstats_alloc_bytes":
			s.HeapBytes = val
		}
	}
	return s, sc.Err()
}

func doRequest(ctx context.Context, hc *http.Client, method, targetURL string, body []byte, headers map[string]string) (int, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, readErr
}

func toWSURL(baseURL, pathAndQuery string) string {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return "ws://localhost:8080" + pathAndQuery
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	ref, _ := url.Parse(pathAndQuery)
	return u.ResolveReference(ref).String()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
