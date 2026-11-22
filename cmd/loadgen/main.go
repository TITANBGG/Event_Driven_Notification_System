// Package main implements a concurrent batch load generator for benchmarking API throughput
// and tracking delivery completion across worker replicas.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type loadConfig struct {
	baseURL     string
	total       int
	batchSize   int
	concurrency int
	channel     string
	channelMix  string
	priorityMix string
	// channelStats pages through every batch after completion to compute per-channel send rates.
	channelStats bool
	pollTimeout  time.Duration
	pollEvery    time.Duration
	// apiKey is sent as Authorization: Bearer on every request (empty: no header).
	apiKey string
}

type priorityWeights struct {
	high   int
	normal int
	low    int
}

type channelWeights struct {
	sms   int
	email int
	push  int
}

// channelStat is derived from persisted sentAt timestamps, so it measures what the rate limiter
// actually let through rather than what the client submitted.
type channelStat struct {
	Sent      int
	FirstSent time.Time
	LastSent  time.Time
}

// EffectiveRate is sends per second between the first and last send on the channel; N sends span
// N-1 inter-send gaps, so (N-1)/span is the steady-state rate the GCRA limiter enforces.
func (c channelStat) EffectiveRate() float64 {
	span := c.LastSent.Sub(c.FirstSent).Seconds()
	if c.Sent < 2 || span <= 0 {
		return 0
	}
	return float64(c.Sent-1) / span
}

type loadResult struct {
	Submitted      int
	BatchLatencies []float64
	CreateElapsed  time.Duration
	TotalElapsed   time.Duration
	FinalStatuses  map[string]int64
	ChannelStats   map[string]channelStat
}

func main() {
	cfg := loadConfig{}
	flag.StringVar(&cfg.baseURL, "url", "http://localhost:8080", "API base URL")
	flag.IntVar(&cfg.total, "n", 2000, "Total notifications to create")
	flag.IntVar(&cfg.batchSize, "batch", 100, "Notifications per batch request (1..1000)")
	flag.IntVar(&cfg.concurrency, "concurrency", 4, "Concurrent batch submission workers")
	flag.StringVar(&cfg.channel, "channel", "mixed", "Target channel: mixed (uses -channel-mix), sms, email, or push")
	flag.StringVar(&cfg.channelMix, "channel-mix", "34:33:33", "Channel weights sms:email:push summing to 100 (with -channel=mixed)")
	flag.StringVar(&cfg.priorityMix, "priority-mix", "20:60:20", "Priority weights high:normal:low summing to 100")
	flag.BoolVar(&cfg.channelStats, "channel-stats", true, "After completion, page through batch items to report per-channel effective send rate")
	flag.DurationVar(&cfg.pollTimeout, "poll-timeout", 5*time.Minute, "Maximum time to wait for all batches to complete")
	flag.StringVar(&cfg.apiKey, "api-key", envOr("API_KEY", defaultAPIKey), "API key sent as Authorization: Bearer (needs write + read; empty: none)")
	cfg.pollEvery = 500 * time.Millisecond
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	res, err := runLoadgen(ctx, cfg, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen error: %v\n", err)
		os.Exit(1)
	}
	if nonTerminalCount(res.FinalStatuses) > 0 {
		os.Exit(1)
	}
}

// parseWeights parses "a:b:c" percentages that must sum to 100.
func parseWeights(flagName, layout, raw string) ([3]int, error) {
	var vals [3]int
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 3 {
		return vals, fmt.Errorf("invalid %s %q: expected %s", flagName, raw, layout)
	}
	sum := 0
	for i, part := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || v < 0 {
			return vals, fmt.Errorf("invalid %s weight %q", flagName, part)
		}
		vals[i] = v
		sum += v
	}
	if sum != 100 {
		return vals, fmt.Errorf("%s weights must sum to 100, got %d", flagName, sum)
	}
	return vals, nil
}

func parsePriorityMix(raw string) (priorityWeights, error) {
	v, err := parseWeights("priority-mix", "high:normal:low", raw)
	return priorityWeights{high: v[0], normal: v[1], low: v[2]}, err
}

func parseChannelMix(raw string) (channelWeights, error) {
	v, err := parseWeights("channel-mix", "sms:email:push", raw)
	return channelWeights{sms: v[0], email: v[1], push: v[2]}, err
}

// selectChannel deterministically maps seq onto the weighted mix. The slot uses a stride of 7
// (coprime with 100) so every block of 100 still hits the exact percentages while the channel
// sequence is decorrelated from the priority slot (seq % 100).
func selectChannel(mode string, w channelWeights, seq int) (string, error) {
	switch m := strings.ToLower(strings.TrimSpace(mode)); m {
	case "mixed":
		slot := (seq * 7) % 100
		switch {
		case slot < w.sms:
			return "sms", nil
		case slot < w.sms+w.email:
			return "email", nil
		default:
			return "push", nil
		}
	case "sms", "email", "push":
		return m, nil
	default:
		return "", fmt.Errorf("unsupported channel %q: want mixed, sms, email, or push", mode)
	}
}

func selectPriority(w priorityWeights, seq int) string {
	slot := seq % 100
	if slot < w.high {
		return "high"
	}
	if slot < w.high+w.normal {
		return "normal"
	}
	return "low"
}

func validateLoadConfig(cfg loadConfig) (priorityWeights, channelWeights, error) {
	if cfg.total <= 0 {
		return priorityWeights{}, channelWeights{}, fmt.Errorf("total notifications (-n=%d) must be > 0", cfg.total)
	}
	if cfg.batchSize <= 0 || cfg.batchSize > 1000 {
		return priorityWeights{}, channelWeights{}, fmt.Errorf("batch size (-batch=%d) must be in [1, 1000]", cfg.batchSize)
	}
	if cfg.concurrency <= 0 {
		return priorityWeights{}, channelWeights{}, fmt.Errorf("concurrency (-concurrency=%d) must be > 0", cfg.concurrency)
	}
	mix := cfg.channelMix
	if mix == "" {
		mix = "34:33:33"
	}
	cw, err := parseChannelMix(mix)
	if err != nil {
		return priorityWeights{}, channelWeights{}, err
	}
	if _, err := selectChannel(cfg.channel, cw, 0); err != nil {
		return priorityWeights{}, channelWeights{}, err
	}
	pw, err := parsePriorityMix(cfg.priorityMix)
	return pw, cw, err
}

func buildBatchPayload(startSeq, count int, channelMode string, cw channelWeights, weights priorityWeights) ([]byte, error) {
	type itemDTO struct {
		Recipient string `json:"recipient"`
		Channel   string `json:"channel"`
		Content   string `json:"content"`
		Priority  string `json:"priority"`
	}
	type batchDTO struct {
		Notifications []itemDTO `json:"notifications"`
	}

	items := make([]itemDTO, count)
	for i := 0; i < count; i++ {
		seq := startSeq + i
		ch, err := selectChannel(channelMode, cw, seq)
		if err != nil {
			return nil, err
		}
		var recipient string
		switch ch {
		case "sms":
			recipient = fmt.Sprintf("+90555%07d", 1000000+(seq%8999999))
		case "email":
			recipient = fmt.Sprintf("loadgen-%d@example.com", seq)
		default:
			recipient = fmt.Sprintf("loadgen-device-token-%d", seq)
		}
		items[i] = itemDTO{
			Recipient: recipient,
			Channel:   ch,
			Content:   fmt.Sprintf("Load test message #%d", seq),
			Priority:  selectPriority(weights, seq),
		}
	}
	return json.Marshal(batchDTO{Notifications: items})
}

func runLoadgen(ctx context.Context, cfg loadConfig, out io.Writer) (loadResult, error) {
	weights, cw, err := validateLoadConfig(cfg)
	if err != nil {
		return loadResult{}, err
	}
	if cfg.pollEvery <= 0 {
		cfg.pollEvery = 500 * time.Millisecond
	}

	baseURL := strings.TrimRight(cfg.baseURL, "/")
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: newBearerTransport(&http.Transport{
			MaxIdleConns:        max(cfg.concurrency*2, 16),
			MaxIdleConnsPerHost: max(cfg.concurrency*2, 16),
			IdleConnTimeout:     60 * time.Second,
		}, cfg.apiKey),
	}

	type batchJob struct {
		startSeq int
		size     int
	}
	var jobs []batchJob
	for offset := 0; offset < cfg.total; offset += cfg.batchSize {
		sz := min(cfg.batchSize, cfg.total-offset)
		jobs = append(jobs, batchJob{startSeq: offset, size: sz})
	}

	jobCh := make(chan batchJob, len(jobs))
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)

	var (
		mu        sync.Mutex
		batchIDs  = make([]string, 0, len(jobs))
		latencies = make([]float64, 0, len(jobs))
		firstErr  error
		submitted atomic.Int64
	)

	startTime := time.Now()
	var wg sync.WaitGroup
	workerCount := min(cfg.concurrency, len(jobs))
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if ctx.Err() != nil {
					return
				}
				payload, buildErr := buildBatchPayload(job.startSeq, job.size, cfg.channel, cw, weights)
				if buildErr != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = buildErr
					}
					mu.Unlock()
					return
				}
				bid, durMS, submitErr := submitBatch(ctx, client, baseURL, payload)
				if submitErr != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = submitErr
					}
					mu.Unlock()
					return
				}
				submitted.Add(int64(job.size))
				mu.Lock()
				batchIDs = append(batchIDs, bid)
				latencies = append(latencies, durMS)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return loadResult{}, firstErr
	}
	if ctx.Err() != nil {
		return loadResult{}, ctx.Err()
	}

	createElapsed := time.Since(startTime)
	createdRate := float64(submitted.Load()) / max(createElapsed.Seconds(), 0.001)
	p50 := percentileFloat(latencies, 0.50)
	p95 := percentileFloat(latencies, 0.95)
	p99 := percentileFloat(latencies, 0.99)
	_, _ = fmt.Fprintf(out, "created=%d batches=%d elapsed=%s created_per_sec=%.1f batch_p50_ms=%.1f batch_p95_ms=%.1f batch_p99_ms=%.1f\n",
		submitted.Load(), len(batchIDs), createElapsed.Round(time.Millisecond), createdRate, p50, p95, p99)

	finalStatuses, err := pollBatchesUntilTerminal(ctx, client, baseURL, batchIDs, cfg.pollTimeout, cfg.pollEvery, out)
	totalElapsed := time.Since(startTime)
	if err != nil {
		return loadResult{
			Submitted:      int(submitted.Load()),
			BatchLatencies: latencies,
			CreateElapsed:  createElapsed,
			TotalElapsed:   totalElapsed,
			FinalStatuses:  finalStatuses,
		}, err
	}

	_, _ = fmt.Fprintf(out, "completed=%d total_elapsed=%s final_statuses=%v\n",
		submitted.Load(), totalElapsed.Round(time.Millisecond), finalStatuses)
	res := loadResult{
		Submitted:      int(submitted.Load()),
		BatchLatencies: latencies,
		CreateElapsed:  createElapsed,
		TotalElapsed:   totalElapsed,
		FinalStatuses:  finalStatuses,
	}
	if cfg.channelStats {
		stats, statErr := collectChannelStats(ctx, client, baseURL, batchIDs)
		if statErr != nil {
			return res, statErr
		}
		res.ChannelStats = stats
	}
	printSummary(out, res)
	return res, nil
}

func printSummary(out io.Writer, res loadResult) {
	sent := res.FinalStatuses["sent"]
	_, _ = fmt.Fprintf(out, "\n=== loadgen summary ===\n")
	_, _ = fmt.Fprintf(out, "notifications          %d\n", res.Submitted)
	_, _ = fmt.Fprintf(out, "create throughput      %.1f notif/s (%s)\n",
		float64(res.Submitted)/max(res.CreateElapsed.Seconds(), 0.001), res.CreateElapsed.Round(time.Millisecond))
	_, _ = fmt.Fprintf(out, "batch create latency   p50=%.1fms p95=%.1fms p99=%.1fms\n",
		percentileFloat(res.BatchLatencies, 0.50), percentileFloat(res.BatchLatencies, 0.95), percentileFloat(res.BatchLatencies, 0.99))
	_, _ = fmt.Fprintf(out, "time to all terminal   %s\n", res.TotalElapsed.Round(time.Millisecond))
	_, _ = fmt.Fprintf(out, "end-to-end throughput  %.1f sent/s\n", float64(sent)/max(res.TotalElapsed.Seconds(), 0.001))
	_, _ = fmt.Fprintf(out, "final statuses         %v\n", res.FinalStatuses)
	channels := make([]string, 0, len(res.ChannelStats))
	for ch := range res.ChannelStats {
		channels = append(channels, ch)
	}
	sort.Strings(channels)
	for _, ch := range channels {
		st := res.ChannelStats[ch]
		_, _ = fmt.Fprintf(out, "channel %-6s         sent=%d span=%s effective_rate=%.1f msg/s\n",
			ch, st.Sent, st.LastSent.Sub(st.FirstSent).Round(time.Millisecond), st.EffectiveRate())
	}
}

// collectChannelStats pages through each batch's items (keyset cursor, 200 per page) and aggregates
// the persisted sentAt timestamps per channel.
func collectChannelStats(ctx context.Context, client *http.Client, baseURL string, batchIDs []string) (map[string]channelStat, error) {
	stats := make(map[string]channelStat)
	for _, id := range batchIDs {
		cursor := ""
		for {
			pageURL := baseURL + "/api/v1/batches/" + id + "?limit=200"
			if cursor != "" {
				pageURL += "&cursor=" + url.QueryEscape(cursor)
			}
			var page struct {
				Items []struct {
					Channel string     `json:"channel"`
					SentAt  *time.Time `json:"sentAt"`
				} `json:"items"`
				NextCursor *string `json:"nextCursor"`
			}
			if err := getJSON(ctx, client, pageURL, &page); err != nil {
				return nil, fmt.Errorf("listing items of batch %s: %w", id, err)
			}
			for _, it := range page.Items {
				if it.SentAt == nil {
					continue
				}
				st := stats[it.Channel]
				if st.Sent == 0 || it.SentAt.Before(st.FirstSent) {
					st.FirstSent = *it.SentAt
				}
				if st.Sent == 0 || it.SentAt.After(st.LastSent) {
					st.LastSent = *it.SentAt
				}
				st.Sent++
				stats[it.Channel] = st
			}
			if page.NextCursor == nil || *page.NextCursor == "" {
				break
			}
			cursor = *page.NextCursor
		}
	}
	return stats, nil
}

func getJSON(ctx context.Context, client *http.Client, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}
	return json.Unmarshal(raw, out)
}

func submitBatch(ctx context.Context, client *http.Client, baseURL string, payload []byte) (string, float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/notifications/batch", bytes.NewReader(payload))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("submitting batch: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()
	elapsedMS := float64(time.Since(start).Microseconds()) / 1000.0

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", elapsedMS, fmt.Errorf("reading batch response: %w", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		return "", elapsedMS, fmt.Errorf("batch create returned HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var parsed struct {
		BatchID string `json:"batchId"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", elapsedMS, fmt.Errorf("decoding batch response: %w", err)
	}
	if parsed.BatchID == "" {
		return "", elapsedMS, errors.New("missing batchId in response")
	}
	return parsed.BatchID, elapsedMS, nil
}

func pollBatchesUntilTerminal(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	batchIDs []string,
	timeout time.Duration,
	interval time.Duration,
	out io.Writer,
) (map[string]int64, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var totals map[string]int64
	iter := 0
	for {
		var err error
		totals, err = fetchAllBatchCounts(pollCtx, client, baseURL, batchIDs)
		if err != nil {
			return totals, err
		}
		pending := nonTerminalCount(totals)
		if pending == 0 {
			return totals, nil
		}
		if iter%4 == 0 {
			_, _ = fmt.Fprintf(out, "draining non_terminal=%d statuses=%v\n", pending, totals)
		}
		iter++

		timer := time.NewTimer(interval)
		select {
		case <-pollCtx.Done():
			timer.Stop()
			return totals, fmt.Errorf("polling timed out (%s): %w", timeout, pollCtx.Err())
		case <-timer.C:
		}
	}
}

func fetchAllBatchCounts(ctx context.Context, client *http.Client, baseURL string, batchIDs []string) (map[string]int64, error) {
	totals := make(map[string]int64)
	for _, id := range batchIDs {
		counts, err := fetchBatchCounts(ctx, client, baseURL, id)
		if err != nil {
			return nil, err
		}
		for st, cnt := range counts {
			totals[st] += cnt
		}
	}
	return totals, nil
}

func fetchBatchCounts(ctx context.Context, client *http.Client, baseURL, batchID string) (map[string]int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/batches/"+batchID+"?limit=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("polling batch %s: %w", batchID, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading batch %s: %w", batchID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("batch %s returned HTTP %d: %s", batchID, resp.StatusCode, string(raw))
	}
	var parsed struct {
		CountsByStatus map[string]int64 `json:"countsByStatus"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("decoding batch %s: %w", batchID, err)
	}
	return parsed.CountsByStatus, nil
}

func nonTerminalCount(counts map[string]int64) int64 {
	return counts["pending"] + counts["processing"] + counts["retrying"] + counts["scheduled"]
}

func percentileFloat(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cp := make([]float64, len(samples))
	copy(cp, samples)
	sort.Float64s(cp)
	pos := p * float64(len(cp)-1)
	lo := int(math.Floor(pos))
	hi := min(lo+1, len(cp)-1)
	frac := pos - float64(lo)
	return cp[lo] + frac*(cp[hi]-cp[lo])
}
