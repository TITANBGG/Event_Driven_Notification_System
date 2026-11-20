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
	priorityMix string
	pollTimeout time.Duration
	pollEvery   time.Duration
}

type priorityWeights struct {
	high   int
	normal int
	low    int
}

type loadResult struct {
	Submitted      int
	BatchLatencies []float64
	CreateElapsed  time.Duration
	TotalElapsed   time.Duration
	FinalStatuses  map[string]int64
}

func main() {
	cfg := loadConfig{}
	flag.StringVar(&cfg.baseURL, "url", "http://localhost:8080", "API base URL")
	flag.IntVar(&cfg.total, "n", 2000, "Total notifications to create")
	flag.IntVar(&cfg.batchSize, "batch", 100, "Notifications per batch request (1..1000)")
	flag.IntVar(&cfg.concurrency, "concurrency", 4, "Concurrent batch submission workers")
	flag.StringVar(&cfg.channel, "channel", "mixed", "Target channel: mixed, sms, email, or push")
	flag.StringVar(&cfg.priorityMix, "priority-mix", "20:60:20", "Priority weights high:normal:low summing to 100")
	flag.DurationVar(&cfg.pollTimeout, "poll-timeout", 5*time.Minute, "Maximum time to wait for all batches to complete")
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

func parsePriorityMix(raw string) (priorityWeights, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 3 {
		return priorityWeights{}, fmt.Errorf("invalid priority-mix %q: expected high:normal:low", raw)
	}
	vals := make([]int, 3)
	sum := 0
	for i, part := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || v < 0 {
			return priorityWeights{}, fmt.Errorf("invalid priority-mix weight %q", part)
		}
		vals[i] = v
		sum += v
	}
	if sum != 100 {
		return priorityWeights{}, fmt.Errorf("priority-mix weights must sum to 100, got %d", sum)
	}
	return priorityWeights{high: vals[0], normal: vals[1], low: vals[2]}, nil
}

func selectChannel(mode string, seq int) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "mixed":
		channels := [3]string{"sms", "email", "push"}
		return channels[seq%3], nil
	case "sms", "email", "push":
		return strings.ToLower(strings.TrimSpace(mode)), nil
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

func validateLoadConfig(cfg loadConfig) (priorityWeights, error) {
	if cfg.total <= 0 {
		return priorityWeights{}, fmt.Errorf("total notifications (-n=%d) must be > 0", cfg.total)
	}
	if cfg.batchSize <= 0 || cfg.batchSize > 1000 {
		return priorityWeights{}, fmt.Errorf("batch size (-batch=%d) must be in [1, 1000]", cfg.batchSize)
	}
	if cfg.concurrency <= 0 {
		return priorityWeights{}, fmt.Errorf("concurrency (-concurrency=%d) must be > 0", cfg.concurrency)
	}
	if _, err := selectChannel(cfg.channel, 0); err != nil {
		return priorityWeights{}, err
	}
	if cfg.pollEvery <= 0 {
		cfg.pollEvery = 500 * time.Millisecond
	}
	return parsePriorityMix(cfg.priorityMix)
}

func buildBatchPayload(startSeq, count int, channelMode string, weights priorityWeights) ([]byte, error) {
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
		ch, err := selectChannel(channelMode, seq)
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
	weights, err := validateLoadConfig(cfg)
	if err != nil {
		return loadResult{}, err
	}

	baseURL := strings.TrimRight(cfg.baseURL, "/")
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        max(cfg.concurrency*2, 16),
			MaxIdleConnsPerHost: max(cfg.concurrency*2, 16),
			IdleConnTimeout:     60 * time.Second,
		},
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
				payload, buildErr := buildBatchPayload(job.startSeq, job.size, cfg.channel, weights)
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
	_, _ = fmt.Fprintf(out, "created=%d batches=%d elapsed=%s created_per_sec=%.1f batch_p50_ms=%.1f batch_p95_ms=%.1f\n",
		submitted.Load(), len(batchIDs), createElapsed.Round(time.Millisecond), createdRate, p50, p95)

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
	return loadResult{
		Submitted:      int(submitted.Load()),
		BatchLatencies: latencies,
		CreateElapsed:  createElapsed,
		TotalElapsed:   totalElapsed,
		FinalStatuses:  finalStatuses,
	}, nil
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
