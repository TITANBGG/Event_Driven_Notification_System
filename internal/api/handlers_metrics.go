package api

import (
	"math"
	"net/http"
	"time"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/queue"
)

const metricsCacheTTL = 2 * time.Second

func (h *handler) handleMetricsSnapshot(w http.ResponseWriter, r *http.Request) {
	now := h.nowFn().UTC()

	h.metricsMu.Lock()
	if now.Before(h.metricsExp) {
		cached := h.metricsCache
		h.metricsMu.Unlock()
		writeJSON(w, h.logger, http.StatusOK, cached)
		return
	}
	h.metricsMu.Unlock()

	var depths queue.MetricsDepths
	if h.queueInspector != nil {
		sampled, err := h.queueInspector.Depths(r.Context())
		if err != nil {
			writeError(w, r, h.logger, err)
			return
		}
		depths = sampled
	}

	windowRows, err := h.store.MetricsWindowCounts(r.Context())
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	latencyStats, err := h.store.LatencyPercentiles(r.Context())
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	windows := make(map[string]map[string]WindowChannelStatsResponse, 3)
	for _, win := range []string{"1m", "5m", "15m"} {
		perChannel := make(map[string]WindowChannelStatsResponse, 3)
		for _, ch := range channels {
			perChannel[ch.String()] = WindowChannelStatsResponse{}
		}
		windows[win] = perChannel
	}
	for _, row := range windowRows {
		perChannel, ok := windows[row.Window]
		if !ok {
			perChannel = make(map[string]WindowChannelStatsResponse, 3)
			windows[row.Window] = perChannel
		}
		var successRate, failureRate float64
		total := row.SentCount + row.FailedCount
		if total > 0 {
			successRate = roundMetricRatio(float64(row.SentCount) / float64(total))
			failureRate = roundMetricRatio(float64(row.FailedCount) / float64(total))
		}
		perChannel[row.Channel.String()] = WindowChannelStatsResponse{
			Sent:        row.SentCount,
			Failed:      row.FailedCount,
			Retrying:    row.RetryingCount,
			SuccessRate: successRate,
			FailureRate: failureRate,
		}
	}

	snapshot := MetricsSnapshotResponse{
		GeneratedAt: now,
		Queues: QueuesSnapshotResponse{
			SMS:      mapChannelQueueDepths(depths, domain.ChannelSMS),
			Email:    mapChannelQueueDepths(depths, domain.ChannelEmail),
			Push:     mapChannelQueueDepths(depths, domain.ChannelPush),
			Delayed:  depths.Delayed,
			DLQ:      depths.DLQ,
			Inflight: depths.Inflight,
		},
		Windows: windows,
		LatencyMS: LatencyMetricsResponse{
			EndToEnd: PercentilesResponse{
				P50: roundMetricMS(latencyStats.EndToEndP50MS),
				P95: roundMetricMS(latencyStats.EndToEndP95MS),
				P99: roundMetricMS(latencyStats.EndToEndP99MS),
			},
			ProviderCall: PercentilesResponse{
				P50: roundMetricMS(latencyStats.ProviderP50MS),
				P95: roundMetricMS(latencyStats.ProviderP95MS),
				P99: roundMetricMS(latencyStats.ProviderP99MS),
			},
		},
	}

	h.metricsMu.Lock()
	h.metricsCache = snapshot
	h.metricsExp = h.nowFn().UTC().Add(metricsCacheTTL)
	h.metricsMu.Unlock()

	writeJSON(w, h.logger, http.StatusOK, snapshot)
}

func mapChannelQueueDepths(depths queue.MetricsDepths, ch domain.Channel) ChannelQueueDepthsResponse {
	perPriority := depths.ByChannel[ch]
	if perPriority == nil {
		return ChannelQueueDepthsResponse{}
	}
	return ChannelQueueDepthsResponse{
		High:   perPriority[domain.PriorityHigh],
		Normal: perPriority[domain.PriorityNormal],
		Low:    perPriority[domain.PriorityLow],
	}
}

func roundMetricRatio(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func roundMetricMS(v float64) float64 {
	return math.Round(v*100) / 100
}
