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

	var windows WindowsResponse
	for _, row := range windowRows {
		stats := windows.slot(row.Window, row.Channel)
		if stats == nil {
			continue // a window or channel this API version does not report
		}
		*stats = WindowChannelStatsResponse{Sent: row.SentCount, Failed: row.FailedCount, Retrying: row.RetryingCount}
		if total := row.SentCount + row.FailedCount; total > 0 {
			success := roundMetricRatio(float64(row.SentCount) / float64(total))
			failure := roundMetricRatio(float64(row.FailedCount) / float64(total))
			stats.SuccessRate, stats.FailureRate = &success, &failure
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

// slot returns the stats cell for a window and channel, or nil for one the response does not have.
func (w *WindowsResponse) slot(window string, ch domain.Channel) *WindowChannelStatsResponse {
	var perChannel *WindowChannelsResponse
	switch window {
	case "1m":
		perChannel = &w.OneMinute
	case "5m":
		perChannel = &w.FiveMinutes
	case "15m":
		perChannel = &w.FifteenMinutes
	default:
		return nil
	}
	switch ch {
	case domain.ChannelSMS:
		return &perChannel.SMS
	case domain.ChannelEmail:
		return &perChannel.Email
	case domain.ChannelPush:
		return &perChannel.Push
	default:
		return nil
	}
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
