package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

//go:embed static/demo.html
var demoHTML []byte

const (
	wsPingInterval = 30 * time.Second
	wsPingTimeout  = 10 * time.Second
	wsWriteTimeout = 5 * time.Second
)

// WSSnapshotMessage is the initial JSON frame sent over GET /api/v1/ws immediately after subscription.
type WSSnapshotMessage struct {
	Type         string                `json:"type"`
	Notification *NotificationResponse `json:"notification,omitempty"`
	Batch        *BatchDetailResponse  `json:"batch,omitempty"`
}

func (h *handler) handleDemoPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(demoHTML)
}

func (h *handler) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	filter, err := parseWSFilter(r)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	// Reject plain HTTP requests before subscribing or reading the snapshot from PostgreSQL. Left to the
	// WebSocket library, they got a text/plain 426 without the error envelope (found by fuzzing) after a
	// wasted database read.
	if !isWebSocketUpgrade(r) {
		w.Header().Set("Upgrade", "websocket")
		writeError(w, r, h.logger, &apiError{
			status:  http.StatusUpgradeRequired,
			code:    "upgrade_required",
			message: "this endpoint only accepts WebSocket upgrade requests (Connection: Upgrade, Upgrade: websocket)",
		})
		return
	}

	// Enforce the Origin allowlist before subscribing to the Hub or reading the snapshot so a
	// disallowed origin receives a standard JSON 403 without consuming a hub slot or DB query.
	originPatterns := buildWSOriginPatterns(h.cfg.CORSAllowedOrigins, r.Host)
	if !isAllowedWSOrigin(r.Host, r.Header.Get("Origin"), originPatterns) {
		writeError(w, r, h.logger, &apiError{
			status:  http.StatusForbidden,
			code:    "forbidden_origin",
			message: "request origin is not allowed for WebSocket connections",
		})
		return
	}

	if h.hub == nil {
		writeError(w, r, h.logger, &apiError{
			status:  http.StatusServiceUnavailable,
			code:    "service_unavailable",
			message: "realtime websocket hub is not available",
		})
		return
	}

	// 1. Subscribe FIRST before reading the snapshot from PostgreSQL so no state transition
	// occurring between snapshot read and stream start can be missed.
	client, unsub, subErr := h.hub.Subscribe(r.Context(), filter)
	if subErr != nil {
		if errors.Is(subErr, realtime.ErrTooManyClients) {
			writeError(w, r, h.logger, &apiError{
				status:  http.StatusServiceUnavailable,
				code:    "too_many_ws_clients",
				message: fmt.Sprintf("maximum concurrent websocket clients (%d) exceeded", h.cfg.MaxWSClients),
			})
			return
		}
		writeError(w, r, h.logger, subErr)
		return
	}
	defer unsub()

	// 2. Build the initial snapshot from PostgreSQL before upgrading so a 404 or DB error can return clean HTTP status if needed,
	// or send the snapshot immediately after WebSocket handshake.
	snapshot, snapErr := h.buildWSSnapshot(r.Context(), filter)
	if snapErr != nil && !errors.Is(snapErr, domain.ErrNotFound) {
		writeError(w, r, h.logger, snapErr)
		return
	}

	conn, acceptErr := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: originPatterns,
	})
	if acceptErr != nil {
		return
	}
	defer func() {
		_ = conn.CloseNow()
	}()

	// Bound incoming control/close frames and drain client reads so Ping/Pong frames are processed by coder/websocket.
	conn.SetReadLimit(4096)
	connCtx := conn.CloseRead(r.Context())

	if snapshot != nil {
		if writeErr := writeWSJSON(connCtx, conn, snapshot); writeErr != nil {
			return
		}
	}

	// 3. Stream live events and send periodic 30s pings.
	pingTicker := time.NewTicker(wsPingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-connCtx.Done():
			_ = conn.Close(websocket.StatusNormalClosure, "connection closed")
			return

		case <-client.Closed():
			// Closed fires for every removal; Dropped tells a backpressure drop (1008) from a normal one.
			// A client whose TCP window is full cannot receive any close frame: such a client is cut by
			// the write timeout instead and sees 1006.
			select {
			case <-client.Dropped():
				_ = conn.Close(websocket.StatusPolicyViolation, "slow client: not reading")
			default:
				_ = conn.Close(websocket.StatusNormalClosure, "hub closed subscription")
			}
			return

		case <-client.Ready():
			for _, frame := range client.Drain() {
				if writeErr := writeWSFrame(connCtx, conn, frame); writeErr != nil {
					return
				}
				client.MarkProgress()
			}

		case <-pingTicker.C:
			pingCtx, cancelPing := context.WithTimeout(connCtx, wsPingTimeout)
			pingErr := conn.Ping(pingCtx)
			cancelPing()
			if pingErr != nil {
				_ = conn.Close(websocket.StatusGoingAway, "ping timeout")
				return
			}
		}
	}
}

// isWebSocketUpgrade reports whether r asks for a WebSocket upgrade (RFC 6455 section 4.1).
func isWebSocketUpgrade(r *http.Request) bool {
	return headerHasToken(r.Header, "Connection", "upgrade") && headerHasToken(r.Header, "Upgrade", "websocket")
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func parseWSFilter(r *http.Request) (realtime.Filter, error) {
	q := r.URL.Query()
	var filter realtime.Filter

	if rawNotifID := strings.TrimSpace(q.Get("notificationId")); rawNotifID != "" {
		id, err := uuid.Parse(rawNotifID)
		if err != nil {
			return realtime.Filter{}, &apiError{
				status:  http.StatusBadRequest,
				code:    "invalid_uuid",
				message: `query parameter "notificationId" must be a valid UUID`,
			}
		}
		filter.NotificationID = &id
	}

	if rawBatchID := strings.TrimSpace(q.Get("batchId")); rawBatchID != "" {
		id, err := uuid.Parse(rawBatchID)
		if err != nil {
			return realtime.Filter{}, &apiError{
				status:  http.StatusBadRequest,
				code:    "invalid_uuid",
				message: `query parameter "batchId" must be a valid UUID`,
			}
		}
		filter.BatchID = &id
	}

	if rawChannel := strings.TrimSpace(q.Get("channel")); rawChannel != "" {
		ch, err := domain.ParseChannel(rawChannel)
		if err != nil {
			return realtime.Filter{}, &apiError{
				status:  http.StatusBadRequest,
				code:    "invalid_channel",
				message: `query parameter "channel" must be one of sms, email, push`,
			}
		}
		filter.Channel = &ch
	}

	if !filter.Valid() {
		return realtime.Filter{}, &apiError{
			status:  http.StatusBadRequest,
			code:    "missing_ws_filter",
			message: "at least one filter query parameter (notificationId, batchId, or channel) is required",
		}
	}
	return filter, nil
}

func (h *handler) buildWSSnapshot(ctx context.Context, filter realtime.Filter) (*WSSnapshotMessage, error) {
	if filter.NotificationID == nil && filter.BatchID == nil {
		return nil, nil
	}

	snap := &WSSnapshotMessage{Type: "snapshot"}

	if filter.NotificationID != nil {
		n, err := h.store.GetNotification(ctx, *filter.NotificationID)
		if err != nil {
			return nil, err
		}
		resp := toNotificationResponse(*n)
		snap.Notification = &resp
	}

	if filter.BatchID != nil {
		b, err := h.store.GetBatch(ctx, *filter.BatchID)
		if err != nil {
			return nil, err
		}
		summary, err := h.store.GetBatchSummary(ctx, *filter.BatchID)
		if err != nil {
			return nil, err
		}
		counts := make(map[string]int64, len(summary))
		for st, cnt := range summary {
			counts[st.String()] = cnt
		}
		items, err := h.store.ListBatchItems(ctx, *filter.BatchID, nil, h.cfg.PageLimitDefault)
		if err != nil {
			return nil, err
		}
		snap.Batch = &BatchDetailResponse{
			BatchID:        b.ID,
			Total:          b.Total,
			CountsByStatus: counts,
			CreatedAt:      b.CreatedAt.UTC(),
			Items:          toNotificationResponses(items),
		}
	}

	return snap, nil
}

func writeWSJSON(ctx context.Context, conn *websocket.Conn, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return writeWSFrame(ctx, conn, raw)
}

// writeWSFrame writes one pre-encoded JSON frame; live events arrive already encoded by the Hub.
func writeWSFrame(ctx context.Context, conn *websocket.Conn, frame []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, frame)
}

func buildWSOriginPatterns(origins []string, reqHost string) []string {
	var patterns []string
	seen := make(map[string]struct{})
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			patterns = append(patterns, p)
		}
	}
	for _, o := range origins {
		o = strings.TrimSpace(o)
		if o == "*" {
			return []string{"*"}
		}
		if u, err := url.Parse(o); err == nil && u.Host != "" {
			add(u.Host)
		} else {
			add(o)
		}
	}
	if reqHost != "" {
		add(reqHost)
	}
	add("localhost:*")
	add("127.0.0.1:*")
	return patterns
}

// isAllowedWSOrigin mirrors coder/websocket's authenticateOrigin (v1.8.14) rule for rule, so the
// handler can answer a refused origin with the standard JSON envelope before websocket.Accept would
// write its own text/plain 403. Any divergence would let one of the two checks decide differently:
// a stricter copy rejects origins the library accepts, a looser one lets the plain-text 403 through.
func isAllowedWSOrigin(reqHost, origin string, patterns []string) bool {
	if origin == "" {
		return true // not a browser request; the library allows it too
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if strings.EqualFold(reqHost, u.Host) {
		return true
	}
	for _, p := range patterns {
		target := u.Host
		if strings.Contains(p, "://") {
			target = u.Scheme + "://" + u.Host
		}
		if matched, matchErr := path.Match(strings.ToLower(p), strings.ToLower(target)); matchErr == nil && matched {
			return true
		}
	}
	return false
}
