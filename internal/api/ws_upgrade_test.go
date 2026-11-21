package api_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A plain GET with a valid filter is not a WebSocket handshake: it must get the standard error envelope
// (fuzzing found a text/plain 426 written by the WebSocket library), while a missing filter still wins.
func TestWebSocketEndpointRejectsNonUpgradeRequestsWithTheErrorEnvelope(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestRouter(t)

	rec := doJSONRequest(t, h, http.MethodGet, "/api/v1/ws?channel=sms", "", nil)
	require.Equal(t, http.StatusUpgradeRequired, rec.Code)
	assert.Equal(t, "websocket", rec.Header().Get("Upgrade"))
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		CorrelationID string `json:"correlationId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "upgrade_required", env.Error.Code)
	assert.NotEmpty(t, env.CorrelationID)

	rec = doJSONRequest(t, h, http.MethodGet, "/api/v1/ws", "", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code, "filter validation still runs first")
	assert.Contains(t, rec.Body.String(), "missing_ws_filter")

	// Headers are matched case-insensitively and as comma-separated tokens (RFC 6455, RFC 9110).
	rec = doJSONRequest(t, h, http.MethodGet, "/api/v1/ws?channel=sms", "", map[string]string{
		"Connection": "keep-alive, Upgrade", "Upgrade": "WebSocket",
	})
	assert.NotEqual(t, http.StatusUpgradeRequired, rec.Code, "a real handshake is not rejected as non-upgrade")
}
