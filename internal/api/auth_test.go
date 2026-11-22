package api_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

const (
	testReadKey  = "reader-key-0123456789abcdef"
	testWriteKey = "writer-key-0123456789abcdef"
	testAdminKey = "admin-key-0123456789abcdefg"
)

func withTestAPIKeys(cfg *config.Config) {
	cfg.APIKeys = []config.APIKey{
		{Name: "reader", Secret: testReadKey, Scopes: []string{config.ScopeRead}},
		{Name: "writer", Secret: testWriteKey, Scopes: []string{config.ScopeWrite}},
		{Name: "ops", Secret: testAdminKey, Scopes: []string{config.ScopeAdmin}},
	}
}

func bearer(key string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + key}
}

// The scope each /api/v1 operation requires. A new route that is not listed here fails
// TestEveryAPIRouteRequiresAuthentication, so no endpoint can be added without deciding its scope.
var routeScopes = map[string]string{
	"POST /api/v1/notifications":             config.ScopeWrite,
	"POST /api/v1/notifications/batch":       config.ScopeWrite,
	"GET /api/v1/notifications":              config.ScopeRead,
	"GET /api/v1/notifications/{id}":         config.ScopeRead,
	"POST /api/v1/notifications/{id}/cancel": config.ScopeWrite,
	"POST /api/v1/notifications/{id}/retry":  config.ScopeAdmin,
	"POST /api/v1/notifications/retry":       config.ScopeAdmin,
	"GET /api/v1/batches/{id}":               config.ScopeRead,
	"POST /api/v1/batches/{id}/cancel":       config.ScopeWrite,
	"POST /api/v1/templates":                 config.ScopeWrite,
	"GET /api/v1/templates":                  config.ScopeRead,
	"GET /api/v1/templates/{id}":             config.ScopeRead,
	"PUT /api/v1/templates/{id}":             config.ScopeWrite,
	"DELETE /api/v1/templates/{id}":          config.ScopeWrite,
	"POST /api/v1/templates/{id}/preview":    config.ScopeRead,
	"GET /api/v1/ws":                         config.ScopeRead,
	"GET /api/v1/metrics":                    config.ScopeRead,
}

var keyForScope = map[string]string{
	config.ScopeRead:  testReadKey,
	config.ScopeWrite: testWriteKey,
	config.ScopeAdmin: testAdminKey,
}

func concretePath(route string) string {
	return strings.ReplaceAll(route, "{id}", "01936f7e-0000-7000-8000-000000000001")
}

func TestEveryAPIRouteRequiresAuthentication(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestRouter(t, withTestAPIKeys)

	var apiRoutes []string
	require.NoError(t, chi.Walk(h.(chi.Router), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if strings.HasPrefix(route, "/api/") {
			apiRoutes = append(apiRoutes, method+" "+route)
		}
		return nil
	}))
	require.NotEmpty(t, apiRoutes)

	for _, op := range apiRoutes {
		scope, listed := routeScopes[op]
		require.True(t, listed, "route %s has no scope in routeScopes", op)

		method, route, _ := strings.Cut(op, " ")
		rec := doJSONRequest(t, h, method, concretePath(route), "", nil)
		assert.Equal(t, http.StatusUnauthorized, rec.Code, "%s without a key", op)
		assert.Contains(t, rec.Body.String(), `"unauthorized"`, op)

		// Every key that lacks the scope gets 403; the scope's own key gets past auth (any non-401/403).
		for keyScope, key := range keyForScope {
			rec = doJSONRequest(t, h, method, concretePath(route), "", bearer(key))
			allowed := keyScope == scope || keyScope == config.ScopeAdmin
			if allowed {
				assert.NotContains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, rec.Code,
					"%s with %s key must pass auth", op, keyScope)
			} else {
				assert.Equal(t, http.StatusForbidden, rec.Code, "%s with %s key", op, keyScope)
			}
		}
	}
}

func TestPublicEndpointsStayOpenWithAuthEnabled(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestRouter(t, withTestAPIKeys)

	for _, path := range []string{"/health/live", "/openapi.yaml", "/demo", "/metrics"} {
		rec := doJSONRequest(t, h, http.MethodGet, path, "", nil)
		assert.Equal(t, http.StatusOK, rec.Code, path)
	}
	// Readiness reports dependency state (503 here: the test router has no Redis inspector), never 401.
	rec := doJSONRequest(t, h, http.MethodGet, "/health/ready", "", nil)
	assert.NotEqual(t, http.StatusUnauthorized, rec.Code)

	// CORS preflight carries no credentials and must still succeed.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/api/v1/notifications", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
	prec := httptest.NewRecorder()
	h.ServeHTTP(prec, req)
	assert.Equal(t, http.StatusNoContent, prec.Code)
	assert.Contains(t, prec.Header().Get("Access-Control-Allow-Headers"), "Authorization")
}

func TestCreateWithWriteKeyAndAuthFailureMetrics(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	withTestAPIKeys(&cfg)
	metrics := observability.NewAPIMetrics()
	h := api.NewRouter(api.Deps{
		Config:   cfg,
		Logger:   observability.NewLogger("error", &bytes.Buffer{}),
		Store:    newFakeStore(),
		Enqueuer: api.NewMemoryEnqueuer(),
		Metrics:  metrics,
	})

	body := `{"recipient":"+905551234567","channel":"sms","content":"auth test"}`
	rec := doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", body, bearer(testWriteKey))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	_ = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", body, nil)
	_ = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", body, bearer("wrong-key-0123456789abcdef"))
	_ = doJSONRequest(t, h, http.MethodPost, "/api/v1/notifications", body, bearer(testReadKey))

	scrape := doJSONRequest(t, h, http.MethodGet, "/metrics", "", nil)
	require.Equal(t, http.StatusOK, scrape.Code)
	text := scrape.Body.String()
	assert.Contains(t, text, `http_auth_failures_total{reason="missing"} 1`)
	assert.Contains(t, text, `http_auth_failures_total{reason="invalid"} 1`)
	assert.Contains(t, text, `http_auth_failures_total{reason="forbidden"} 1`)
	assert.NotContains(t, text, testWriteKey)
}

func TestWebSocketAuthenticatesWithBearerSubprotocol(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	withTestAPIKeys(&cfg)

	hub := realtime.NewHub(realtime.HubConfig{MaxClients: 10})
	hubCtx, cancelHub := context.WithCancel(t.Context())
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(hubCtx) }()
	t.Cleanup(func() {
		cancelHub()
		assert.NoError(t, <-hubDone)
	})

	router := api.NewRouter(api.Deps{
		Config: cfg,
		Logger: observability.NewLogger("error", &bytes.Buffer{}),
		Store:  newFakeStore(),
		Hub:    hub,
	})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/v1/ws?channel=sms"

	dial := func(opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		return websocket.Dial(ctx, wsURL, opts)
	}
	closeBody := func(resp *http.Response) {
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}

	// No credential: rejected before the upgrade with the standard 401.
	_, resp, err := dial(nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	closeBody(resp)

	// Browser path: token in the subprotocol list; the server selects only "bearer".
	conn, resp, err := dial(&websocket.DialOptions{Subprotocols: []string{"bearer", testReadKey}})
	require.NoError(t, err)
	closeBody(resp)
	assert.Equal(t, "bearer", conn.Subprotocol(), "the token must never be echoed as the selected subprotocol")
	_ = conn.Close(websocket.StatusNormalClosure, "")

	// Non-browser path: Authorization header on the handshake.
	conn, resp, err = dial(&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + testReadKey}}})
	require.NoError(t, err)
	closeBody(resp)
	assert.Empty(t, conn.Subprotocol())
	_ = conn.Close(websocket.StatusNormalClosure, "")

	// A write-only key cannot subscribe to status events.
	_, resp, err = dial(&websocket.DialOptions{Subprotocols: []string{"bearer", testWriteKey}})
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	closeBody(resp)
}
