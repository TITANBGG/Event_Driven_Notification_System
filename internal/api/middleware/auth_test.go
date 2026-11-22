package middleware_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api/middleware"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
)

type recordingAuthObserver struct {
	mu      sync.Mutex
	reasons []string
}

func (o *recordingAuthObserver) IncAuthFailure(reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reasons = append(o.reasons, reason)
}

const (
	readerSecret = "reader-secret-0123456789"
	writerSecret = "writer-secret-0123456789"
	adminSecret  = "admin-secret-01234567890"
)

func newAuthTestRouter(t *testing.T, keys []config.APIKey, obs middleware.AuthFailureObserver, logBuf *bytes.Buffer) http.Handler {
	t.Helper()
	if logBuf == nil {
		logBuf = &bytes.Buffer{}
	}
	logger := observability.NewLogger("info", logBuf)
	auth := middleware.NewAuthenticator(keys, obs)

	r := chi.NewRouter()
	r.Use(middleware.CorrelationID(logger))
	r.Use(middleware.Authenticate(auth))
	ok := func(w http.ResponseWriter, r *http.Request) {
		observability.LoggerFrom(r.Context(), logger).Info("handled")
		w.WriteHeader(http.StatusNoContent)
	}
	r.With(middleware.RequireScope(auth, config.ScopeRead)).Get("/read", ok)
	r.With(middleware.RequireScope(auth, config.ScopeWrite)).Post("/write", ok)
	r.With(middleware.RequireScope(auth, config.ScopeAdmin)).Post("/admin", ok)
	return r
}

func testKeys() []config.APIKey {
	return []config.APIKey{
		{Name: "reader", Secret: readerSecret, Scopes: []string{config.ScopeRead}},
		{Name: "writer", Secret: writerSecret, Scopes: []string{config.ScopeWrite}},
		{Name: "ops", Secret: adminSecret, Scopes: []string{config.ScopeAdmin}},
	}
}

func TestAuthenticateAndRequireScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		path        string
		headers     map[string]string
		wantStatus  int
		wantCode    string
		wantReason  string
		wantWWWAuth string
	}{
		{name: "missing header", method: http.MethodGet, path: "/read",
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized", wantReason: middleware.AuthFailureMissing,
			wantWWWAuth: `Bearer realm="notification-api"`},
		{name: "wrong scheme", method: http.MethodGet, path: "/read",
			headers:    map[string]string{"Authorization": "Basic " + readerSecret},
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized", wantReason: middleware.AuthFailureInvalid,
			wantWWWAuth: `Bearer realm="notification-api", error="invalid_token"`},
		{name: "unknown key", method: http.MethodGet, path: "/read",
			headers:    map[string]string{"Authorization": "Bearer not-a-configured-key-000"},
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized", wantReason: middleware.AuthFailureInvalid},
		{name: "empty bearer", method: http.MethodGet, path: "/read",
			headers:    map[string]string{"Authorization": "Bearer "},
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized", wantReason: middleware.AuthFailureInvalid},
		{name: "read key reads", method: http.MethodGet, path: "/read",
			headers: map[string]string{"Authorization": "Bearer " + readerSecret}, wantStatus: http.StatusNoContent},
		{name: "scheme is case-insensitive", method: http.MethodGet, path: "/read",
			headers: map[string]string{"Authorization": "bearer " + readerSecret}, wantStatus: http.StatusNoContent},
		{name: "read key cannot write", method: http.MethodPost, path: "/write",
			headers:    map[string]string{"Authorization": "Bearer " + readerSecret},
			wantStatus: http.StatusForbidden, wantCode: "forbidden", wantReason: middleware.AuthFailureForbidden,
			wantWWWAuth: `Bearer realm="notification-api", error="insufficient_scope", scope="write"`},
		{name: "write key cannot read", method: http.MethodGet, path: "/read",
			headers:    map[string]string{"Authorization": "Bearer " + writerSecret},
			wantStatus: http.StatusForbidden, wantCode: "forbidden", wantReason: middleware.AuthFailureForbidden},
		{name: "write key cannot replay", method: http.MethodPost, path: "/admin",
			headers:    map[string]string{"Authorization": "Bearer " + writerSecret},
			wantStatus: http.StatusForbidden, wantCode: "forbidden", wantReason: middleware.AuthFailureForbidden},
		{name: "admin implies read", method: http.MethodGet, path: "/read",
			headers: map[string]string{"Authorization": "Bearer " + adminSecret}, wantStatus: http.StatusNoContent},
		{name: "admin implies write", method: http.MethodPost, path: "/write",
			headers: map[string]string{"Authorization": "Bearer " + adminSecret}, wantStatus: http.StatusNoContent},
		{name: "admin replays", method: http.MethodPost, path: "/admin",
			headers: map[string]string{"Authorization": "Bearer " + adminSecret}, wantStatus: http.StatusNoContent},
		{name: "websocket subprotocol token", method: http.MethodGet, path: "/read",
			headers: map[string]string{
				"Upgrade": "websocket", "Connection": "Upgrade",
				"Sec-WebSocket-Protocol": "bearer, " + readerSecret,
			}, wantStatus: http.StatusNoContent},
		{name: "subprotocol ignored without upgrade", method: http.MethodGet, path: "/read",
			headers:    map[string]string{"Sec-WebSocket-Protocol": "bearer, " + readerSecret},
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized", wantReason: middleware.AuthFailureMissing},
		{name: "subprotocol bearer without token", method: http.MethodGet, path: "/read",
			headers:    map[string]string{"Upgrade": "websocket", "Sec-WebSocket-Protocol": "bearer"},
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized", wantReason: middleware.AuthFailureMissing},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obs := &recordingAuthObserver{}
			h := newAuthTestRouter(t, testKeys(), obs, nil)

			req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, nil)
			req.Header.Set("X-Correlation-ID", "corr-auth-1")
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tc.wantStatus, rec.Code)
			if tc.wantCode == "" {
				assert.Empty(t, obs.reasons)
				return
			}
			var body struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
				CorrelationID string `json:"correlationId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tc.wantCode, body.Error.Code)
			assert.Equal(t, "corr-auth-1", body.CorrelationID)
			assert.NotContains(t, rec.Body.String(), "secret", "error bodies must never echo a credential")
			assert.Equal(t, []string{tc.wantReason}, obs.reasons)
			if tc.wantWWWAuth != "" {
				assert.Equal(t, tc.wantWWWAuth, rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

func TestAuthDisabledIsNoOp(t *testing.T) {
	t.Parallel()
	obs := &recordingAuthObserver{}
	h := newAuthTestRouter(t, nil, obs, nil)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/read"}, {http.MethodPost, "/write"}, {http.MethodPost, "/admin"},
	} {
		req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNoContent, rec.Code, tc.path)
	}
	assert.Empty(t, obs.reasons)
	assert.False(t, middleware.NewAuthenticator(nil, nil).Enabled())
}

func TestAuthLogsKeyNameNeverSecret(t *testing.T) {
	t.Parallel()
	var logBuf bytes.Buffer
	h := newAuthTestRouter(t, testKeys(), nil, &logBuf)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/read", nil)
	req.Header.Set("Authorization", "Bearer "+readerSecret)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	var entry map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logBuf.Bytes()), &entry))
	assert.Equal(t, "reader", entry["api_key_name"])
	assert.NotContains(t, logBuf.String(), readerSecret)
}

func TestPrincipalFromContext(t *testing.T) {
	t.Parallel()
	auth := middleware.NewAuthenticator(testKeys(), nil)

	var got middleware.Principal
	var found bool
	h := middleware.Authenticate(auth)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, found = middleware.PrincipalFrom(r.Context())
	}))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+writerSecret)
	h.ServeHTTP(httptest.NewRecorder(), req)

	require.True(t, found)
	assert.Equal(t, "writer", got.Name)
	assert.True(t, got.HasScope(config.ScopeWrite))
	assert.False(t, got.HasScope(config.ScopeRead))
	assert.False(t, got.HasScope(config.ScopeAdmin))
}

// RequireScope without a preceding Authenticate must fail closed rather than let the request through.
func TestRequireScopeWithoutAuthenticateFailsClosed(t *testing.T) {
	t.Parallel()
	auth := middleware.NewAuthenticator(testKeys(), nil)
	h := middleware.RequireScope(auth, config.ScopeRead)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// Keys rotate atomically: the new key works and the old one stops at once, while an empty set or
// reloading a disabled authenticator is refused so a bad edit can neither lock out nor open the API.
func TestAuthenticatorReloadRotatesKeys(t *testing.T) {
	t.Parallel()
	auth := middleware.NewAuthenticator([]config.APIKey{{Name: "old", Secret: readerSecret, Scopes: []string{config.ScopeRead}}}, nil)
	h := middleware.Authenticate(auth)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	call := func(secret string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+secret)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	require.Equal(t, http.StatusNoContent, call(readerSecret))

	require.NoError(t, auth.Reload([]config.APIKey{{Name: "new", Secret: writerSecret, Scopes: []string{config.ScopeRead}}}))
	assert.Equal(t, http.StatusNoContent, call(writerSecret), "the new key is accepted")
	assert.Equal(t, http.StatusUnauthorized, call(readerSecret), "the old key is revoked")

	assert.Error(t, auth.Reload(nil), "an empty key set is refused")
	assert.Equal(t, http.StatusNoContent, call(writerSecret), "a refused reload keeps the current keys")
	assert.Error(t, middleware.NewAuthenticator(nil, nil).Reload([]config.APIKey{{Name: "x", Secret: adminSecret, Scopes: []string{"admin"}}}),
		"a reload cannot switch authentication on")
}
