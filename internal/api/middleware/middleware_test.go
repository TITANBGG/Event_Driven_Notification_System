package middleware_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/api/middleware"
	"github.com/TITANBGG/notification-system/internal/observability"
)

func TestRecovererLogsPanicAndEchoesCorrelationID(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := observability.NewLogger("info", &logBuf)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer(logger))
	r.Use(middleware.CorrelationID(logger))
	r.Get("/panic", func(_ http.ResponseWriter, _ *http.Request) {
		panic("boom")
	})
	r.Get("/partial", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		panic("after-header")
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic", nil)
	req.Header.Set("X-Correlation-ID", "corr-panic-123")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "corr-panic-123", rec.Header().Get("X-Correlation-ID"))
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")

	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		CorrelationID string `json:"correlationId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "internal_error", body.Error.Code)
	assert.Equal(t, "corr-panic-123", body.CorrelationID)

	var logEntry map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logBuf.Bytes()), &logEntry))
	assert.Equal(t, "corr-panic-123", logEntry["correlation_id"])
	assert.Equal(t, "boom", logEntry["panic"])
	assert.NotEmpty(t, logEntry["stack"])

	// When headers were already sent, Recoverer must not overwrite status or append corrupt JSON.
	partialReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/partial", nil)
	partialRec := httptest.NewRecorder()
	r.ServeHTTP(partialRec, partialReq)
	assert.Equal(t, http.StatusCreated, partialRec.Code)
	assert.Empty(t, partialRec.Body.String())
}

func TestCorrelationIDValidationAndGeneration(t *testing.T) {
	t.Parallel()

	logger := observability.NewLogger("info", &bytes.Buffer{})
	tests := []struct {
		name      string
		incoming  string
		expectNew bool
	}{
		{name: "valid custom id", incoming: "order.42_req-1", expectNew: false},
		{name: "empty header generates uuid", incoming: "", expectNew: true},
		{name: "invalid characters rejected", incoming: "bad id with spaces!", expectNew: true},
		{name: "too long rejected", incoming: strings.Repeat("a", 129), expectNew: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ctxCID string
			var hasCtxLogger bool

			h := middleware.CorrelationID(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctxCID = observability.CorrelationID(r.Context())
				hasCtxLogger = observability.Logger(r.Context()) != nil
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			if tc.incoming != "" {
				req.Header.Set("X-Correlation-ID", tc.incoming)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			gotHeader := rec.Header().Get("X-Correlation-ID")
			assert.NotEmpty(t, gotHeader)
			assert.Equal(t, gotHeader, ctxCID)
			assert.True(t, hasCtxLogger)
			if tc.expectNew {
				assert.NotEqual(t, tc.incoming, gotHeader)
				assert.Len(t, gotHeader, 36)
			} else {
				assert.Equal(t, tc.incoming, gotHeader)
			}
		})
	}
}

func TestRequestLoggerLogsRoutePatternAndCorrelationID(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := observability.NewLogger("info", &logBuf)

	r := chi.NewRouter()
	r.Use(middleware.CorrelationID(logger))
	r.Use(middleware.RequestLogger(logger))
	r.Get("/api/v1/notifications/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/notifications/1234", nil)
	req.Header.Set("X-Correlation-ID", "trace-req-9")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var entry map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logBuf.Bytes()), &entry))
	assert.Equal(t, "GET", entry["method"])
	assert.Equal(t, "/api/v1/notifications/{id}", entry["route"])
	assert.Equal(t, float64(200), entry["status"])
	assert.Equal(t, float64(11), entry["bytes"])
	assert.Equal(t, "trace-req-9", entry["correlation_id"])
	assert.Contains(t, entry, "duration_ms")
	assert.Equal(t, slog.LevelInfo.String(), entry["level"])
}

func TestCORS(t *testing.T) {
	t.Parallel()

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.CORS([]string{"http://localhost:8081"})(next)

	t.Run("preflight allowed origin", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/api/v1/notifications", nil)
		req.Header.Set("Origin", "http://localhost:8081")
		req.Header.Set("Access-Control-Request-Method", "POST")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, "http://localhost:8081", rec.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "Idempotent-Replayed, X-Correlation-ID", rec.Header().Get("Access-Control-Expose-Headers"))
		assert.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "Idempotency-Key")
		assert.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "X-Correlation-ID")
	})

	t.Run("disallowed origin receives no allow header", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/notifications", nil)
		req.Header.Set("Origin", "https://evil.example")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "Origin", rec.Header().Get("Vary"))
	})
}

func TestBodyLimit(t *testing.T) {
	t.Parallel()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			http.Error(w, "max bytes triggered", http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	h := middleware.BodyLimit(16, 64, "/api/v1/notifications/batch")(next)

	t.Run("single route blocks oversize content-length", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/notifications", strings.NewReader(strings.Repeat("a", 17)))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		assert.Contains(t, rec.Body.String(), "payload_too_large")
	})

	t.Run("batch route allows up to batch limit", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(strings.Repeat("b", 40)))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("chunked unknown content length enforces MaxBytesReader", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(strings.Repeat("c", 65)))
		req.ContentLength = -1
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})
}

func TestRequireJSON(t *testing.T) {
	t.Parallel()

	okHandler := middleware.RequireJSON()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name        string
		method      string
		contentType string
		body        string
		wantCode    int
	}{
		{name: "post json passes", method: http.MethodPost, contentType: "application/json; charset=utf-8", body: `{"a":1}`, wantCode: http.StatusOK},
		{name: "post text plain fails 415", method: http.MethodPost, contentType: "text/plain", body: `hello`, wantCode: http.StatusUnsupportedMediaType},
		{name: "post body without content-type fails 415", method: http.MethodPost, contentType: "", body: `{"a":1}`, wantCode: http.StatusUnsupportedMediaType},
		{name: "empty post without content-type passes for cancel/retry", method: http.MethodPost, contentType: "", body: "", wantCode: http.StatusOK},
		{name: "get passes without content-type", method: http.MethodGet, contentType: "", body: "", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}
			req := httptest.NewRequestWithContext(t.Context(), tc.method, "/test", bodyReader)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			okHandler.ServeHTTP(rec, req)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestTimeoutSetsContextDeadline(t *testing.T) {
	t.Parallel()

	h := middleware.Timeout(5 * time.Second)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadline, ok := r.Context().Deadline()
		assert.True(t, ok)
		assert.WithinDuration(t, time.Now().Add(5*time.Second), deadline, time.Second)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

type recordedHTTPCall struct {
	route  string
	method string
	status int
}

type fakeHTTPObserver struct {
	calls []recordedHTTPCall
}

func (f *fakeHTTPObserver) ObserveHTTPRequest(route, method string, status int, _ time.Duration) {
	f.calls = append(f.calls, recordedHTTPCall{route: route, method: method, status: status})
}

func TestRequestLoggerCorrelationIDAndMetricsRoutePattern(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := observability.NewLogger("info", &logBuf)
	obs := &fakeHTTPObserver{}

	r := chi.NewRouter()
	r.Use(middleware.CorrelationID(logger))
	r.Use(middleware.RequestLogger(logger))
	r.Use(middleware.Metrics(obs))
	r.Get("/api/v1/notifications/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/notifications/01936f7e-0001-7000-8000-000000000001", nil)
	req.Header.Set("X-Correlation-ID", "cid-trace-42")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, obs.calls, 1)
	assert.Equal(t, "/api/v1/notifications/{id}", obs.calls[0].route)
	assert.Equal(t, http.MethodGet, obs.calls[0].method)
	assert.Equal(t, http.StatusOK, obs.calls[0].status)

	var entry map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logBuf.Bytes()), &entry))
	assert.Equal(t, "cid-trace-42", entry["correlation_id"])
	assert.Equal(t, "/api/v1/notifications/{id}", entry["route"])
	assert.Equal(t, float64(http.StatusOK), entry["status"])
}
