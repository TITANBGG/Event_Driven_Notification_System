package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TITANBGG/notification-system/internal/api/middleware"
)

func TestRequireStaticBearer(t *testing.T) {
	t.Parallel()
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	const token = "scrape-token-0123456789"

	for _, tc := range []struct {
		name, configured, header string
		want                     int
	}{
		{"disabled: open", "", "", http.StatusOK},
		{"missing", token, "", http.StatusUnauthorized},
		{"wrong", token, "Bearer scrape-token-wrong-000000", http.StatusUnauthorized},
		{"right", token, "Bearer " + token, http.StatusOK},
	} {
		h := middleware.RequireStaticBearer(tc.configured)(ok)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, tc.want, rec.Code, tc.name)
		assert.NotContains(t, rec.Body.String(), token, tc.name)
	}
}
