package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    int
		delay     time.Duration
		timeout   time.Duration
		overrideU string
		wantErr   bool
	}{
		{name: "200 ok", status: http.StatusOK, timeout: time.Second, wantErr: false},
		{name: "204 no content", status: http.StatusNoContent, timeout: time.Second, wantErr: false},
		{name: "404 not found", status: http.StatusNotFound, timeout: time.Second, wantErr: true},
		{name: "503 unavailable", status: http.StatusServiceUnavailable, timeout: time.Second, wantErr: true},
		{name: "timeout exceeded", status: http.StatusOK, delay: 150 * time.Millisecond, timeout: 30 * time.Millisecond, wantErr: true},
		{name: "invalid scheme", overrideU: "ftp://localhost:8080/health", timeout: time.Second, wantErr: true},
		{name: "empty url", overrideU: "", timeout: time.Second, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := tc.overrideU
			if tc.overrideU == "" && tc.name != "empty url" {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if tc.delay > 0 {
						timer := time.NewTimer(tc.delay)
						select {
						case <-timer.C:
						case <-r.Context().Done():
							timer.Stop()
							return
						}
					}
					w.WriteHeader(tc.status)
				}))
				defer srv.Close()
				target = srv.URL + "/health/live"
			}

			ctx, cancel := context.WithTimeout(t.Context(), tc.timeout)
			defer cancel()

			err := probe(ctx, &http.Client{Timeout: tc.timeout}, target)
			if tc.wantErr {
				require.Error(t, err)
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
