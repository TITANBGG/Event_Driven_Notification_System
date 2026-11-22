package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holdServer serves /api/v1/ws: "stream" sends an event every 10ms until the client goes away,
// "drop" closes after one event, "silent" never sends.
func holdServer(t *testing.T, mode string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx := c.CloseRead(r.Context())
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for sent := 0; ; sent++ {
			if mode == "drop" && sent == 1 {
				_ = c.Close(websocket.StatusPolicyViolation, "slow client")
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if mode == "silent" {
					continue
				}
				if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"status"}`)); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRunWSHold(t *testing.T) {
	t.Parallel()
	cases := []struct {
		mode    string
		wantErr string
	}{
		{"stream", ""},
		{"drop", "3 readers disconnected early"},
		{"silent", "3 received nothing"},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			srv := holdServer(t, tc.mode)
			var out bytes.Buffer
			err := runWSHold(t.Context(), srv.URL, "", 3, 3, 300*time.Millisecond, &out)
			if tc.wantErr == "" {
				require.NoError(t, err, out.String())
				assert.Contains(t, out.String(), "ws-hold: PASS")
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
