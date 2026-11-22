package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
	"github.com/TITANBGG/notification-system/internal/realtime"
)

// Whatever a client sends, the API must answer with a documented 4xx (or a 2xx) and the standard error
// envelope; a 5xx here would be a bug, since the fake store never fails.
func FuzzAPINeverReturns5xxForClientInput(f *testing.F) {
	seeds := []struct {
		route uint8
		body  string
		key   string
		query string
	}{
		{0, `{"recipient":"+905551234567","channel":"sms","content":"hi"}`, "", ""},
		{0, `{"recipient":"a@example.com","channel":"email","subject":"s","content":"hi","scheduledAt":"2026-11-21T11:00:00Z"}`, "k-1", ""},
		{1, `{"notifications":[{"recipient":"tok","channel":"push","title":"t","content":"x"}]}`, "b-1", ""},
		{2, `{"name":"t","channel":"sms","body":"Hi {{name}}"}`, "", ""},
		{3, ``, "", "status=failed,pending&channel=sms&limit=10&from=2026-11-01T00:00:00Z"},
		{4, ``, "", ""},
		{6, ``, "", "channel=sms"},
	}
	for _, s := range seeds {
		f.Add(s.route, s.body, s.key, s.query)
	}

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	if err != nil {
		f.Fatal(err)
	}
	now := func() time.Time { return time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC) }
	// A running Hub, as in production; without it /api/v1/ws answers 503 by design.
	hubCtx, stopHub := context.WithCancel(context.Background())
	hub := realtime.NewHub(realtime.HubConfig{Logger: observability.NewLogger("error", &bytes.Buffer{})})
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Run(hubCtx) }()
	f.Cleanup(func() {
		stopHub()
		if err := <-hubDone; err != nil {
			f.Errorf("hub stopped with error: %v", err)
		}
	})

	f.Fuzz(func(t *testing.T, route uint8, body, key, query string) {
		// A fresh store per input keeps memory flat over millions of executions.
		store := newFakeStore()
		store.now = now
		h := api.NewRouter(api.Deps{
			Config: cfg, Logger: observability.NewLogger("error", &bytes.Buffer{}),
			Store: store, Enqueuer: api.NewMemoryEnqueuer(), Hub: hub, Now: now,
		})
		const someID = "01936f7e-0000-7000-8000-000000000001"
		var method, path string
		switch route % 7 {
		case 0:
			method, path = http.MethodPost, "/api/v1/notifications"
		case 1:
			method, path = http.MethodPost, "/api/v1/notifications/batch"
		case 2:
			method, path = http.MethodPost, "/api/v1/templates"
		case 3:
			method, path = http.MethodGet, "/api/v1/notifications"
		case 4:
			method, path = http.MethodPost, "/api/v1/notifications/"+someID+"/cancel"
		case 5:
			method, path = http.MethodPost, "/api/v1/notifications/"+someID+"/retry"
		case 6:
			method, path = http.MethodGet, "/api/v1/ws" // plain GET: filter validation runs before the upgrade
		}
		target := path
		if q := strings.Map(func(r rune) rune {
			if r < 0x21 || r == '#' || r > 0x7e {
				return -1 // keep the request line parseable; the router never sees such bytes
			}
			return r
		}, query); q != "" {
			target += "?" + q
		}
		req, reqErr := http.NewRequestWithContext(t.Context(), method, "http://api.test"+target, strings.NewReader(body))
		if reqErr != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if key != "" && !strings.ContainsAny(key, "\r\n\x00") {
			req.Header.Set("Idempotency-Key", key)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code >= 500 {
			t.Fatalf("%s %s with body %q -> %d: %s", method, target, body, rec.Code, rec.Body.String())
		}
		if rec.Code >= 400 {
			var env struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || env.Error.Code == "" {
				t.Fatalf("%s %s -> %d without the error envelope: %q", method, target, rec.Code, rec.Body.String())
			}
		}
	})
}
