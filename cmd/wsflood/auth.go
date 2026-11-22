package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// defaultAPIKey is the local-dev admin key docker-compose.yml enables by default.
//
//nolint:gosec // G101: the public local-dev key that docker-compose.yml enables, documented in README; not a secret.
const defaultAPIKey = "local-dev-key-change-me"

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

// bearerTransport adds Authorization: Bearer <key> to every request that does not set its own,
// so a boundary case can still send a deliberately wrong key.
type bearerTransport struct {
	base http.RoundTripper
	key  string
}

func newBearerTransport(base http.RoundTripper, key string) http.RoundTripper {
	if key == "" {
		return base
	}
	return bearerTransport{base: base, key: key}
}

func (t bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Authorization") != "" {
		return t.base.RoundTrip(r)
	}
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+t.key)
	return t.base.RoundTrip(r)
}

// authHeader returns the WebSocket handshake headers for apiKey (nil when auth is not used).
func authHeader(apiKey string) http.Header {
	if apiKey == "" {
		return nil
	}
	return http.Header{"Authorization": []string{"Bearer " + apiKey}}
}

// checkUnknownAPIKeyRejected verifies that an unknown key is refused with the standard 401 envelope.
func checkUnknownAPIKeyRejected(ctx context.Context, hc *http.Client, baseURL string, out io.Writer) int {
	const name = "Unknown API key -> 401 unauthorized"
	code, body, err := doRequest(ctx, hc, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/notifications?limit=1", nil,
		map[string]string{"Authorization": "Bearer wsflood-unknown-key-0000000000"})
	if err != nil {
		_, _ = fmt.Fprintf(out, "FAIL  %-50s request error: %v\n", name, err)
		return 1
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &env)
	if code != http.StatusUnauthorized || env.Error.Code != "unauthorized" {
		_, _ = fmt.Fprintf(out, "FAIL  %-50s got HTTP %d code=%q want 401 unauthorized\n", name, code, env.Error.Code)
		return 1
	}
	_, _ = fmt.Fprintf(out, "PASS  %-50s HTTP %d\n", name, code)
	return 0
}
