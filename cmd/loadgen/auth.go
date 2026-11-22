package main

import (
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

// bearerTransport adds Authorization: Bearer <key> to every request that does not set its own.
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
