package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
)

// isAllowedWSOrigin answers refused origins with the JSON envelope before websocket.Accept runs, so
// it must decide exactly as the library does: if it allowed an origin the library refuses, the client
// would get the library's text/plain 403 again; if it refused one the library allows, it would break
// a working client. This compares both decisions over the same origins and patterns.
func TestIsAllowedWSOriginMatchesTheWebSocketLibrary(t *testing.T) {
	t.Parallel()
	const reqHost = "api.example:8080"
	patternSets := map[string][]string{
		"default":       buildWSOriginPatterns([]string{"http://localhost:8081"}, reqHost),
		"wildcard":      buildWSOriginPatterns([]string{"*"}, reqHost),
		"scheme+host":   {"https://app.example"},
		"host glob":     {"*.example.com"},
		"invalid glob":  {"[bad"},
		"no patterns":   nil,
		"mixed case":    {"App.Example:443"},
		"port wildcard": {"localhost:*"},
	}
	origins := []string{
		"", "null", "http://localhost:8081", "http://LOCALHOST:9999", "http://127.0.0.1:1",
		"https://evil.example", "http://api.example:8080", "https://APP.example", "http://app.example",
		"https://app.example:443", "https://a.example.com", "://malformed", "http://[::1]:8080", "file://",
	}
	for name, patterns := range patternSets {
		for _, origin := range origins {
			libAllowed := libraryAcceptsOrigin(t, reqHost, origin, patterns)
			assert.Equal(t, libAllowed, isAllowedWSOrigin(reqHost, origin, patterns),
				"patterns %s %q, origin %q: library allowed=%v", name, patterns, origin, libAllowed)
		}
	}
}

// libraryAcceptsOrigin runs the real websocket.Accept on a handshake with this Origin and reports
// whether it got past the origin check (anything but its 403).
func libraryAcceptsOrigin(t *testing.T, reqHost, origin string, patterns []string) bool {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+reqHost+"/api/v1/ws", nil)
	req.Host = reqHost
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rec := httptest.NewRecorder()
	// The recorder cannot be hijacked, so an accepted handshake fails after the origin check with a
	// different status; only 403 means the origin itself was refused.
	if c, err := websocket.Accept(rec, req, &websocket.AcceptOptions{OriginPatterns: patterns}); err == nil {
		_ = c.CloseNow()
	}
	return rec.Code != http.StatusForbidden
}
