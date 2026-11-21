package provider

import (
	"testing"
	"time"
)

// Retry-After comes from the provider, so any header value must yield either no hint or a delay that is
// never negative (a negative delay would make the worker retry immediately in a loop).
func FuzzParseRetryAfterIsNeverNegative(f *testing.F) {
	for _, seed := range []string{"", "1", "120", "-5", " 30 ", "Wed, 21 Oct 2026 07:28:00 GMT", "soon"} {
		f.Add(seed)
	}
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, raw string) {
		d := parseRetryAfter(raw, now)
		if d != nil && *d < 0 {
			t.Fatalf("parseRetryAfter(%q) = %v, want >= 0", raw, *d)
		}
	})
}
