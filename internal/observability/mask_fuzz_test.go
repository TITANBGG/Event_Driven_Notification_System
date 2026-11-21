package observability_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/TITANBGG/notification-system/internal/observability"
)

// Masked recipients go into every log line, so the masking must hold for any input, not only for the
// well-formed examples: the raw value never appears, and phone numbers and push tokens keep at least
// three characters hidden (an email keeps its domain by design but never its whole local part).
func FuzzMaskRecipientNeverRevealsTheRawValue(f *testing.F) {
	for _, seed := range [][2]string{
		{"sms", "+905551234567"},
		{"email", "ali@example.com"},
		{"push", "device-token-0123456789abcdef"},
		{"SMS", "  +491701234567  "},
		{"unknown", "something"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, channel, recipient string) {
		masked := observability.MaskRecipient(channel, recipient)
		raw := strings.TrimSpace(recipient)
		if raw == "" || !utf8.ValidString(raw) {
			return // masking works on runes; invalid UTF-8 is rejected by validation before it is logged
		}
		n := utf8.RuneCountInString(raw)
		// A raw value that itself contains '*' can coincide with the "***" marker, so skip the substring check.
		if n > 1 && !strings.Contains(raw, "*") && strings.Contains(masked, raw) {
			t.Fatalf("masked %q still contains the raw value %q", masked, raw)
		}

		if strings.EqualFold(strings.TrimSpace(channel), "email") {
			// Exact shape: first rune of the local part, "***", then the domain; nothing else of the local part.
			if at := strings.IndexByte(raw, '@'); at > 0 && at < len(raw)-1 {
				first, _ := utf8.DecodeRuneInString(raw)
				if want := string(first) + "***" + raw[at:]; masked != want {
					t.Fatalf("masked %q, want %q for %q", masked, want, raw)
				}
			} else if masked != "***" {
				t.Fatalf("email without a usable '@' must mask fully, got %q for %q", masked, raw)
			}
			return
		}
		// Phone and token masks keep a prefix and a suffix around "***".
		kept := utf8.RuneCountInString(masked) - 3
		hidden := n - kept
		if want := min(3, n); hidden < want {
			t.Fatalf("masked %q hides only %d of %d characters of %q (want >= %d)", masked, hidden, n, raw, want)
		}
	})
}
