package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
)

func TestRequestHashCanonicalAndDefaults(t *testing.T) {
	t.Parallel()

	// Different map key ordering produces identical SHA-256 hash.
	h1, err := domain.RequestHash(map[string]any{
		"recipient": "+905551234567",
		"channel":   "sms",
		"content":   "Siparişiniz kargoya verildi",
		"priority":  "normal",
		"nested":    map[string]any{"b": 2, "a": 1},
	})
	require.NoError(t, err)

	h2, err := domain.RequestHash(map[string]any{
		"priority":  "normal",
		"nested":    map[string]any{"a": 1, "b": 2},
		"content":   "Siparişiniz kargoya verildi",
		"channel":   "sms",
		"recipient": "+905551234567",
	})
	require.NoError(t, err)
	assert.Len(t, h1, 64)
	assert.Equal(t, h1, h2)

	// Priority omitted vs explicit "normal" produces identical hash.
	hOmitted, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hi",
	})
	require.NoError(t, err)

	hExplicit, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hi",
		Priority:  "normal",
	})
	require.NoError(t, err)
	assert.Equal(t, hOmitted, hExplicit)

	// Different priority changes hash.
	hHigh, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hi",
		Priority:  "high",
	})
	require.NoError(t, err)
	assert.NotEqual(t, hOmitted, hHigh)
}
