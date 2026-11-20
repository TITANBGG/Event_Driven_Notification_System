package domain_test

import (
	"testing"
	"time"

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

	// Priority omitted vs explicit "normal", and uppercase vs lowercase channel/priority produce identical hashes.
	hOmitted, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hi",
	})
	require.NoError(t, err)

	hExplicit, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient: "+905551234567",
		Channel:   " SMS ",
		Content:   "hi",
		Priority:  "NORMAL",
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

func TestRequestHashScheduledAtTimezoneAndBatchDefaults(t *testing.T) {
	t.Parallel()

	ist := time.FixedZone("TRT", 3*3600)
	tIST := time.Date(2026, 11, 21, 13, 0, 0, 0, ist)
	tUTC := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	hIST, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient:   "+905551234567",
		Channel:     "SMS",
		Content:     "scheduled",
		ScheduledAt: &tIST,
	})
	require.NoError(t, err)

	hUTC, err := domain.RequestHash(domain.CreateNotificationInput{
		Recipient:   "+905551234567",
		Channel:     "sms",
		Content:     "scheduled",
		Priority:    "normal",
		ScheduledAt: &tUTC,
	})
	require.NoError(t, err)
	assert.Equal(t, hIST, hUTC, "equivalent instants in different offsets must hash identically")

	// Batch wrapper applies item defaults, lowercase normalization, and UTC timestamps.
	b1 := domain.BatchCreateInput{
		Notifications: []domain.CreateNotificationInput{
			{Recipient: "+905551234567", Channel: "SMS", Content: "a", ScheduledAt: &tIST},
			{Recipient: "ali@example.com", Channel: "EMAIL", Content: "b", Priority: "HIGH"},
		},
	}
	b2 := domain.BatchCreateInput{
		Notifications: []domain.CreateNotificationInput{
			{Recipient: "+905551234567", Channel: "sms", Content: "a", Priority: "normal", ScheduledAt: &tUTC},
			{Recipient: "ali@example.com", Channel: "email", Content: "b", Priority: "high"},
		},
	}
	hb1, err := domain.RequestHash(b1)
	require.NoError(t, err)
	hb2, err := domain.RequestHash(b2)
	require.NoError(t, err)
	assert.Equal(t, hb1, hb2)
}
