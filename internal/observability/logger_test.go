package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/observability"
)

func TestMaskPhone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: ""},
		{in: "1234", want: "***"},
		{in: "5551234", want: "55***34"},
		{in: "+905551234567", want: "+90555***4567"},
		{in: " +905551234567 ", want: "+90555***4567"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, observability.MaskPhone(tc.in))
		})
	}
}

func TestMaskEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: ""},
		{in: "invalid-email", want: "***"},
		{in: "@example.com", want: "***"},
		{in: "a@example.com", want: "a***@example.com"},
		{in: "ali.nebi@example.com", want: "a***@example.com"},
		{in: "şükrü@example.com", want: "ş***@example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, observability.MaskEmail(tc.in))
		})
	}
}

func TestMaskToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: ""},
		{in: "short", want: "***"},
		{in: "12345678", want: "***"},
		{in: "fcm_abcdef1234567890xyz", want: "fcm_***0xyz"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, observability.MaskToken(tc.in))
		})
	}
}

func TestMaskDatabaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{
			in:   "postgres://notif:secret123@postgres:5432/notif?sslmode=disable",
			want: "postgres://notif:***@postgres:5432/notif?sslmode=disable",
		},
		{
			in:   "postgres://notif@postgres:5432/notif",
			want: "postgres://notif@postgres:5432/notif",
		},
		{
			in:   "not-a-dsn",
			want: "***",
		},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, observability.MaskDatabaseURL(tc.in))
		})
	}
}

func TestLoggerContextEnrichmentAndLevels(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := observability.NewLogger("warn", &buf)

	ctx := observability.WithLogger(context.Background(), base)
	ctx = observability.WithCorrelationID(ctx, "corr-abc-123")
	ctx = observability.WithFields(ctx,
		slog.String("notification_id", "01936f7e-0001"),
		slog.String("channel", "sms"),
	)

	assert.Equal(t, "corr-abc-123", observability.CorrelationID(ctx))

	// Info is filtered when level=warn.
	observability.Logger(ctx).Info("should be filtered out")
	assert.Empty(t, buf.String())

	// Warn is emitted with correlation_id and context fields.
	observability.Logger(ctx).Warn("transient provider timeout", slog.Int("attempt", 2))

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "transient provider timeout", record["msg"])
	assert.Equal(t, "corr-abc-123", record["correlation_id"])
	assert.Equal(t, "01936f7e-0001", record["notification_id"])
	assert.Equal(t, "sms", record["channel"])
	assert.Equal(t, float64(2), record["attempt"])
}
