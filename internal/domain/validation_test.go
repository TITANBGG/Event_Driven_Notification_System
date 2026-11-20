package domain_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
)

func strPtr(s string) *string { return &s }

func TestValidateSMSRecipientAndSegments(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		recipient string
		content   string
		wantOK    bool
		wantCode  string
	}{
		{name: "valid Turkey E.164", recipient: "+905551234567", content: "hello", wantOK: true},
		{name: "valid US E.164", recipient: "+14155552671", content: "hello", wantOK: true},
		{name: "leading whitespace rejected", recipient: " +905551234567", content: "hello", wantCode: "invalid_e164"},
		{name: "trailing whitespace rejected", recipient: "+905551234567 ", content: "hello", wantCode: "invalid_e164"},
		{name: "missing leading plus", recipient: "905551234567", content: "hello", wantCode: "invalid_e164"},
		{name: "local leading zero", recipient: "05551234567", content: "hello", wantCode: "invalid_e164"},
		{name: "plus zero invalid", recipient: "+05551234567", content: "hello", wantCode: "invalid_e164"},
		{name: "too short (7 digits)", recipient: "+1234567", content: "hello", wantCode: "invalid_e164"},
		{name: "too long (16 digits)", recipient: "+1234567890123456", content: "hello", wantCode: "invalid_e164"},
		{name: "whitespace rejected", recipient: "+90 555 123 4567", content: "hello", wantCode: "invalid_e164"},
		{name: "10 segments GSM-7 valid", recipient: "+905551234567", content: strings.Repeat("a", 1530), wantOK: true},
		{name: "11 segments GSM-7 rejected", recipient: "+905551234567", content: strings.Repeat("a", 1531), wantCode: "too_many_segments"},
		{name: "10 segments UCS-2 valid", recipient: "+905551234567", content: strings.Repeat("ş", 670), wantOK: true},
		{name: "11 segments UCS-2 rejected", recipient: "+905551234567", content: strings.Repeat("ş", 671), wantCode: "too_many_segments"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ve := domain.ValidateNotification(domain.CreateNotificationInput{
				Recipient: tc.recipient,
				Channel:   "sms",
				Content:   tc.content,
			}, now, domain.ValidationOptions{})

			if tc.wantOK {
				assert.Nil(t, ve)
				return
			}
			require.NotNil(t, ve)
			assert.Equal(t, tc.wantCode, ve.Fields[0].Code)
		})
	}
}

func TestValidateEmail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		in       domain.CreateNotificationInput
		wantOK   bool
		wantCode string
	}{
		{
			name:   "valid email without subject",
			in:     domain.CreateNotificationInput{Recipient: "ali@example.com", Channel: "email", Content: "Welcome"},
			wantOK: true,
		},
		{
			name:     "leading space rejected",
			in:       domain.CreateNotificationInput{Recipient: " ali@example.com", Channel: "email", Content: "Welcome"},
			wantCode: "invalid_email",
		},
		{
			name:     "display name rejected",
			in:       domain.CreateNotificationInput{Recipient: "Ali <ali@example.com>", Channel: "email", Content: "Welcome"},
			wantCode: "invalid_email",
		},
		{
			name:     "angle bracket address rejected",
			in:       domain.CreateNotificationInput{Recipient: "<ali@example.com>", Channel: "email", Content: "Welcome"},
			wantCode: "invalid_email",
		},
		{
			name:     "exceeds 254 chars",
			in:       domain.CreateNotificationInput{Recipient: strings.Repeat("a", 245) + "@example.com", Channel: "email", Content: "Welcome"},
			wantCode: "invalid_email",
		},
		{
			name:     "subject with CR/LF rejected",
			in:       domain.CreateNotificationInput{Recipient: "ali@example.com", Channel: "email", Subject: strPtr("Hi\r\nBcc:evil@x.com"), Content: "Welcome"},
			wantCode: "invalid_subject",
		},
		{
			name:     "subject exceeds 200 chars",
			in:       domain.CreateNotificationInput{Recipient: "ali@example.com", Channel: "email", Subject: strPtr(strings.Repeat("x", 201)), Content: "Welcome"},
			wantCode: "subject_too_long",
		},
		{
			name:   "content exactly 100KB allowed",
			in:     domain.CreateNotificationInput{Recipient: "ali@example.com", Channel: "email", Content: strings.Repeat("a", 102400)},
			wantOK: true,
		},
		{
			name:     "content 100KB+1 rejected",
			in:       domain.CreateNotificationInput{Recipient: "ali@example.com", Channel: "email", Content: strings.Repeat("a", 102401)},
			wantCode: "content_too_large",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ve := domain.ValidateNotification(tc.in, now, domain.ValidationOptions{})
			if tc.wantOK {
				assert.Nil(t, ve)
				return
			}
			require.NotNil(t, ve)
			assert.Equal(t, tc.wantCode, ve.Fields[0].Code)
		})
	}
}

func TestValidateChannelInappropriateFieldsRejected(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		in        domain.CreateNotificationInput
		wantField string
	}{
		{
			name:      "sms rejects subject",
			in:        domain.CreateNotificationInput{Recipient: "+905551234567", Channel: "sms", Content: "hi", Subject: strPtr("S")},
			wantField: "subject",
		},
		{
			name:      "sms rejects title",
			in:        domain.CreateNotificationInput{Recipient: "+905551234567", Channel: "sms", Content: "hi", Title: strPtr("T")},
			wantField: "title",
		},
		{
			name:      "email rejects title",
			in:        domain.CreateNotificationInput{Recipient: "ali@example.com", Channel: "email", Content: "hi", Title: strPtr("T")},
			wantField: "title",
		},
		{
			name:      "push rejects subject",
			in:        domain.CreateNotificationInput{Recipient: "device_tok_123", Channel: "push", Content: "hi", Subject: strPtr("S")},
			wantField: "subject",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ve := domain.ValidateNotification(tc.in, now, domain.ValidationOptions{})
			require.NotNil(t, ve)
			assert.Equal(t, tc.wantField, ve.Fields[0].Field)
			assert.Equal(t, "unsupported_field", ve.Fields[0].Code)
		})
	}
}

func TestValidatePushAndPayloadLimit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)

	// Token with space rejected.
	ve := domain.ValidateNotification(domain.CreateNotificationInput{
		Recipient: "device token 123",
		Channel:   "push",
		Content:   "Ping",
	}, now, domain.ValidationOptions{})
	require.NotNil(t, ve)
	assert.Equal(t, "invalid_device_token", ve.Fields[0].Code)

	// Construct exact 4096-byte vs 4097-byte serialized push payload {"to":"...","channel":"push","content":"..."}.
	baseOverhead, err := json.Marshal(struct {
		To      string `json:"to"`
		Channel string `json:"channel"`
		Content string `json:"content"`
	}{To: "", Channel: "push", Content: ""})
	require.NoError(t, err)

	remaining := 4096 - len(baseOverhead)
	tokenLen := remaining - 100
	contentLen := 100

	veOK := domain.ValidateNotification(domain.CreateNotificationInput{
		Recipient: strings.Repeat("t", tokenLen),
		Channel:   "push",
		Content:   strings.Repeat("c", contentLen),
	}, now, domain.ValidationOptions{})
	assert.Nil(t, veOK, "exact 4096-byte push payload must succeed")

	veTooBig := domain.ValidateNotification(domain.CreateNotificationInput{
		Recipient: strings.Repeat("t", tokenLen),
		Channel:   "push",
		Content:   strings.Repeat("c", contentLen+1),
	}, now, domain.ValidationOptions{})
	require.NotNil(t, veTooBig)
	assert.Equal(t, "payload_too_large", veTooBig.Fields[0].Code)
}

func TestValidateMultipleErrorsBatchPrefixScheduledAndIdempotency(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC)

	// Multiple errors aggregated with batch prefix.
	ve := domain.ValidateNotificationWithPrefix("notifications[3].", domain.CreateNotificationInput{
		Recipient:      "05551234567",
		Channel:        "sms",
		Priority:       "urgent",
		Content:        strings.Repeat("ş", 671),
		IdempotencyKey: strPtr("bad\nkey"),
	}, now, domain.ValidationOptions{})
	require.NotNil(t, ve)
	assert.Len(t, ve.Fields, 4)
	assert.Equal(t, "request has 4 invalid fields", ve.Error())

	fields := make(map[string]string)
	for _, f := range ve.Fields {
		fields[f.Field] = f.Code
	}
	assert.Equal(t, "invalid_priority", fields["notifications[3].priority"])
	assert.Equal(t, "invalid_e164", fields["notifications[3].recipient"])
	assert.Equal(t, "too_many_segments", fields["notifications[3].content"])
	assert.Equal(t, "invalid_idempotency_key", fields["notifications[3].idempotencyKey"])

	// ScheduledAt validation boundaries.
	opts := domain.ValidationOptions{
		CheckScheduledAt:   true,
		ClockSkewTolerance: 2 * time.Second,
		ScheduleMaxAhead:   720 * time.Hour,
	}
	past := now.Add(-3 * time.Second)
	require.NotNil(t, domain.ValidateNotification(domain.CreateNotificationInput{
		Recipient: "+905551234567", Channel: "sms", Content: "hi", ScheduledAt: &past,
	}, now, opts))

	withinSkew := now.Add(-1 * time.Second)
	assert.Nil(t, domain.ValidateNotification(domain.CreateNotificationInput{
		Recipient: "+905551234567", Channel: "sms", Content: "hi", ScheduledAt: &withinSkew,
	}, now, opts))

	tooFar := now.Add(721 * time.Hour)
	veFar := domain.ValidateNotification(domain.CreateNotificationInput{
		Recipient: "+905551234567", Channel: "sms", Content: "hi", ScheduledAt: &tooFar,
	}, now, opts)
	require.NotNil(t, veFar)
	assert.Equal(t, "scheduled_too_far", veFar.Fields[0].Code)
}
