package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/TITANBGG/notification-system/internal/domain"
)

func fieldCodes(ve *domain.ValidationError) map[string]string {
	out := map[string]string{}
	if ve == nil {
		return out
	}
	for _, f := range ve.Fields {
		if _, seen := out[f.Field]; !seen {
			out[f.Field] = f.Code
		}
	}
	return out
}

// NUL used to pass validation and fail at PostgreSQL with a 500; control characters are now a 422.
func TestValidateRejectsNULAndControlCharacters(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	subj := func(s string) *string { return &s }

	tests := []struct {
		name      string
		in        domain.CreateNotificationInput
		wantField string
		wantCode  string
	}{
		{"NUL in sms content", domain.CreateNotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi\x00there"}, "content", "invalid_character"},
		{"bell in push content", domain.CreateNotificationInput{Channel: "push", Recipient: "device-token", Content: "ding\a"}, "content", "invalid_character"},
		{"C1 control in email content", domain.CreateNotificationInput{Channel: "email", Recipient: "a@example.com", Content: "x\u0085y"}, "content", "invalid_character"},
		{"NUL in push token", domain.CreateNotificationInput{Channel: "push", Recipient: "tok\x00en", Content: "hi"}, "recipient", "invalid_character"},
		{"NUL in subject", domain.CreateNotificationInput{Channel: "email", Recipient: "a@example.com", Content: "hi", Subject: subj("a\x00b")}, "subject", "invalid_character"},
		{"CRLF in subject keeps its own code", domain.CreateNotificationInput{Channel: "email", Recipient: "a@example.com", Content: "hi", Subject: subj("a\r\nBcc: x")}, "subject", "invalid_subject"},
		{"dotless email domain", domain.CreateNotificationInput{Channel: "email", Recipient: "user@localhost", Content: "hi"}, "recipient", "invalid_email"},
		{"trailing-dot-only domain", domain.CreateNotificationInput{Channel: "email", Recipient: "user@gmailcom.", Content: "hi"}, "recipient", "invalid_email"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ve := domain.ValidateNotification(tc.in, now, domain.ValidationOptions{})
			assert.Equal(t, tc.wantCode, fieldCodes(ve)[tc.wantField])
		})
	}
}

func TestValidateKeepsLayoutCharactersInBodies(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	for _, in := range []domain.CreateNotificationInput{
		{Channel: "sms", Recipient: "+905551234567", Content: "line1\nline2\r\n\ttab \f form feed (GSM-7 extension)"},
		{Channel: "email", Recipient: "first.last@mail.example.co.uk", Content: "Hello,\n\nthanks"},
		{Channel: "push", Recipient: "device-token", Content: "Türkçe ğüşıöç 😀"},
	} {
		assert.Nil(t, domain.ValidateNotification(in, now, domain.ValidationOptions{}), "%q must stay valid", in.Content)
	}
}
