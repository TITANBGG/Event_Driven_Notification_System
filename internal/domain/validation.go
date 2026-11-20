package domain

import (
	"encoding/json"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxEmailRecipientRunes = 254
	maxEmailSubjectRunes   = 200
	maxEmailContentBytes   = 102400 // 100 KB

	maxPushTokenRunes   = 4096
	maxPushTitleRunes   = 100
	maxPushContentRunes = 2000
	maxPushPayloadBytes = 4096

	maxIdempotencyKeyLen = 255
)

var e164Pattern = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)

// ValidateNotification checks all common, channel-specific, scheduling, and idempotency rules.
//
// WHY: Collecting every validation failure in one pass produces clear, actionable 422 details
// without leaking downstream invalid payloads to Redis or external providers.
//
// Expected output example:
//
//	ValidateNotification(badSMS, now, opts).Fields -> [
//	  {Field: "recipient", Code: "invalid_e164", Message: "must match ^\\+[1-9]\\d{7,14}$"},
//	  {Field: "content", Code: "too_many_segments", Message: "11 segments (GSM-7), max 10"},
//	]
func ValidateNotification(in CreateNotificationInput, now time.Time, opts ValidationOptions) *ValidationError {
	return ValidateNotificationWithPrefix("", in, now, opts)
}

// ValidateNotificationWithPrefix validates an input while prefixing field paths (e.g. "notifications[3].").
func ValidateNotificationWithPrefix(prefix string, in CreateNotificationInput, now time.Time, opts ValidationOptions) *ValidationError {
	ve := &ValidationError{}

	channel, chErr := ParseChannel(in.Channel)
	if strings.TrimSpace(in.Channel) == "" {
		ve.Add(joinField(prefix, "channel"), "required", "channel is required")
	} else if chErr != nil {
		ve.Add(joinField(prefix, "channel"), "invalid_channel", "must be one of sms, email, push")
	}

	if strings.TrimSpace(in.Priority) != "" {
		if _, err := ParsePriority(in.Priority); err != nil {
			ve.Add(joinField(prefix, "priority"), "invalid_priority", "must be one of high, normal, low")
		}
	}

	recipient := strings.TrimSpace(in.Recipient)
	if recipient == "" {
		ve.Add(joinField(prefix, "recipient"), "required", "recipient is required")
	}

	contentEmpty := strings.TrimSpace(in.Content) == ""
	if !opts.AllowEmptyContent && contentEmpty {
		ve.Add(joinField(prefix, "content"), "required", "content is required")
	}

	if chErr == nil {
		switch channel {
		case ChannelSMS:
			validateSMS(prefix, in, recipient, contentEmpty, ve)
		case ChannelEmail:
			validateEmail(prefix, in, recipient, ve)
		case ChannelPush:
			validatePush(prefix, in, recipient, opts.ExtendedProviderFields, ve)
		}
	}

	if opts.CheckScheduledAt && in.ScheduledAt != nil {
		validateScheduledAt(prefix, *in.ScheduledAt, now, opts, ve)
	}

	if in.IdempotencyKey != nil && *in.IdempotencyKey != "" {
		if !IsValidIdempotencyKey(*in.IdempotencyKey) {
			ve.Add(joinField(prefix, "idempotencyKey"), "invalid_idempotency_key", "must be 1..255 printable ASCII characters")
		}
	}

	if !ve.HasErrors() {
		return nil
	}
	return ve
}

func validateSMS(prefix string, in CreateNotificationInput, recipient string, contentEmpty bool, ve *ValidationError) {
	if recipient != "" && !e164Pattern.MatchString(in.Recipient) {
		ve.Add(joinField(prefix, "recipient"), "invalid_e164", `must match ^\+[1-9]\d{7,14}$`)
	}
	if !contentEmpty {
		info := SMSInfo(in.Content)
		if info.Segments > MaxSMSSegments {
			ve.Add(
				joinField(prefix, "content"),
				"too_many_segments",
				fmt.Sprintf("%d segments (%s), max %d", info.Segments, info.Encoding, MaxSMSSegments),
			)
		}
	}
}

func validateEmail(prefix string, in CreateNotificationInput, recipient string, ve *ValidationError) {
	if recipient != "" {
		if utf8.RuneCountInString(in.Recipient) > maxEmailRecipientRunes {
			ve.Add(joinField(prefix, "recipient"), "invalid_email", "email recipient exceeds 254 characters")
		} else {
			parsed, err := mail.ParseAddress(in.Recipient)
			// Require exact equality so display names ("Alice <a@b.com>") and angle brackets are rejected.
			if err != nil || parsed.Address != in.Recipient {
				ve.Add(joinField(prefix, "recipient"), "invalid_email", "must be a valid email address without a display name")
			}
		}
	}

	if in.Subject != nil {
		subj := *in.Subject
		if strings.ContainsAny(subj, "\r\n") {
			ve.Add(joinField(prefix, "subject"), "invalid_subject", "subject must not contain CR or LF characters")
		}
		if utf8.RuneCountInString(subj) > maxEmailSubjectRunes {
			ve.Add(joinField(prefix, "subject"), "subject_too_long", "subject exceeds 200 characters")
		}
	}

	if len(in.Content) > maxEmailContentBytes {
		ve.Add(joinField(prefix, "content"), "content_too_large", "email content exceeds 102400 bytes")
	}
}

func validatePush(prefix string, in CreateNotificationInput, recipient string, extendedFields bool, ve *ValidationError) {
	if recipient != "" {
		runeLen := utf8.RuneCountInString(in.Recipient)
		if runeLen < 1 || runeLen > maxPushTokenRunes || containsWhitespace(in.Recipient) {
			ve.Add(joinField(prefix, "recipient"), "invalid_device_token", "device token must be 1..4096 characters with no whitespace")
		}
	}

	if in.Title != nil && utf8.RuneCountInString(*in.Title) > maxPushTitleRunes {
		ve.Add(joinField(prefix, "title"), "title_too_long", "push title exceeds 100 characters")
	}

	if utf8.RuneCountInString(in.Content) > maxPushContentRunes {
		ve.Add(joinField(prefix, "content"), "content_too_long", "push content exceeds 2000 characters")
	}

	payload := struct {
		To      string  `json:"to"`
		Channel string  `json:"channel"`
		Content string  `json:"content"`
		Title   *string `json:"title,omitempty"`
	}{
		To:      in.Recipient,
		Channel: string(ChannelPush),
		Content: in.Content,
	}
	if extendedFields && in.Title != nil && *in.Title != "" {
		payload.Title = in.Title
	}
	if encoded, err := json.Marshal(payload); err == nil && len(encoded) > maxPushPayloadBytes {
		ve.Add(joinField(prefix, "content"), "payload_too_large", fmt.Sprintf("serialized push payload is %d bytes, max %d", len(encoded), maxPushPayloadBytes))
	}
}

func validateScheduledAt(prefix string, scheduledAt, now time.Time, opts ValidationOptions, ve *ValidationError) {
	skew := opts.ClockSkewTolerance
	maxAhead := opts.ScheduleMaxAhead
	if maxAhead <= 0 {
		maxAhead = 720 * time.Hour
	}
	minAllowed := now.UTC().Add(-skew)
	maxAllowed := now.UTC().Add(maxAhead)
	target := scheduledAt.UTC()

	if !target.After(minAllowed) {
		ve.Add(joinField(prefix, "scheduledAt"), "scheduled_in_past", "scheduledAt must be in the future")
		return
	}
	if target.After(maxAllowed) {
		ve.Add(joinField(prefix, "scheduledAt"), "scheduled_too_far", fmt.Sprintf("scheduledAt exceeds max horizon (%s)", maxAhead))
	}
}

// IsValidIdempotencyKey verifies that k is 1..255 printable ASCII characters (0x20..0x7E).
func IsValidIdempotencyKey(k string) bool {
	if len(k) < 1 || len(k) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7E {
			return false
		}
	}
	return true
}

func containsWhitespace(s string) bool {
	for _, r := range s {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

func joinField(prefix, field string) string {
	if prefix == "" {
		return field
	}
	return prefix + field
}
