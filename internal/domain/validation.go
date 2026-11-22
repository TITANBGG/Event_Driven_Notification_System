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

// ValidateNotification evaluates all common, channel-specific, scheduling, and idempotency rules in one pass.
func ValidateNotification(in CreateNotificationInput, now time.Time, opts ValidationOptions) *ValidationError {
	return ValidateNotificationWithPrefix("", in, now, opts)
}

// ValidateNotificationWithPrefix validates an input while prefixing field paths (e.g., "notifications[3].").
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

	recipientBlank := strings.TrimSpace(in.Recipient) == ""
	if recipientBlank {
		ve.Add(joinField(prefix, "recipient"), "required", "recipient is required")
	}

	contentEmpty := strings.TrimSpace(in.Content) == ""
	if !opts.AllowEmptyContent && contentEmpty {
		ve.Add(joinField(prefix, "content"), "required", "content is required")
	}

	// PostgreSQL text cannot store NUL (it was a 500); other control characters have no place in a
	// message either and break log lines and provider payloads. Tab, newline, carriage return and
	// form feed (a GSM 03.38 extension character) stay allowed in bodies.
	checkText := func(field, value, allowed string) {
		if hasForbiddenControl(value, allowed) {
			ve.Add(joinField(prefix, field), "invalid_character", "must not contain NUL or other control characters")
		}
	}
	checkText("recipient", in.Recipient, "")
	checkText("content", in.Content, "\t\n\r\f")
	if in.Subject != nil {
		// CR/LF in a subject is reported by its own header-injection rule (invalid_subject).
		checkText("subject", *in.Subject, "\r\n")
	}
	if in.Title != nil {
		checkText("title", *in.Title, "")
	}

	if chErr == nil {
		switch channel {
		case ChannelSMS:
			validateSMS(prefix, in, recipientBlank, contentEmpty, ve)
		case ChannelEmail:
			validateEmail(prefix, in, recipientBlank, ve)
		case ChannelPush:
			validatePush(prefix, in, recipientBlank, opts.ExtendedProviderFields, ve)
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

func validateSMS(prefix string, in CreateNotificationInput, recipientBlank, contentEmpty bool, ve *ValidationError) {
	if !recipientBlank && !e164Pattern.MatchString(in.Recipient) {
		ve.Add(joinField(prefix, "recipient"), "invalid_e164", `must match ^\+[1-9]\d{7,14}$`)
	}
	if in.Subject != nil {
		ve.Add(joinField(prefix, "subject"), "unsupported_field", "subject is only supported for email channel")
	}
	if in.Title != nil {
		ve.Add(joinField(prefix, "title"), "unsupported_field", "title is only supported for push channel")
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

func validateEmail(prefix string, in CreateNotificationInput, recipientBlank bool, ve *ValidationError) {
	if !recipientBlank {
		if utf8.RuneCountInString(in.Recipient) > maxEmailRecipientRunes {
			ve.Add(joinField(prefix, "recipient"), "invalid_email", "email recipient exceeds 254 characters")
		} else {
			parsed, err := mail.ParseAddress(in.Recipient)
			// Require exact equality so display names ("Alice <a@b.com>"), angle brackets, or extra spaces fail.
			if err != nil || parsed.Address != in.Recipient {
				ve.Add(joinField(prefix, "recipient"), "invalid_email", "must be a valid email address without a display name")
			} else if domain := in.Recipient[strings.LastIndex(in.Recipient, "@")+1:]; !strings.Contains(strings.Trim(domain, "."), ".") {
				// net/mail accepts RFC 5322 dot-atoms such as "user@localhost"; no public mailbox lives
				// on a single-label domain, so require a dot to catch typos like "user@gmailcom".
				ve.Add(joinField(prefix, "recipient"), "invalid_email", "email domain must contain a dot (e.g. example.com)")
			}
		}
	}

	if in.Title != nil {
		ve.Add(joinField(prefix, "title"), "unsupported_field", "title is only supported for push channel")
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

func validatePush(prefix string, in CreateNotificationInput, recipientBlank bool, extendedFields bool, ve *ValidationError) {
	if !recipientBlank {
		runeLen := utf8.RuneCountInString(in.Recipient)
		if runeLen < 1 || runeLen > maxPushTokenRunes || containsWhitespace(in.Recipient) {
			ve.Add(joinField(prefix, "recipient"), "invalid_device_token", "device token must be 1..4096 characters with no whitespace")
		}
	}

	if in.Subject != nil {
		ve.Add(joinField(prefix, "subject"), "unsupported_field", "subject is only supported for email channel")
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

// IsValidIdempotencyKey verifies that k consists of 1..255 printable ASCII bytes.
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

// hasForbiddenControl reports NUL, C0/C1 control characters and DEL, except the characters in
// allowed (message bodies may contain tab, newline, carriage return and form feed).
func hasForbiddenControl(s, allowed string) bool {
	for _, r := range s {
		if unicode.IsControl(r) && !strings.ContainsRune(allowed, r) {
			return true
		}
	}
	return false
}
