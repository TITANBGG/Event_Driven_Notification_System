package domain_test

import (
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
)

func FuzzSMSInfo(f *testing.F) {
	seeds := []string{
		"",
		"Hello world",
		"Siparisiniz kargoya verildi",
		"Siparişiniz kargoya verildi 🚀",
		"€€€€€€€€€€€€€€€€€€€€",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",  // 160 GSM-7
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 161 GSM-7
		"😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀",                                                                                                                               // 70 UTF-16 units
		"😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀",                                                                                                                              // 72 UTF-16 units -> 2 segments
		"👨‍👩‍👧‍👦\u200d🔥{}\\[~]|€\f\r\n",
		string([]byte{0xff, 0xfe, 0xfd, 0x80, 0x00, 'a'}),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, content string) {
		info := domain.SMSInfo(content)
		again := domain.SMSInfo(content)
		if info != again {
			t.Fatalf("SMSInfo not deterministic: %+v vs %+v", info, again)
		}

		if info.Encoding != domain.SMSEncodingGSM7 && info.Encoding != domain.SMSEncodingUCS2 {
			t.Fatalf("unexpected encoding %q", info.Encoding)
		}

		if content == "" {
			if info.Units != 0 || info.Segments != 0 || info.Encoding != domain.SMSEncodingGSM7 {
				t.Fatalf("unexpected empty SMSInfo: %+v", info)
			}
			return
		}

		if info.Segments < 1 || info.Units < 1 {
			t.Fatalf("non-empty content produced invalid counts: %+v", info)
		}

		if info.Encoding == domain.SMSEncodingGSM7 {
			if info.Units <= 160 && info.Segments != 1 {
				t.Fatalf("GSM-7 units=%d expected 1 segment, got %d", info.Units, info.Segments)
			}
			if info.Units > 160 && info.Segments < 2 {
				t.Fatalf("GSM-7 units=%d expected >=2 segments, got %d", info.Units, info.Segments)
			}
		} else {
			if info.Units <= 70 && info.Segments != 1 {
				t.Fatalf("UCS-2 units=%d expected 1 segment, got %d", info.Units, info.Segments)
			}
			if info.Units > 70 && info.Segments < 2 {
				t.Fatalf("UCS-2 units=%d expected >=2 segments, got %d", info.Units, info.Segments)
			}
		}
	})
}

func FuzzValidateNotification(f *testing.F) {
	f.Add("sms", "high", "+905551234567", "", "", "Hello GSM-7", int64(0), "idem-1", false, false)
	f.Add("email", "normal", "user@example.com", "Welcome", "", "<b>Hi</b>", int64(60), "idem-2", true, false)
	f.Add("push", "low", "device-token-abc", "", "Alert", "Body 🚀", int64(3600), "", false, true)
	f.Add("SMS", "HIGH", "Alice <a@b.com>", "bad\r\nsub", "title", "", int64(-10), "bad key \n", true, true)
	f.Add("", "", "", "", "", string([]byte{0xff, 0xfe}), int64(1<<40), string([]byte{0x00, 0x7f}), false, false)

	baseNow := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	f.Fuzz(func(
		t *testing.T,
		channel, priority, recipient, subject, title, content string,
		scheduleOffsetSec int64,
		idemKey string,
		allowEmpty, extPush bool,
	) {
		_, _ = domain.ParseChannel(channel)
		_, _ = domain.ParsePriority(priority)
		_ = domain.IsValidIdempotencyKey(idemKey)

		in := domain.CreateNotificationInput{
			Recipient: recipient,
			Channel:   channel,
			Priority:  priority,
			Content:   content,
		}
		if subject != "" {
			in.Subject = &subject
		}
		if title != "" {
			in.Title = &title
		}
		if idemKey != "" {
			in.IdempotencyKey = &idemKey
		}
		if scheduleOffsetSec != 0 {
			// Bound extreme int64 seconds to avoid overflowing time.Duration while still hitting >720h and past.
			sec := scheduleOffsetSec % (10 * 365 * 24 * 3600)
			ts := baseNow.Add(time.Duration(sec) * time.Second)
			in.ScheduledAt = &ts
		}

		opts := domain.ValidationOptions{
			AllowEmptyContent:      allowEmpty,
			CheckScheduledAt:       true,
			ClockSkewTolerance:     2 * time.Second,
			ScheduleMaxAhead:       720 * time.Hour,
			ExtendedProviderFields: extPush,
		}

		ve := domain.ValidateNotification(in, baseNow, opts)
		if ve != nil && !ve.HasErrors() {
			t.Fatalf("non-nil ValidationError has zero fields")
		}

		if utf8.ValidString(in.Recipient) && utf8.ValidString(in.Content) &&
			utf8.ValidString(in.Channel) && utf8.ValidString(in.Priority) {
			h1, err1 := domain.RequestHash(&in)
			h2, err2 := domain.RequestHash(in.Normalize())
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("RequestHash normalization error mismatch: %v vs %v", err1, err2)
			}
			if err1 == nil && h1 != h2 {
				t.Fatalf("RequestHash mismatch after Normalize: %s vs %s", h1, h2)
			}
		}

		// Also exercise template ID field with random UUIDs without panicking.
		id := uuid.New()
		in.TemplateID = &id
		_ = domain.ValidateNotificationWithPrefix("notifications[0].", in, baseNow, opts)
	})
}
