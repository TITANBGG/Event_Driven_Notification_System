package domain_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TITANBGG/notification-system/internal/domain"
)

func TestSMSInfoSegmentation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		content  string
		wantEnc  domain.SMSEncoding
		wantUnit int
		wantSeg  int
	}{
		{
			name:     "empty",
			content:  "",
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 0,
			wantSeg:  0,
		},
		{
			name:     "160 GSM-7 chars fits 1 segment",
			content:  strings.Repeat("a", 160),
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 160,
			wantSeg:  1,
		},
		{
			name:     "161 GSM-7 chars splits into 2 segments",
			content:  strings.Repeat("a", 161),
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 161,
			wantSeg:  2,
		},
		{
			name:     "159 ASCII plus euro equals 161 septets -> 2 segments",
			content:  strings.Repeat("a", 159) + "€",
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 161,
			wantSeg:  2,
		},
		{
			name:     "extension table chars cost 2 septets each",
			content:  "^{}\\[~]|€\f",
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 20,
			wantSeg:  1,
		},
		{
			name:     "ESC pair never split across 153-septet boundary",
			content:  strings.Repeat("a", 152) + "€" + strings.Repeat("b", 151) + "€",
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 152 + 2 + 151 + 2,
			wantSeg:  3, // seg1: 152a, seg2: €+151b (153), seg3: € (2)
		},
		{
			name:     "Turkish GSM-7 basic subset stays GSM-7 (ö ü Ö Ü Ç)",
			content:  "ÖÜÇ öü",
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 6,
			wantSeg:  1,
		},
		{
			name:     "Turkish Ş forces UCS-2 and fits 1 segment",
			content:  "Şifreniz: 1234",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 14,
			wantSeg:  1,
		},
		{
			name:     "each non-GSM7 Turkish char forces UCS-2: ç",
			content:  "ç",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 1,
			wantSeg:  1,
		},
		{
			name:     "non-GSM7 Turkish: ğ",
			content:  "ğ",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 1,
			wantSeg:  1,
		},
		{
			name:     "non-GSM7 Turkish: Ğ",
			content:  "Ğ",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 1,
			wantSeg:  1,
		},
		{
			name:     "non-GSM7 Turkish: ı",
			content:  "ı",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 1,
			wantSeg:  1,
		},
		{
			name:     "non-GSM7 Turkish: İ",
			content:  "İ",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 1,
			wantSeg:  1,
		},
		{
			name:     "non-GSM7 Turkish: ş",
			content:  "ş",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 1,
			wantSeg:  1,
		},
		{
			name:     "70 UCS-2 units fits 1 segment",
			content:  "ş" + strings.Repeat("a", 69),
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 70,
			wantSeg:  1,
		},
		{
			name:     "71 UCS-2 units splits into 2 segments",
			content:  "ş" + strings.Repeat("a", 70),
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 71,
			wantSeg:  2,
		},
		{
			name:     "emoji counts as 2 UTF-16 code units",
			content:  "😀",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 2,
			wantSeg:  1,
		},
		{
			name:     "69 BMP + emoji = 71 UCS-2 units -> 2 segments",
			content:  strings.Repeat("a", 69) + "😀",
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 71,
			wantSeg:  2,
		},
		{
			name:     "surrogate pair at 67-unit boundary is never split across parts",
			content:  strings.Repeat("ş", 66) + "😀" + strings.Repeat("a", 66),
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 66 + 2 + 66,
			wantSeg:  3, // seg1: 66 ş, seg2: 😀+65a (67), seg3: 1a
		},
		{
			name:     "10 GSM-7 segments boundary (1530 septets)",
			content:  strings.Repeat("a", 1530),
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 1530,
			wantSeg:  10,
		},
		{
			name:     "11 GSM-7 segments boundary (1531 septets)",
			content:  strings.Repeat("a", 1531),
			wantEnc:  domain.SMSEncodingGSM7,
			wantUnit: 1531,
			wantSeg:  11,
		},
		{
			name:     "10 UCS-2 segments boundary (670 units)",
			content:  strings.Repeat("ş", 670),
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 670,
			wantSeg:  10,
		},
		{
			name:     "11 UCS-2 segments boundary (671 units)",
			content:  strings.Repeat("ş", 671),
			wantEnc:  domain.SMSEncodingUCS2,
			wantUnit: 671,
			wantSeg:  11,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := domain.SMSInfo(tc.content)
			assert.Equal(t, tc.wantEnc, got.Encoding)
			assert.Equal(t, tc.wantUnit, got.Units)
			assert.Equal(t, tc.wantSeg, got.Segments)
		})
	}
}
