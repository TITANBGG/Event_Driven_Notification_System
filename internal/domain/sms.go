package domain

import "unicode/utf16"

const (
	// GSM-7 single vs multipart septet limits per 3GPP TS 23.038 / UDH concatenation overhead (7 septets).
	gsm7SingleMax    = 160
	gsm7MultipartMax = 153

	// UCS-2 (UTF-16BE) single vs multipart 16-bit code-unit limits.
	ucs2SingleMax    = 70
	ucs2MultipartMax = 67

	// MaxSMSSegments caps SMS length to 10 segments (1530 GSM-7 septets or 670 UCS-2 code units).
	MaxSMSSegments = 10
)

// SMSEncoding identifies the character encoding chosen for SMS segmentation.
type SMSEncoding string

// Supported SMS encodings.
const (
	SMSEncodingGSM7 SMSEncoding = "GSM-7"
	SMSEncodingUCS2 SMSEncoding = "UCS-2"
)

// SMSSegmentInfo reports the encoding, character/code-unit count, and segment count of an SMS body.
//
// Expected output example:
//
//	SMSInfo("Şifreniz: 1234") -> SMSSegmentInfo{Encoding: "UCS-2", Units: 14, Segments: 1}
type SMSSegmentInfo struct {
	Encoding SMSEncoding `json:"encoding"`
	Units    int         `json:"units"`
	Segments int         `json:"segments"`
}

// gsm7Basic holds the ETSI GSM 03.38 7-bit default alphabet (1 septet per character).
// WHY: Notice Turkish characters ö, ü, Ö, Ü, Ç are present here while ç, ğ, Ğ, ı, İ, ş, Ş are absent,
// forcing messages containing the latter characters to fall back to UCS-2 (documented limitation).
var gsm7Basic = map[rune]struct{}{
	'@': {}, '£': {}, '$': {}, '¥': {}, 'è': {}, 'é': {}, 'ù': {}, 'ì': {}, 'ò': {}, 'Ç': {},
	'\n': {}, 'Ø': {}, 'ø': {}, '\r': {}, 'Å': {}, 'å': {},
	'Δ': {}, '_': {}, 'Φ': {}, 'Γ': {}, 'Λ': {}, 'Ω': {}, 'Π': {}, 'Ψ': {}, 'Σ': {}, 'Θ': {}, 'Ξ': {},
	'Æ': {}, 'æ': {}, 'ß': {}, 'É': {},
	' ': {}, '!': {}, '"': {}, '#': {}, '¤': {}, '%': {}, '&': {}, '\'': {}, '(': {}, ')': {},
	'*': {}, '+': {}, ',': {}, '-': {}, '.': {}, '/': {},
	'0': {}, '1': {}, '2': {}, '3': {}, '4': {}, '5': {}, '6': {}, '7': {}, '8': {}, '9': {},
	':': {}, ';': {}, '<': {}, '=': {}, '>': {}, '?': {}, '¡': {},
	'A': {}, 'B': {}, 'C': {}, 'D': {}, 'E': {}, 'F': {}, 'G': {}, 'H': {}, 'I': {}, 'J': {},
	'K': {}, 'L': {}, 'M': {}, 'N': {}, 'O': {}, 'P': {}, 'Q': {}, 'R': {}, 'S': {}, 'T': {},
	'U': {}, 'V': {}, 'W': {}, 'X': {}, 'Y': {}, 'Z': {},
	'Ä': {}, 'Ö': {}, 'Ñ': {}, 'Ü': {}, '§': {}, '¿': {},
	'a': {}, 'b': {}, 'c': {}, 'd': {}, 'e': {}, 'f': {}, 'g': {}, 'h': {}, 'i': {}, 'j': {},
	'k': {}, 'l': {}, 'm': {}, 'n': {}, 'o': {}, 'p': {}, 'q': {}, 'r': {}, 's': {}, 't': {},
	'u': {}, 'v': {}, 'w': {}, 'x': {}, 'y': {}, 'z': {},
	'ä': {}, 'ö': {}, 'ñ': {}, 'ü': {}, 'à': {},
}

// gsm7Extension holds the GSM 03.38 extension table characters, each costing 2 septets (ESC + code).
var gsm7Extension = map[rune]struct{}{
	'\f': {}, '^': {}, '{': {}, '}': {}, '\\': {}, '[': {}, '~': {}, ']': {}, '|': {}, '€': {},
}

// SMSInfo calculates GSM-7 vs UCS-2 encoding, unit count (septets or UTF-16 code units),
// and exact segment count without ever splitting a GSM-7 ESC pair or UTF-16 surrogate pair.
//
// Expected output example:
//
//	SMSInfo(strings.Repeat("a", 160))      -> {Encoding: "GSM-7", Units: 160, Segments: 1}
//	SMSInfo(strings.Repeat("a", 159)+"€")  -> {Encoding: "GSM-7", Units: 161, Segments: 2}
//	SMSInfo(strings.Repeat("a", 69)+"😀")  -> {Encoding: "UCS-2", Units: 71, Segments: 2}
func SMSInfo(content string) SMSSegmentInfo {
	if content == "" {
		return SMSSegmentInfo{
			Encoding: SMSEncodingGSM7,
			Units:    0,
			Segments: 0,
		}
	}

	isGSM7 := true
	totalSeptets := 0
	for _, r := range content {
		if _, ok := gsm7Basic[r]; ok {
			totalSeptets++
			continue
		}
		if _, ok := gsm7Extension[r]; ok {
			totalSeptets += 2
			continue
		}
		isGSM7 = false
		break
	}

	if isGSM7 {
		if totalSeptets <= gsm7SingleMax {
			return SMSSegmentInfo{
				Encoding: SMSEncodingGSM7,
				Units:    totalSeptets,
				Segments: 1,
			}
		}
		segments := countSegments(content, gsm7MultipartMax, func(r rune) int {
			if _, ok := gsm7Extension[r]; ok {
				return 2
			}
			return 1
		})
		return SMSSegmentInfo{
			Encoding: SMSEncodingGSM7,
			Units:    totalSeptets,
			Segments: segments,
		}
	}

	totalUTF16Units := 0
	for _, r := range content {
		totalUTF16Units += utf16RuneUnits(r)
	}
	if totalUTF16Units <= ucs2SingleMax {
		return SMSSegmentInfo{
			Encoding: SMSEncodingUCS2,
			Units:    totalUTF16Units,
			Segments: 1,
		}
	}
	segments := countSegments(content, ucs2MultipartMax, utf16RuneUnits)
	return SMSSegmentInfo{
		Encoding: SMSEncodingUCS2,
		Units:    totalUTF16Units,
		Segments: segments,
	}
}

func utf16RuneUnits(r rune) int {
	n := utf16.RuneLen(r)
	if n < 0 {
		return 1
	}
	return n
}

// countSegments packs character unit costs greedily into fixed-capacity segments so a multi-unit
// character (GSM-7 ESC pair or UTF-16 surrogate pair) is never split across a segment boundary.
func countSegments(content string, perSegmentLimit int, costFn func(rune) int) int {
	segments := 1
	used := 0
	for _, r := range content {
		cost := costFn(r)
		if used+cost > perSegmentLimit {
			segments++
			used = cost
			continue
		}
		used += cost
	}
	return segments
}
