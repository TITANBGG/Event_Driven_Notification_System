package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// RequestHash computes the lowercase SHA-256 hex digest of canonical JSON (sorted keys, defaults applied).
//
// WHY: Idempotency keys must accept functionally identical requests regardless of JSON key ordering
// or omitted default fields (such as missing priority vs explicit priority="normal") while catching
// genuine payload conflicts (HTTP 409 idempotency_conflict).
//
// Expected output example:
//
//	h1, _ := RequestHash(CreateNotificationInput{Recipient: "+905551234567", Channel: "sms", Content: "hi"})
//	h2, _ := RequestHash(map[string]any{"priority": "normal", "content": "hi", "channel": "sms", "recipient": "+905551234567"})
//	// h1 == h2 (64-char SHA-256 hex string)
func RequestHash(v any) (string, error) {
	normalized := applyDefaults(v)

	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("marshaling value for hash: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return "", fmt.Errorf("decoding canonical value: %w", err)
	}

	var buf bytes.Buffer
	if err := writeCanonicalJSON(&buf, generic); err != nil {
		return "", fmt.Errorf("encoding canonical JSON: %w", err)
	}

	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

func applyDefaults(v any) any {
	switch typed := v.(type) {
	case CreateNotificationInput:
		if strings.TrimSpace(typed.Priority) == "" {
			typed.Priority = string(PriorityNormal)
		}
		return typed
	case *CreateNotificationInput:
		if typed == nil {
			return nil
		}
		cp := *typed
		if strings.TrimSpace(cp.Priority) == "" {
			cp.Priority = string(PriorityNormal)
		}
		return cp
	case []CreateNotificationInput:
		out := make([]CreateNotificationInput, len(typed))
		for i, item := range typed {
			if strings.TrimSpace(item.Priority) == "" {
				item.Priority = string(PriorityNormal)
			}
			out[i] = item
		}
		return out
	default:
		return v
	}
}

func writeCanonicalJSON(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeJSONString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := writeCanonicalJSON(buf, val[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
		return nil

	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalJSON(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
		return nil

	case json.Number:
		buf.WriteString(val.String())
		return nil

	default:
		encoded, err := json.Marshal(val)
		if err != nil {
			return err
		}
		buf.Write(encoded)
		return nil
	}
}

func writeJSONString(buf *bytes.Buffer, s string) error {
	encoded, err := json.Marshal(s)
	if err != nil {
		return err
	}
	buf.Write(encoded)
	return nil
}
