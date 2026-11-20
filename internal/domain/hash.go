package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

type canonicalizer interface {
	canonicalize() any
}

// RequestHash computes the lowercase SHA-256 hex digest of canonical JSON (sorted keys, normalized defaults, UTC timestamps).
// Used by API idempotency checks so equivalent requests ("SMS" vs "sms", omitted priority vs "normal",
// +03:00 vs Z) replay consistently rather than triggering spurious 409 conflicts.
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
	case *CreateNotificationInput:
		if typed == nil {
			return nil
		}
		return typed.Normalize()
	case *BatchCreateInput:
		if typed == nil {
			return nil
		}
		return typed.Normalize()
	case canonicalizer:
		return typed.canonicalize()
	case []CreateNotificationInput:
		out := make([]CreateNotificationInput, len(typed))
		for i, item := range typed {
			out[i] = item.Normalize()
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
