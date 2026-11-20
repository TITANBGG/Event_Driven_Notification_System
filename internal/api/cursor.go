package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/repository"
)

type cursorPayload struct {
	T  time.Time `json:"t"`
	ID uuid.UUID `json:"id"`
}

// EncodeCursor serializes a keyset boundary (created_at, id) into an opaque URL-safe cursor.
func EncodeCursor(c repository.Cursor) (string, error) {
	payload := cursorPayload{
		T:  c.CreatedAt.UTC(),
		ID: c.ID,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeCursor validates and decodes an opaque pagination cursor back into a repository.Cursor.
func DecodeCursor(raw string) (*repository.Cursor, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, &apiError{
			status:  http.StatusBadRequest,
			code:    "invalid_cursor",
			message: "cursor parameter is malformed",
		}
	}

	var payload cursorPayload
	dec := json.NewDecoder(bytes.NewReader(decoded))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		return nil, &apiError{
			status:  http.StatusBadRequest,
			code:    "invalid_cursor",
			message: "cursor parameter is malformed",
		}
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, &apiError{
			status:  http.StatusBadRequest,
			code:    "invalid_cursor",
			message: "cursor parameter is malformed",
		}
	}
	if payload.T.IsZero() || payload.ID == uuid.Nil {
		return nil, &apiError{
			status:  http.StatusBadRequest,
			code:    "invalid_cursor",
			message: "cursor parameter is invalid",
		}
	}

	return &repository.Cursor{
		CreatedAt: payload.T.UTC(),
		ID:        payload.ID,
	}, nil
}
