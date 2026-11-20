package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"unicode/utf8"
)

// decodeJSON reads a single strict JSON value into dst, rejecting unknown fields and trailing data
// while preserving *http.MaxBytesError so the central error mapper returns 413 instead of 400.
func decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return &apiError{
			status:  http.StatusBadRequest,
			code:    "malformed_json",
			message: "request body must not be empty",
		}
	}

	// The body is already capped by the body-limit middleware, so buffering it is bounded.
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return &apiError{
			status:  http.StatusBadRequest,
			code:    "malformed_json",
			message: "reading request body: " + err.Error(),
		}
	}
	// encoding/json silently replaces invalid UTF-8 with U+FFFD; reject it instead so a client sending
	// e.g. Windows-1254 bytes gets a clear 400 rather than a garbled SMS delivered to a customer.
	if !utf8.Valid(raw) {
		return &apiError{
			status:  http.StatusBadRequest,
			code:    "malformed_json",
			message: "request body must be valid UTF-8",
		}
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		if errors.Is(err, io.EOF) {
			return &apiError{
				status:  http.StatusBadRequest,
				code:    "malformed_json",
				message: "request body must not be empty",
			}
		}
		return &apiError{
			status:  http.StatusBadRequest,
			code:    "malformed_json",
			message: "malformed JSON request: " + err.Error(),
		}
	}

	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return &apiError{
			status:  http.StatusBadRequest,
			code:    "malformed_json",
			message: "request body must contain a single JSON object",
		}
	}

	return nil
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		if logger != nil {
			logger.Error("failed to marshal JSON response", slog.String("error", err.Error()))
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"internal server error"}}` + "\n"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(raw); err != nil && logger != nil {
		logger.Warn("failed to write HTTP response body", slog.String("error", err.Error()))
		return
	}
	_, _ = w.Write([]byte("\n"))
}
