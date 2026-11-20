package domain

import (
	"errors"
	"fmt"
)

// Typed sentinel errors mapped to HTTP status codes in one place (internal/api/errors.go).
//
// Expected output example:
//
//	errors.Is(err, domain.ErrInvalidTransition) -> mapped to HTTP 409 Conflict
var (
	ErrNotFound            = errors.New("notification not found")
	ErrInvalidTransition   = errors.New("invalid status transition")
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	ErrLeaseLost           = errors.New("worker processing lease lost")
	ErrDuplicateKey        = errors.New("duplicate key")
)

// FieldError describes a single validation failure at a specific JSON field path.
type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ValidationError collects all input validation violations for a single or batch request.
//
// WHY: Returning every field violation at once (e.g., notifications[3].recipient and content)
// prevents frustrating multi-round API fix loops for callers.
//
// Expected output example:
//
//	ve.Error() -> "request has 2 invalid fields"
type ValidationError struct {
	Fields []FieldError
}

// Error implements the error interface using the canonical API summary message.
func (e *ValidationError) Error() string {
	if e == nil || len(e.Fields) == 0 {
		return "validation failed"
	}
	if len(e.Fields) == 1 {
		return "request has 1 invalid field"
	}
	return fmt.Sprintf("request has %d invalid fields", len(e.Fields))
}

// Add appends a single field validation error.
func (e *ValidationError) Add(field, code, message string) {
	e.Fields = append(e.Fields, FieldError{
		Field:   field,
		Code:    code,
		Message: message,
	})
}

// HasErrors reports whether at least one field error has been recorded.
func (e *ValidationError) HasErrors() bool {
	return e != nil && len(e.Fields) > 0
}
