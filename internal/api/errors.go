package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/observability"
)

// ErrorResponse is the standard envelope returned by every API error response.
type ErrorResponse struct {
	Error         ErrorBody `json:"error"`
	CorrelationID string    `json:"correlationId,omitempty"`
}

// ErrorBody carries the machine-readable error code, message, and optional per-field details.
type ErrorBody struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Details []domain.FieldError `json:"details,omitempty"`
}

type apiError struct {
	status  int
	code    string
	message string
	details []domain.FieldError
}

func (e *apiError) Error() string {
	return e.message
}

// writeError maps all API and domain errors to HTTP status codes in a single place
// and never exposes internal error strings to clients on 500 responses.
func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	cid := observability.CorrelationID(r.Context())

	var ae *apiError
	if errors.As(err, &ae) {
		writeJSON(w, logger, ae.status, ErrorResponse{
			Error: ErrorBody{
				Code:    ae.code,
				Message: ae.message,
				Details: ae.details,
			},
			CorrelationID: cid,
		})
		return
	}

	var ve *domain.ValidationError
	if errors.As(err, &ve) {
		code := "validation_failed"
		if len(ve.Fields) > 0 {
			allDup := true
			for _, f := range ve.Fields {
				if f.Code != "duplicate_idempotency_key" {
					allDup = false
					break
				}
			}
			if allDup {
				code = "duplicate_idempotency_key"
			}
		}
		writeJSON(w, logger, http.StatusUnprocessableEntity, ErrorResponse{
			Error: ErrorBody{
				Code:    code,
				Message: ve.Error(),
				Details: ve.Fields,
			},
			CorrelationID: cid,
		})
		return
	}

	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		msg := fmt.Sprintf("request body exceeds limit of %d bytes", maxErr.Limit)
		if r.URL.Path == batchCreatePath {
			msg += "; split batch into smaller requests"
		}
		writeJSON(w, logger, http.StatusRequestEntityTooLarge, ErrorResponse{
			Error: ErrorBody{
				Code:    "payload_too_large",
				Message: msg,
			},
			CorrelationID: cid,
		})
		return
	}

	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, logger, http.StatusNotFound, ErrorResponse{
			Error: ErrorBody{
				Code:    "not_found",
				Message: "requested resource was not found",
			},
			CorrelationID: cid,
		})
	case errors.Is(err, domain.ErrInvalidTransition):
		writeJSON(w, logger, http.StatusConflict, ErrorResponse{
			Error: ErrorBody{
				Code:    "invalid_transition",
				Message: "operation not permitted in the resource's current state",
			},
			CorrelationID: cid,
		})
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeJSON(w, logger, http.StatusConflict, ErrorResponse{
			Error: ErrorBody{
				Code:    "idempotency_conflict",
				Message: "idempotency key was previously used with a different request payload",
			},
			CorrelationID: cid,
		})
	case errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, logger, http.StatusGatewayTimeout, ErrorResponse{
			Error: ErrorBody{
				Code:    "request_timeout",
				Message: "request processing exceeded deadline",
			},
			CorrelationID: cid,
		})
	default:
		route := "unmatched"
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			if pat := rctx.RoutePattern(); pat != "" {
				route = pat
			}
		}
		observability.LoggerFrom(r.Context(), logger).Error("unhandled api error",
			slog.String("route", route),
			slog.String("method", r.Method),
			slog.String("error", err.Error()),
		)
		writeJSON(w, logger, http.StatusInternalServerError, ErrorResponse{
			Error: ErrorBody{
				Code:    "internal_error",
				Message: "internal server error",
			},
			CorrelationID: cid,
		})
	}
}

func notFoundHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, logger, http.StatusNotFound, ErrorResponse{
			Error: ErrorBody{
				Code:    "not_found",
				Message: "endpoint not found",
			},
			CorrelationID: observability.CorrelationID(r.Context()),
		})
	}
}

func methodNotAllowedHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, logger, http.StatusMethodNotAllowed, ErrorResponse{
			Error: ErrorBody{
				Code:    "method_not_allowed",
				Message: "HTTP method not allowed for this endpoint",
			},
			CorrelationID: observability.CorrelationID(r.Context()),
		})
	}
}
