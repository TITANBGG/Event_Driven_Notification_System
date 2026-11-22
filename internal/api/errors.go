package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/TITANBGG/notification-system/internal/app"
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

	var keyConflict *app.ItemKeyConflictError
	if errors.As(err, &keyConflict) {
		details := make([]domain.FieldError, 0, len(keyConflict.Items))
		for _, it := range keyConflict.Items {
			field := "idempotencyKey"
			if it.Index >= 0 {
				field = fmt.Sprintf("notifications[%d].idempotencyKey", it.Index)
			}
			details = append(details, domain.FieldError{
				Field:   field,
				Code:    "item_idempotency_key_exists",
				Message: fmt.Sprintf("already exists for %s %s", it.Existing.ResourceType, it.Existing.ResourceID),
			})
		}
		message := "one or more batch item idempotency keys already exist"
		if len(details) == 0 {
			message = "one or more item idempotency keys already exist"
		}
		writeJSON(w, logger, http.StatusConflict, ErrorResponse{
			Error:         ErrorBody{Code: "item_idempotency_key_exists", Message: message, Details: details},
			CorrelationID: cid,
		})
		return
	}

	var ve *domain.ValidationError
	if errors.As(err, &ve) {
		code := "validation_failed"
		if len(ve.Fields) > 0 {
			firstCode := ve.Fields[0].Code
			switch firstCode {
			case "duplicate_idempotency_key", "missing_variable", "template_not_found":
				allSame := true
				for _, f := range ve.Fields {
					if f.Code != firstCode {
						allSame = false
						break
					}
				}
				if allSame {
					code = firstCode
				}
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
	case isDatabaseUnavailable(err):
		observability.LoggerFrom(r.Context(), logger).Error("database unavailable during request",
			slog.String("route", routePattern(r)),
			slog.String("method", r.Method),
			slog.String("error", err.Error()),
		)
		writeJSON(w, logger, http.StatusServiceUnavailable, ErrorResponse{
			Error: ErrorBody{
				Code:    "service_unavailable",
				Message: "database temporarily unavailable",
			},
			CorrelationID: cid,
		})
	default:
		observability.LoggerFrom(r.Context(), logger).Error("unhandled api error",
			slog.String("route", routePattern(r)),
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

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pat := rctx.RoutePattern(); pat != "" {
			return pat
		}
	}
	return "unmatched"
}

func isDatabaseUnavailable(err error) bool {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
		switch pgErr.Code {
		case "57P01", "57P02", "57P03":
			return true
		}
	}
	return false
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
