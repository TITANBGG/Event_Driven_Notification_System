package middleware

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/TITANBGG/notification-system/internal/observability"
)

type middlewareErrorResponse struct {
	Error         middlewareErrorBody `json:"error"`
	CorrelationID string              `json:"correlationId,omitempty"`
}

type middlewareErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func correlationIDFromContext(ctx context.Context, w http.ResponseWriter) string {
	if cid := observability.CorrelationID(ctx); cid != "" {
		return cid
	}
	if w != nil {
		return w.Header().Get("X-Correlation-ID")
	}
	return ""
}

func writeMiddlewareError(ctx context.Context, w http.ResponseWriter, status int, code, message string) {
	payload := middlewareErrorResponse{
		Error: middlewareErrorBody{
			Code:    code,
			Message: message,
		},
		CorrelationID: correlationIDFromContext(ctx, w),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, `{"error":{"code":"internal_error","message":"internal server error"}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
	_, _ = w.Write([]byte("\n"))
}
