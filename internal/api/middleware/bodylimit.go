package middleware

import (
	"fmt"
	"net/http"
)

// BodyLimit enforces MAX_BODY_BYTES globally while permitting BATCH_MAX_BODY_BYTES on the batch route.
func BodyLimit(defaultMax, batchMax int64, batchPath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			limit := defaultMax
			isBatch := batchPath != "" && r.URL.Path == batchPath
			if isBatch {
				limit = batchMax
			}

			if limit > 0 {
				if r.ContentLength > limit {
					msg := fmt.Sprintf("request body exceeds limit of %d bytes", limit)
					if isBatch {
						msg += "; split batch into smaller requests"
					}
					writeMiddlewareError(r.Context(), w, http.StatusRequestEntityTooLarge, "payload_too_large", msg)
					return
				}
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}

			next.ServeHTTP(w, r)
		})
	}
}
