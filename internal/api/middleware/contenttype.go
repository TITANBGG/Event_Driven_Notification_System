package middleware

import (
	"mime"
	"net/http"
	"strings"
)

// RequireJSON rejects POST/PUT/PATCH requests whose body or Content-Type header is not application/json,
// while permitting empty-body action POSTs (such as /cancel and /retry) without a Content-Type header.
func RequireJSON() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost, http.MethodPut, http.MethodPatch:
				hasBody := r.ContentLength > 0 || (r.ContentLength == -1 && r.Body != nil && r.Body != http.NoBody)
				ct := strings.TrimSpace(r.Header.Get("Content-Type"))
				if ct != "" || hasBody {
					mediaType, _, err := mime.ParseMediaType(ct)
					if err != nil || mediaType != "application/json" {
						writeMiddlewareError(r.Context(), w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
						return
					}
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}
