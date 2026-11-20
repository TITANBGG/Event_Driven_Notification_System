package api

import (
	"net/http"

	apispec "github.com/TITANBGG/notification-system/api"
)

func (h *handler) handleHealthLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.logger, http.StatusOK, LiveStatusResponse{Status: "ok"})
}

func (h *handler) handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(apispec.OpenAPISpec); err != nil {
		h.logger.Warn("failed to write OpenAPI spec response", "error", err.Error())
	}
}
