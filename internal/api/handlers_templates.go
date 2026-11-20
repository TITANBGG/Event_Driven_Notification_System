package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/repository"
	"github.com/TITANBGG/notification-system/internal/template"
)

func (h *handler) handleCreateTemplate(w http.ResponseWriter, r *http.Request) {
	var req upsertTemplateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	if ve := template.ValidateDefinition(template.DefinitionInput{
		Name:      req.Name,
		Channel:   req.Channel,
		Subject:   req.Subject,
		Title:     req.Title,
		Body:      req.Body,
		Variables: req.Variables,
	}); ve != nil {
		writeError(w, r, h.logger, ve)
		return
	}

	id, err := newUUIDv7()
	if err != nil {
		writeError(w, r, h.logger, fmt.Errorf("generating template id: %w", err))
		return
	}

	ch, _ := domain.ParseChannel(req.Channel)
	vars := normalizeTemplateVarsSlice(req.Variables)
	candidate := domain.Template{
		ID:        id,
		Name:      strings.TrimSpace(req.Name),
		Channel:   ch,
		Subject:   req.Subject,
		Title:     req.Title,
		Body:      req.Body,
		Variables: vars,
	}

	created, err := h.store.InsertTemplate(r.Context(), candidate)
	if err != nil {
		if errors.Is(err, domain.ErrDuplicateKey) {
			writeError(w, r, h.logger, &apiError{
				status:  http.StatusConflict,
				code:    "template_name_exists",
				message: fmt.Sprintf("an active template named %q already exists", candidate.Name),
			})
			return
		}
		writeError(w, r, h.logger, err)
		return
	}

	writeJSON(w, h.logger, http.StatusCreated, toTemplateResponse(*created))
}

func (h *handler) handleListTemplates(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ve := &domain.ValidationError{}

	var filter repository.TemplateListFilter
	if rawCh := strings.TrimSpace(q.Get("channel")); rawCh != "" {
		ch, err := domain.ParseChannel(rawCh)
		if err != nil {
			ve.Add("channel", "invalid_channel", "must be one of sms, email, push")
		} else {
			filter.Channel = &ch
		}
	}

	limit, ok := parsePageLimit(q.Get("limit"), h.cfg.PageLimitDefault, h.cfg.PageLimitMax, ve)
	if ok {
		filter.Limit = limit + 1
	}

	if ve.HasErrors() {
		writeError(w, r, h.logger, ve)
		return
	}

	cursor, err := DecodeCursor(q.Get("cursor"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	filter.Cursor = cursor

	rows, err := h.store.ListTemplates(r.Context(), filter)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	var nextCursor *string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		enc, encErr := EncodeCursor(repository.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if encErr != nil {
			writeError(w, r, h.logger, fmt.Errorf("encoding template pagination cursor: %w", encErr))
			return
		}
		nextCursor = &enc
	}

	writeJSON(w, h.logger, http.StatusOK, TemplateListResponse{
		Data:       toTemplateResponses(rows),
		NextCursor: nextCursor,
	})
}

func (h *handler) handleGetTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	tmpl, err := h.store.GetTemplate(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	writeJSON(w, h.logger, http.StatusOK, toTemplateResponse(*tmpl))
}

func (h *handler) handleUpdateTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	var req upsertTemplateRequest
	if decErr := decodeJSON(r, &req); decErr != nil {
		writeError(w, r, h.logger, decErr)
		return
	}

	if ve := template.ValidateDefinition(template.DefinitionInput{
		Name:      req.Name,
		Channel:   req.Channel,
		Subject:   req.Subject,
		Title:     req.Title,
		Body:      req.Body,
		Variables: req.Variables,
	}); ve != nil {
		writeError(w, r, h.logger, ve)
		return
	}

	ch, _ := domain.ParseChannel(req.Channel)
	vars := normalizeTemplateVarsSlice(req.Variables)
	candidate := domain.Template{
		ID:        id,
		Name:      strings.TrimSpace(req.Name),
		Channel:   ch,
		Subject:   req.Subject,
		Title:     req.Title,
		Body:      req.Body,
		Variables: vars,
	}

	updated, err := h.store.UpdateTemplate(r.Context(), candidate)
	if err != nil {
		if errors.Is(err, domain.ErrDuplicateKey) {
			writeError(w, r, h.logger, &apiError{
				status:  http.StatusConflict,
				code:    "template_name_exists",
				message: fmt.Sprintf("an active template named %q already exists", candidate.Name),
			})
			return
		}
		writeError(w, r, h.logger, err)
		return
	}

	writeJSON(w, h.logger, http.StatusOK, toTemplateResponse(*updated))
}

func (h *handler) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	if err := h.store.SoftDeleteTemplate(r.Context(), id); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) handlePreviewTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDParam(r, "id")
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	var req previewTemplateRequest
	if decErr := decodeJSON(r, &req); decErr != nil {
		writeError(w, r, h.logger, decErr)
		return
	}

	tmpl, err := h.store.GetTemplate(r.Context(), id)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	rendered, ve := template.Render("preview.", *tmpl, req.Variables)
	if ve != nil {
		writeError(w, r, h.logger, ve)
		return
	}

	resp := TemplatePreviewResponse{
		Subject:         rendered.Subject,
		Title:           rendered.Title,
		Content:         rendered.Content,
		UnusedVariables: rendered.UnusedVariables,
	}
	if tmpl.Channel == domain.ChannelSMS {
		info := domain.SMSInfo(rendered.Content)
		resp.SMS = &SMSPreviewInfo{
			Encoding: string(info.Encoding),
			Units:    info.Units,
			Segments: info.Segments,
		}
	}

	writeJSON(w, h.logger, http.StatusOK, resp)
}

func normalizeTemplateVarsSlice(vars []string) []string {
	if len(vars) == 0 {
		return []string{}
	}
	out := make([]string, len(vars))
	for i, v := range vars {
		out[i] = strings.TrimSpace(v)
	}
	return out
}
