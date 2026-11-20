package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	apispec "github.com/TITANBGG/notification-system/api"
	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/config"
)

type openAPIDocument struct {
	OpenAPI string                    `yaml:"openapi"`
	Paths   map[string]map[string]any `yaml:"paths"`
}

func TestOpenAPISpecMatchesRouter(t *testing.T) {
	t.Parallel()

	var doc openAPIDocument
	require.NoError(t, yaml.Unmarshal(apispec.OpenAPISpec, &doc))
	assert.Equal(t, "3.0.3", doc.OpenAPI)

	specOps := make(map[string]bool)
	for path, methods := range doc.Paths {
		for method := range methods {
			switch strings.ToUpper(method) {
			case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
				specOps[fmt.Sprintf("%s %s", strings.ToUpper(method), path)] = true
			}
		}
	}

	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	require.NoError(t, err)

	r := api.NewRouter(api.Deps{
		Config:   cfg,
		Store:    newFakeStore(),
		Enqueuer: api.NewMemoryEnqueuer(),
	})

	routerOps := make(map[string]bool)
	err = chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		key := fmt.Sprintf("%s %s", strings.ToUpper(method), route)
		routerOps[key] = true
		return nil
	})
	require.NoError(t, err)

	for op := range routerOps {
		assert.True(t, specOps[op], "router operation %s missing from api/openapi.yaml", op)
	}
	for op := range specOps {
		assert.True(t, routerOps[op], "OpenAPI operation %s not registered in chi router", op)
	}
}
