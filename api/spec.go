// Package api embeds the OpenAPI 3.0.3 specification so the HTTP server and route parity tests share one source of truth.
package api

import _ "embed"

// OpenAPISpec contains the raw YAML bytes of api/openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
