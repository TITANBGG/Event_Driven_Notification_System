// Package middleware provides HTTP middleware for panic recovery, correlation ID propagation,
// structured request logging, CORS, body size limits, content-type checks, and request timeouts.
//
// WHY: Enforcing cross-cutting HTTP protections once in a deterministic middleware chain eliminates
// boilerplate and guarantees every response carries X-Correlation-ID.
//
// Expected output example (Phase 3+):
//
//	Response Headers: X-Correlation-ID: 7d3c..., Access-Control-Allow-Origin: http://localhost:8081
package middleware
