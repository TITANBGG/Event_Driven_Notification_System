// Package api exposes the HTTP REST router, handlers, DTOs, keyset pagination cursors,
// and centralized domain-to-HTTP error mapping for the notification management API.
//
// WHY: Consolidating error translation and request decoding in one layer ensures consistent
// JSON error payloads with correlationId across every endpoint.
//
// Expected output example (Phase 3+):
//
//	POST /api/v1/notifications -> 202 Accepted {"id":"01936f7e-...","status":"pending",...}
package api
