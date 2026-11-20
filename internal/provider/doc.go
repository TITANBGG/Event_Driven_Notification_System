// Package provider implements the external HTTP notification delivery client,
// Idempotency-Key propagation, Retry-After parsing, and outcome classification.
//
// WHY: Centralizing HTTP status/network error classification prevents retrying 4xx permanent failures
// and prevents 400 client errors from falsely tripping the provider circuit breaker.
//
// Expected output example (Phase 4+):
//
//	client.Send(ctx, n) on HTTP 202 -> Result{Outcome: OutcomeSent, ProviderMessageID: "3f1c2b9e-..."}
package provider
