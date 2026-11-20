// Package worker orchestrates channel consumer loops, retry backoff calculation,
// per-channel circuit breakers, the delayed ZSET mover, the PostgreSQL/Redis reconciler, and heartbeats.
//
// WHY: Coordinating Claim, rate limiting, fenced status updates, and orphan recovery in one decoupled
// engine guarantees no notification is silently lost even under worker crashes or transient Redis outages.
//
// Expected output example (Phase 4+):
//
//	worker.Run(ctx) -> processes queued notifications concurrently and drains inflight items cleanly on SIGTERM
package worker
