// Package queue implements the Redis priority dispatch queues, per-worker inflight lists,
// delayed retry/schedule ZSET, DLQ, and atomic Lua scripts embedded via go:embed.
//
// WHY: Using LMOVE into per-worker inflight lists combined with {notif} hash-tagged Lua scripts
// prevents message loss on worker crashes and keeps queue operations cluster-slot safe.
//
// Expected output example (Phase 4+):
//
//	queue.Dequeue(ctx, ChannelSMS, "worker-1") -> (&Meta{ID: "01936f7e-...", Priority: "high"}, true, nil)
package queue
