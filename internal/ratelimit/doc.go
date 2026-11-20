// Package ratelimit implements the distributed per-channel GCRA rate limiter backed by Redis.
//
// WHY: Sharing Burst=1 GCRA buckets across all worker replicas guarantees the strict 100 msg/s/channel
// provider ceiling holds horizontally and fails closed if Redis becomes unreachable.
//
// Expected output example (Phase 4+):
//
//	limiter.Wait(ctx, domain.ChannelSMS) -> paces concurrent callers >= 10ms apart; returns nil or ctx.Err()
package ratelimit
