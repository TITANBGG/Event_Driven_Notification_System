// Package realtime implements the Redis Pub/Sub subscriber and WebSocket client hub
// with per-client filtering, backpressure disconnect, and heartbeat pings.
//
// WHY: Subscribing to Redis once per API replica and disconnecting slow consumers avoids blocking
// the fan-out loop and keeps WebSocket broadcasts horizontally scalable.
//
// Expected output example (Phase 10):
//
//	GET /api/v1/ws?batchId=... -> delivers snapshot JSON followed by live status transition events
package realtime
