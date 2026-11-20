package queue

import (
	"fmt"

	"github.com/TITANBGG/notification-system/internal/domain"
)

// Hash tag {notif} pins every Redis key in the notification namespace to one Cluster slot
// so multi-key Lua scripts always execute atomically.
const (
	HashTag             = "{notif}"
	MsgHashKey          = HashTag + ":msg"
	DelayedZSetKey      = HashTag + ":delayed"
	DLQListKey          = HashTag + ":dlq"
	QueuePrefix         = HashTag + ":q:"
	InflightPrefix      = HashTag + ":inflight:"
	InflightScanPattern = InflightPrefix + "*"
	HeartbeatPrefix     = HashTag + ":hb:"
	ReconcilerLockKey   = HashTag + ":reconciler:lock"
	EventsPubSubChannel = "notif:events"
	RateLimitPrefix     = HashTag + ":rl:"

	DefaultDLQMaxLen = 100000
)

// Key returns the Redis list key for a specific channel and priority tier.
func Key(ch domain.Channel, pr domain.Priority) string {
	return fmt.Sprintf("%s%s:%s", QueuePrefix, ch, pr)
}

// InflightKey returns the per-worker inflight list key.
func InflightKey(workerID string) string {
	return InflightPrefix + workerID
}

// HeartbeatKey returns the per-worker liveness key with expiring TTL.
func HeartbeatKey(workerID string) string {
	return HeartbeatPrefix + workerID
}

// RateLimitKey returns the per-channel redis_rate GCRA key.
func RateLimitKey(ch domain.Channel) string {
	return RateLimitPrefix + ch.String()
}
