-- Unconditionally store message metadata and push the ID onto its queue list.
-- Used by the reconciler to recover stale pending rows missing from Redis.
-- KEYS[1]: message metadata hash ({notif}:msg)
-- KEYS[2]: channel priority queue list ({notif}:q:<channel>:<priority>)
-- ARGV[1]: notification ID
-- ARGV[2]: serialized Meta JSON
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1

