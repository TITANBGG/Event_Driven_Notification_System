-- Move specific leftover IDs from one worker's inflight list back to the consuming end of their queue.
-- Used by the live worker's inflight sweeper: an ID only moves if LREM actually removed it, so an item
-- that was acked concurrently is never resurrected.
-- KEYS[1]: message metadata hash ({notif}:msg)
-- KEYS[2]: worker inflight list ({notif}:inflight:<workerId>)
-- ARGV[1]: queue key prefix ({notif}:q:)
-- ARGV[2..n]: notification IDs
local count = 0
for i = 2, #ARGV do
  local id = ARGV[i]
  if redis.call("LREM", KEYS[2], 1, id) > 0 then
    local raw = redis.call("HGET", KEYS[1], id)
    if raw then
      local ok, meta = pcall(cjson.decode, raw)
      if ok and type(meta) == "table" and meta.channel and meta.priority then
        redis.call("RPUSH", ARGV[1] .. meta.channel .. ":" .. meta.priority, id)
        count = count + 1
      else
        redis.call("HDEL", KEYS[1], id)
      end
    end
  end
end
return count
