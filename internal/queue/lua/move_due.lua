local ids = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
local moved = 0
for _, id in ipairs(ids) do
  redis.call("ZREM", KEYS[2], id)
  local raw = redis.call("HGET", KEYS[1], id)
  if raw then
    local ok, meta = pcall(cjson.decode, raw)
    if ok and type(meta) == "table" and meta.channel and meta.priority then
      local qkey = ARGV[3] .. meta.channel .. ":" .. meta.priority
      redis.call("LPUSH", qkey, id)
      moved = moved + 1
    else
      redis.call("HDEL", KEYS[1], id)
    end
  end
end
return moved

