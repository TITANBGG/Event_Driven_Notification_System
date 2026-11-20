local count = 0
while true do
  local id = redis.call("LPOP", KEYS[2])
  if not id then
    break
  end
  local raw = redis.call("HGET", KEYS[1], id)
  if raw then
    local ok, meta = pcall(cjson.decode, raw)
    if ok and type(meta) == "table" and meta.channel and meta.priority then
      local qkey = ARGV[1] .. meta.channel .. ":" .. meta.priority
      redis.call("RPUSH", qkey, id)
      count = count + 1
    else
      redis.call("HDEL", KEYS[1], id)
    end
  end
end
return count

