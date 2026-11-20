if #KEYS >= 3 and KEYS[3] ~= "" then
  redis.call("LREM", KEYS[3], 1, ARGV[1])
end
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("LPUSH", KEYS[2], ARGV[1])
local limit = tonumber(ARGV[2])
if limit and limit > 0 then
  redis.call("LTRIM", KEYS[2], 0, limit - 1)
end
return 1
