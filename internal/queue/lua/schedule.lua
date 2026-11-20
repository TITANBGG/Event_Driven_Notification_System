redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])
if KEYS[3] ~= nil and KEYS[3] ~= "" then
  redis.call("LREM", KEYS[3], 1, ARGV[1])
end
return 1

