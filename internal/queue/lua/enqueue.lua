if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1

