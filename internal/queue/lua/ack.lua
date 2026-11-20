redis.call("LREM", KEYS[2], 1, ARGV[1])
redis.call("HDEL", KEYS[1], ARGV[1])
return 1

