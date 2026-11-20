-- Release a distributed lock only if held by the caller's token.
-- KEYS[1]: lock key
-- ARGV[1]: owner token
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0

