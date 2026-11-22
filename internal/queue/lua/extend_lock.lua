-- Extend a distributed lock's TTL only if it is still held by the caller's token, so a replica whose
-- lease already expired can never prolong a lease another replica has since acquired.
-- KEYS[1]: lock key
-- ARGV[1]: owner token
-- ARGV[2]: new TTL in milliseconds
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
