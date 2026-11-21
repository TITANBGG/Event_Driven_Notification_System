-- Move the oldest ID of the first non-empty queue (KEYS[3..]) onto the worker's inflight list (KEYS[2])
-- and return {id, meta}. RPOP + LPUSH is LMOVE RIGHT LEFT, split on purpose: under maxmemory with
-- noeviction, Redis rejects a script whose FIRST write is a denyoom command (LMOVE is one), but lets a
-- script that has already written finish so it stays atomic. RPOP is not denyoom, so workers keep
-- draining (and freeing memory) while Redis is full; with LMOVE a full Redis with nothing in flight
-- could never drain again (found by the 64 MB stress test, 2026-11-21). Memory-neutral: one ID moves.
for i = 3, #KEYS do
  local id = redis.call("RPOP", KEYS[i])
  if id then
    redis.call("LPUSH", KEYS[2], id)
    local meta = redis.call("HGET", KEYS[1], id)
    if not meta then
      meta = ""
    end
    return {id, meta}
  end
end
return nil
