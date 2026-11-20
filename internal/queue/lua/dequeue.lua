for i = 3, #KEYS do
  local id = redis.call("LMOVE", KEYS[i], KEYS[2], "RIGHT", "LEFT")
  if id then
    local meta = redis.call("HGET", KEYS[1], id)
    if not meta then
      meta = ""
    end
    return {id, meta}
  end
end
return nil

