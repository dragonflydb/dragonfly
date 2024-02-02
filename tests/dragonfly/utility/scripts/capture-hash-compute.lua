#!lua flags=disable-atomicity
--[[

Script for quickly computing single 64bit hash for keys of types specified in ARGV[].
Keys of every type are sorted lexicographically to ensure consistent order.
]]--

local OUT_HASH = 0
local TYPE_FUNCS = {}

local function process(type)
    local cursor = "0"
    local keys = {}
    local callback = TYPE_FUNCS[type]

    repeat
        local result = redis.call("SCAN", cursor, "COUNT", 500, "TYPE", type)
        cursor = result[1]
        local scan_keys = result[2]
        for i, key in ipairs(scan_keys) do
            table.insert(keys, key)
        end
    until cursor == "0"

    -- sort to provide consistent order
    table.sort(keys)
    for _, key in ipairs(keys) do
        -- add key to hash
        OUT_HASH = dragonfly.ihash(OUT_HASH, key)
        -- hand hash over to callback
        OUT_HASH = callback(key, OUT_HASH)
    end
end

TYPE_FUNCS['string'] = function(key, hash)
    -- add value to hash
    return dragonfly.ihash(hash, redis.call('GET', key))
end

TYPE_FUNCS['list'] = function(key, hash)
    -- add values to hash
    return dragonfly.ihash(hash, redis.call('LRANGE', key, 0, -1))
end

TYPE_FUNCS['set'] =  function(key, hash)
    -- add values to hash, sort before to avoid ambiguity
    local items = redis.call('SMEMBERS', key)
    table.sort(items)
    return dragonfly.ihash(hash, items)
end

TYPE_FUNCS['zset'] =  function(key, hash)
    -- add values to hash, ZRANGE returns always sorted values
    return dragonfly.ihash(hash, redis.call('ZRANGE', key, 0, -1, 'WITHSCORES'))
end

TYPE_FUNCS['hash'] =  function(key, hash)
    -- add values to hash, first convert to key-value pairs and sort
    local items = redis.call('HGETALL', key)
    local paired_items = {}
    for i = 1, #items, 2 do
        table.insert(paired_items, items[i] .. '->' .. items[i+1])
    end
    table.sort(paired_items)
    return dragonfly.ihash(hash, paired_items)
end

TYPE_FUNCS['json'] =  function(key, hash)
    -- add values to hash, note JSON.GET returns just a string
    return dragonfly.ihash(hash, redis.call('JSON.GET', key))
end

for _, type in ipairs(ARGV) do
    process(type)
end


return OUT_HASH
