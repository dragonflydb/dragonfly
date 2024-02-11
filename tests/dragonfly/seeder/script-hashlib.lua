local LH_funcs = {}

function LH_funcs.string(key, hash)
    -- add value to hash
    return dragonfly.ihash(hash, redis.call('GET', key))
end

function LH_funcs.list(key, hash)
    -- add values to hash
    return dragonfly.ihash(hash, redis.call('LRANGE', key, 0, -1))
end

function LH_funcs.set(key, hash)
    -- add values to hash, sort before to avoid ambiguity
    local items = redis.call('SMEMBERS', key)
    table.sort(items)
    return dragonfly.ihash(hash, items)
end

function LH_funcs.zset(key, hash)
    -- add values to hash, ZRANGE returns always sorted values
    return dragonfly.ihash(hash, redis.call('ZRANGE', key, 0, -1, 'WITHSCORES'))
end

function LH_funcs.hash(key, hash)
    -- add values to hash, first convert to key-value pairs and sort
    local items = redis.call('HGETALL', key)
    local paired_items = {}
    for i = 1, #items, 2 do
        table.insert(paired_items, items[i] .. '->' .. items[i+1])
    end
    table.sort(paired_items)
    return dragonfly.ihash(hash, paired_items)
end

function LH_funcs.json(key, hash)
    -- add values to hash, note JSON.GET returns just a string
    return dragonfly.ihash(hash, redis.call('JSON.GET', key))
end
