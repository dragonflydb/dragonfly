local LH_funcs = {}

function LH_funcs.string(key, hash)
    -- add value to hash
    return dragonfly.ihash(hash, false, 'GET', key)
end

function LH_funcs.list(key, hash)
    -- add values to hash
    return dragonfly.ihash(hash, false, 'LRANGE', key, 0, -1)
end

function LH_funcs.set(key, hash)
    -- add values to hash, sort before to avoid ambiguity
    return dragonfly.ihash(hash, true, 'SMEMBERS', key)
end

function LH_funcs.zset(key, hash)
    -- add values to hash, ZRANGE returns always sorted values
    return dragonfly.ihash(hash, false, 'ZRANGE', key, 0, -1, 'WITHSCORES')
end

function LH_funcs.hash(key, hash)
    -- add values to hash, first convert to key-value pairs and sort
    return dragonfly.ihash(hash, true, 'HGETALL', key)
end

function LH_funcs.json(key, hash)
    -- add values to hash, note JSON.GET returns just a string
    return dragonfly.ihash(hash, false, 'JSON.GET', key)
end

function LH_funcs.stream(key, hash)
    return dragonfly.ihash(hash, false, 'XRANGE', key, '-', '+')
end

function LH_funcs.cf(key, hash)
    local info = redis.call('CF.INFO', key)
    local fields = {key}
    for i = 1, #info, 2 do
        -- Allocated vector capacity can differ after compaction and full sync.
        if info[i] ~= 'Size' then
            table.insert(fields, info[i])
            table.insert(fields, tostring(info[i + 1]))
        end
    end
    return dragonfly.ihash(hash, false, 'ECHO', cjson.encode(fields))
end

function LH_funcs.sbf(key, hash)
    return dragonfly.ihash(hash, false, 'BF.INFO', key)
end

function LH_funcs.cms(key, hash)
    return dragonfly.ihash(hash, false, 'CMS.INFO', key)
end

function LH_funcs.topk(key, hash)
    return dragonfly.ihash(hash, false, 'TOPK.LIST', key)
end
