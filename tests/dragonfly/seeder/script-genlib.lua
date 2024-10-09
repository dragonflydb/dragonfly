local LG_funcs = {}

function LG_funcs.init(dsize, csize)
    LG_funcs.dsize = dsize
    LG_funcs.csize = csize
    LG_funcs.esize = math.ceil(dsize / csize)
end

-- strings
-- store blobs of random chars

function LG_funcs.add_string(key)
    redis.apcall('SET', key, dragonfly.randstr(LG_funcs.dsize))
end

function LG_funcs.mod_string(key)
    -- APPEND and SETRANGE are the only modifying operations for strings,
    -- issue APPEND rarely to not grow data too much
    if math.random() < 0.05 then
        redis.apcall('APPEND', key, '+')
    else
        local replacement = dragonfly.randstr(LG_funcs.dsize // 2)
        redis.apcall('SETRANGE', key, math.random(0, LG_funcs.dsize // 2), replacement)
    end
end

-- lists
-- store list of random blobs of default container/element sizes

function LG_funcs.add_list(key)
    local elements = dragonfly.randstr(LG_funcs.esize, LG_funcs.csize)
    redis.apcall('LPUSH', key, unpack(elements))
end

function LG_funcs.mod_list(key)
    -- equally likely pops and pushes, we rely on the list size being large enough
    -- to "highly likely" not get emptied out by consequitve pops
    local action = math.random(1, 4)
    if action == 1 then
        redis.apcall('RPOP', key)
    elseif action == 2 then
        redis.apcall('LPOP', key)
    elseif action == 3 then
        redis.apcall('LPUSH', key, dragonfly.randstr(LG_funcs.esize))
    else
        redis.apcall('RPUSH', key, dragonfly.randstr(LG_funcs.esize))
    end
end

-- sets
-- store sets of blobs of default container/element sizes

function LG_funcs.add_set(key, keys)
    if #keys > 100 and math.random() < 0.05 then
        -- we assume that elements overlap with a very low proabiblity, so
        -- SDIFF is expected to be equal to the origin set.
        -- Repeating this operation too often can lead to two equal sets being chosen
        local i1 = math.random(#keys)
        local i2 = math.random(#keys)
        while i1 == i2 do
            i2 = math.random(#keys)
        end
        redis.apcall('SDIFFSTORE', key, keys[i1], keys[i2])
    else
        local elements = dragonfly.randstr(LG_funcs.esize, LG_funcs.csize)
        redis.apcall('SADD', key, unpack(elements))
    end
end

function LG_funcs.mod_set(key)
     -- equally likely pops and additions
    if math.random() < 0.5 then
        redis.apcall('SPOP', key)
    else
        redis.apcall('SADD', key, dragonfly.randstr(LG_funcs.esize))
    end
end


-- hashes
-- store  {to_string(i): value for i in [1, csize]},
-- where `value` is a random string for even indices and a number for odd indices

function LG_funcs.add_hash(key)
    local blobs  = dragonfly.randstr(LG_funcs.esize, LG_funcs.csize / 2)
    local htable = {}
    for i = 1,  LG_funcs.csize, 2 do
        htable[i * 2 - 1] = tostring(i)
        htable[i * 2] = math.random(0, 1000)
    end
    for i = 2,  LG_funcs.csize, 2 do
        htable[i * 2 - 1] = tostring(i)
        htable[i * 2] = blobs[i // 2]
    end
    redis.apcall('HSET', key, unpack(htable))
end

function LG_funcs.mod_hash(key)
    local idx = math.random(LG_funcs.csize)
    if idx % 2 == 1 then
        redis.apcall('HINCRBY', key, tostring(idx), 1)
    else
        redis.apcall('HSET', key, tostring(idx), dragonfly.randstr(LG_funcs.esize))
    end
end

-- sorted sets

function LG_funcs.add_zset(key, keys)
    -- TODO: We don't support ZDIFFSTORE
    local blobs  = dragonfly.randstr(LG_funcs.esize, LG_funcs.csize)
    local ztable = {}
    for i = 1,  LG_funcs.csize do
        ztable[i * 2 - 1] = tostring(i)
        ztable[i * 2] = blobs[i]
    end
    redis.apcall('ZADD', key, unpack(ztable))
end

function LG_funcs.mod_zset(key, dbsize)
    local action = math.random(1, 4)
    if action <= 2 then
        redis.apcall('ZADD', key, math.random(0, LG_funcs.csize * 2), dragonfly.randstr(LG_funcs.esize))
    elseif action == 3 then
        redis.apcall('ZPOPMAX', key)
    else
        redis.apcall('ZPOPMIN', key)
    end
end

-- json
-- store single list of integers inside object

function LG_funcs.add_json(key)
    -- generate single list of counters
    local seed = math.random(100)
    local counters = {}
    for i = 1, LG_funcs.csize do
        counters[i] = ((i + seed) * 123) % 701
    end
    redis.apcall('JSON.SET', key, '$', cjson.encode({counters = counters}))
end

function LG_funcs.mod_json(key, dbsize)
    local action = math.random(1, 4)
    if action == 1 then
        redis.apcall('JSON.ARRAPPEND', key, '$.counters', math.random(701))
    elseif action == 2 then
        redis.apcall('JSON.ARRPOP', key, '$.counters')
    elseif action == 3 then
        redis.apcall('JSON.NUMMULTBY', key, '$.counters[' .. math.random(LG_funcs.csize ) .. ']', 2)
    else
        redis.apcall('JSON.NUMINCRBY', key, '$.counters[' .. math.random(LG_funcs.csize ) .. ']', 1)
    end
end
