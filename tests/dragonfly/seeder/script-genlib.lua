local LG_funcs = {}

function LG_funcs.init(dsize, csize, large_val_perc, large_val_sz, huge_value_csize)
    LG_funcs.dsize = dsize
    LG_funcs.csize = csize
    LG_funcs.esize = math.ceil(dsize / csize)
    LG_funcs.huge_value_percentage = large_val_perc
    LG_funcs.huge_value_size = large_val_sz
    LG_funcs.huge_value_csize = huge_value_csize
end

local huge_entries = 0

local function randstr(huge_entry)
    local str
    if huge_entry then
        str = dragonfly.randstr(LG_funcs.huge_value_size)
        huge_entries = huge_entries + 1
    else
        str = dragonfly.randstr(LG_funcs.esize)
    end
    return str
end

local function randstr_sequence(huge_entry)
    local strs
    if huge_entry then
        strs = dragonfly.randstr(LG_funcs.huge_value_size, LG_funcs.huge_value_csize)
        huge_entries = huge_entries + 1
    else
        strs = dragonfly.randstr(LG_funcs.esize, LG_funcs.csize)
    end
    return strs
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

function LG_funcs.add_list(key, keys)
    local is_huge = keys[key]
    redis.apcall('LPUSH', key, unpack(randstr_sequence(is_huge)))
end

function LG_funcs.mod_list(key, keys)
    -- equally likely pops and pushes, we rely on the list size being large enough
    -- to "highly likely" not get emptied out by consequitve pops
    local action = math.random(1, 4)
    if action == 1 then
        redis.apcall('RPOP', key)
    elseif action == 2 then
        redis.apcall('LPOP', key)
    elseif action == 3 then
      redis.apcall('LPUSH', key, randstr(false))
    else
      redis.apcall('RPUSH', key, randstr(false))
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
        local is_huge = keys[key]
        redis.apcall('SADD', key, unpack(randstr_sequence(is_huge)))
    end
end

function LG_funcs.mod_set(key, keys)
     -- equally likely pops and additions
    if math.random() < 0.5 then
        redis.apcall('SPOP', key)
    else
        local is_huge = keys[key]
        redis.apcall('SADD', key, randstr(false))
    end
end


-- hashes
-- store  {to_string(i): value for i in [1, csize]},
-- where `value` is a random string for even indices and a number for odd indices

function LG_funcs.add_hash(key, keys)
    local blobs
    local is_huge = keys[key]
    local limit = LG_funcs.csize
    if is_huge then
        limit = LG_funcs.huge_value_csize
        blobs  = dragonfly.randstr(LG_funcs.huge_value_size, limit)
        huge_entries = huge_entries + 1
    else
        blobs  = dragonfly.randstr(LG_funcs.esize, LG_funcs.csize / 2)
    end

    local htable = {}
    for i = 1, limit, 2 do
        htable[i * 2 - 1] = tostring(i)
        htable[i * 2] = math.random(0, 1000)
    end
    for i = 2, limit, 2 do
        htable[i * 2 - 1] = tostring(i)
        htable[i * 2] = blobs[i // 2]
    end
    redis.apcall('HSET', key, unpack(htable))
end

function LG_funcs.mod_hash(key, keys)
    local idx = math.random(LG_funcs.csize)
    if idx % 2 == 1 then
        redis.apcall('HINCRBY', key, tostring(idx), 1)
    else
      redis.apcall('HSET', key, tostring(idx), randstr(false))
    end
end

-- sorted sets

function LG_funcs.add_zset(key, keys)
    -- TODO: We don't support ZDIFFSTORE
    local is_huge = keys[key]
    local blobs = randstr_sequence(is_huge)

    local ztable = {}

    local limit = LG_funcs.csize
    if is_huge then
      limit = LG_funcs.huge_value_csize
    end

    for i = 1, limit do
        ztable[i * 2 - 1] = tostring(i)
        ztable[i * 2] = blobs[i]
    end
    redis.apcall('ZADD', key, unpack(ztable))
end

function LG_funcs.mod_zset(key, keys)
    local action = math.random(1, 4)
    if action <= 2 then
        local size = LG_funcs.csize * 2
        redis.apcall('ZADD', key, math.random(0, size), randstr(false))
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

function LG_funcs.get_huge_entries()
  return huge_entries
end
