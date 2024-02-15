local LG_funcs = {}

-- strings

function LG_funcs.add_string(key, dsize)
    local char = string.char(math.random(65, 90))
    redis.apcall('SET', key, string.rep(char, dsize))
end

function LG_funcs.mod_string(key, dsize)
    -- APPEND and SETRANGE are the only modifying operations for strings,
    -- issue APPEND rarely to not grow data too much
    if math.random() < 0.05 then
        redis.apcall('APPEND', key, '+')
    else
        local char = string.char(math.random(65, 90))
        local replacement = string.rep(char, math.random(0, dsize / 2))
        redis.apcall('SETRANGE', key, math.random(0, dsize / 2), replacement)
    end
end
