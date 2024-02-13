#!lua flags=disable-atomicity
--[[
Script for quickly computing single 64bit hash for keys of types specified in ARGV[].
Keys of every type are sorted lexicographically to ensure consistent order.
]]--

-- import:hashlib --
-- import:utillib --

-- inputs
local requested_types = ARGV

local OUT_HASH = 0

local function process(type)
    local keys = LU_collect_keys('', type)
    local hfunc = LH_funcs[type]

    -- sort to provide consistent order
    table.sort(keys)
    for _, key in ipairs(keys) do
        -- add key to hash
        OUT_HASH = dragonfly.ihash(OUT_HASH, key)
        -- hand hash over to callback
        OUT_HASH = hfunc(key, OUT_HASH)
    end
end

for _, type in ipairs(requested_types) do
    process(string.lower(type))
end


return OUT_HASH
