--!df flags=disable-atomicity
--[[
Script for quickly computing single 64bit hash for keys of types specified in ARGV[].
Keys of every type are sorted lexicographically to ensure consistent order.
]]--

-- import:hashlib --
-- import:utillib --

-- inputs
local type = ARGV[1]

local OUT_HASH = 0

local function process(type)
    local keys = LU_collect_keys('', type)
    local hfunc = LH_funcs[type]

    -- sort to provide consistent order
    table.sort(keys)
    for _, key in ipairs(keys) do
        -- hand hash over to callback
        OUT_HASH = hfunc(key, OUT_HASH)
    end
end

process(string.lower(type))

return OUT_HASH
