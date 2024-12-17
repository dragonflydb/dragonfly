--!df flags=disable-atomicity

--[[
Script for quickly generating various data
]] --
-- import:genlib --
-- import:utillib --

-- inputs: unit identifiers
local prefix = ARGV[1]
local type = ARGV[2]
local key_counter = tonumber(ARGV[3])
local stop_key = ARGV[4]

-- inputs: task specific
local key_target = tonumber(ARGV[5])
local total_ops = tonumber(ARGV[6])
local min_dev = tonumber(ARGV[7])
local data_size = tonumber(ARGV[8])
local collection_size = tonumber(ARGV[9])
-- Probability of each key in key_target to be a big value
local huge_value_target = tonumber(ARGV[10])
local huge_value_size = tonumber(ARGV[11])

-- collect all keys belonging to this script
-- assumes exclusive ownership
local keys = LU_collect_keys(prefix, type)

LG_funcs.init(data_size, collection_size, huge_value_target, huge_value_size)
local addfunc = LG_funcs['add_' .. string.lower(type)]
local modfunc = LG_funcs['mod_' .. string.lower(type)]
local huge_entries = LG_funcs["get_huge_entries"]

local function action_add()
    local key = prefix .. tostring(key_counter)
    local op_type = string.lower(type)
    key_counter = key_counter + 1

    table.insert(keys, key)
    addfunc(key, keys)
end

local function action_mod()
    local key = keys[math.random(#keys)]
    modfunc(key, keys)
end

local function action_del()
    local key_idx = math.random(#keys)
    keys[key_idx], keys[#keys] = keys[#keys], keys[key_idx]

    local key = table.remove(keys)
    redis.acall('DEL', key)
end

-- set equilibrium point as key target, see intensity calculations below
local real_target = key_target
key_target = key_target / 0.956

-- accumulative probabilities: [add, add + delete, modify = 1-( add + delete) ]
local p_add = 0
local p_del = 0

local counter = 0
while true do
    counter = counter + 1

    -- break if we reached target ops
    if total_ops > 0 and counter > total_ops then
        break
    end

    -- break if we reached our target deviation
    if min_dev > 0 and math.abs(#keys - real_target) / real_target < min_dev then
        break
    end

    -- break if stop key was set (every 100 ops to not slow down)
    if stop_key ~= '' and counter % 100 == 0 and redis.call('EXISTS', stop_key) then
        break
    end

    -- fast path, if we have less than half of the target, always grow
    if #keys * 2 < key_target then
        action_add()
        goto continue
    end

    -- update probability only every 10 iterations
    if counter % 10 == 0 then
        -- calculate intensity (not normalized probabilities)
        -- please see attached plots in PR to understand convergence
        -- https://github.com/dragonflydb/dragonfly/pull/2556

        -- the add intensity is monotonically decreasing with keycount growing,
        -- the delete intensity is monotonically increasing with keycount growing,
        -- the point where the intensities are equal is the equilibrium point,
        -- based on the formulas it's ~0.956 * key_target
        local i_add = math.max(0, 1 - (#keys / key_target) ^ 16)
        local i_del = (#keys / key_target) ^ 16

        -- we are only interested in large amounts of modification commands when we are in an
        -- equilibrium, where there are no low intensities
        local i_mod = math.max(0, 7 * math.min(i_add, i_del) ^ 3)

        -- transform intensities to [0, 1] probability ranges
        local sum = i_add + i_del + i_mod
        p_add = i_add / sum
        p_del = p_add + i_del / sum
    end

    -- generate random action
    local p = math.random()
    if p < p_add then
        action_add()
    elseif p < p_del then
        action_del()
    else
        action_mod()
    end

    ::continue::
end

-- clear stop key
if stop_key ~= '' then
    redis.call('DEL', stop_key)
end

return tostring(key_counter) .. " " .. tostring(huge_entries())
