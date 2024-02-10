-- collect all keys into table specific type on specific prefix. Uses SCAN--
local function LU_collect_keys(prefix, type)
    local pattern = prefix .. "*"
    local cursor = "0"
    local keys = {}
    repeat
        local result = redis.call("SCAN", cursor, "COUNT", 500, "TYPE", type, "MATCH", pattern)
        cursor = result[1]
        local scan_keys = result[2]
        for i, key in ipairs(scan_keys) do
            table.insert(keys, key)
        end
    until cursor == "0"
    return keys
end
