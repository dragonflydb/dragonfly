import json

import pytest
import redis

pytestmark = []
pytestmark.extend(
    [
        pytest.mark.asyncio,
    ]
)

lua_modules_test = pytest.importorskip("lupa")


@pytest.mark.load_lua_modules("cjson")
async def test_async_asgi_ratelimit_script(async_redis: redis.Redis):
    script = """
local ruleset = cjson.decode(ARGV[1])

-- Set limits
for i, key in pairs(KEYS) do
    redis.call('SET', key, ruleset[key][1], 'EX', ruleset[key][2], 'NX')
end

-- Check limits
for i = 1, #KEYS do
    local value = redis.call('GET', KEYS[i])
    if value and tonumber(value) < 1 then
        return ruleset[KEYS[i]][2]
    end
end

-- Decrease limits
for i, key in pairs(KEYS) do
    redis.call('DECR', key)
end
return 0
"""

    script = async_redis.register_script(script)
    ruleset = {"path:get:user:name": (1, 1)}
    await script(keys=list(ruleset.keys()), args=[json.dumps(ruleset)])


@pytest.mark.load_lua_modules("cjson")
def test_asgi_ratelimit_script(r: redis.Redis):
    script = """
local ruleset = cjson.decode(ARGV[1])

-- Set limits
for i, key in pairs(KEYS) do
    redis.call('SET', key, ruleset[key][1], 'EX', ruleset[key][2], 'NX')
end

-- Check limits
for i = 1, #KEYS do
    local value = redis.call('GET', KEYS[i])
    if value and tonumber(value) < 1 then
        return ruleset[KEYS[i]][2]
    end
end

-- Decrease limits
for i, key in pairs(KEYS) do
    redis.call('DECR', key)
end
return 0
"""

    script = r.register_script(script)
    ruleset = {"path:get:user:name": (1, 1)}
    script(keys=list(ruleset.keys()), args=[json.dumps(ruleset)])
