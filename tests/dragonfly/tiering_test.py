from . import dfly_args

import async_timeout
import asyncio
import redis.asyncio as aioredis

BASIC_ARGS = {"port": 6379, "proactor_threads": 1, "tiered_prefix_v2": "/tmp/tiering_test_backing"}


# remove once proudct requirments are tested
@dfly_args(BASIC_ARGS)
async def test_tiering_simple(async_client: aioredis.Redis):
    fill_script = """#!lua flags=disable-atomicity
        for i = 1, 100 do
            redis.call('SET', 'k' .. i, string.rep('a', 3000))
        end
    """

    # Store 100 entries
    await async_client.eval(fill_script, 0)

    # Wait for all to be offloaded
    with async_timeout.timeout(1):
        info = await async_client.info("TIERED_V2")
        while info["tiered_v2_total_stashes"] != 100:
            info = await async_client.info("TIERED_V2")
            await asyncio.sleep(0.1)
        assert 3000 * 100 <= info["tiered_v2_allocated_bytes"] <= 4096 * 100

    # Fetch back
    for key in (f"k{i}" for i in range(1, 100 + 1)):
        assert len(await async_client.execute_command("GET", key)) == 3000
    assert (await async_client.info("TIERED_V2"))["tiered_v2_total_fetches"] == 100

    # Store again
    await async_client.eval(fill_script, 0)

    # Wait to be deleted
    with async_timeout.timeout(1):
        while (await async_client.info("TIERED_V2"))["tiered_v2_allocated_bytes"] > 0:
            await asyncio.sleep(0.1)
