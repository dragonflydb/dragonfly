import asyncio
from redis import asyncio as aioredis

import pytest


@pytest.mark.parametrize("index", range(50))
class TestBlPop:
    async def async_blpop(client: aioredis.Redis):
        return await client.blpop(["list1{t}", "list2{t}", "list2{t}", "list1{t}"], 0.5)

    async def blpop_mult_keys(async_client: aioredis.Redis, key: str, val: str):
        task = asyncio.create_task(TestBlPop.async_blpop(async_client))
        await async_client.lpush(key, val)
        result = await asyncio.wait_for(task, 3)
        assert result[1] == val
        watched = await async_client.execute_command("DEBUG WATCHED")
        assert watched == ["awaked", [], "watched", []]

    async def test_blpop_multiple_keys(self, async_client: aioredis.Redis, index):
        await TestBlPop.blpop_mult_keys(async_client, "list1{t}", "a")
        await TestBlPop.blpop_mult_keys(async_client, "list2{t}", "b")
