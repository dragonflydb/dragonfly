import pytest
import asyncio
import aioredis
import random
from itertools import count, chain, repeat

from .utility import *
from . import *


@pytest.mark.skip
@dfly_args({"proactor_threads": 12})
@pytest.mark.asyncio
async def test_memory_usage(async_pool):
    async def get_mem(client):
        data = await client.info('memory')
        return data['used_memory'], data['used_memory_rss'], data['used_memory_human'], data['used_memory_rss_human']

    async def run(keys, saturation):
        await client.execute_command(f"DEBUG MEMORYFUZZ {keys} {saturation}")
        return await get_mem(client)

    async def find_keynum(client, mem_target):
        keys = 100000
        while True:
            _, used_rss, _, human_rss = await run(keys, 1.0)
            if used_rss * 2 < base_memory:
                keys *= 2
            else:
                keys = int(keys * base_memory/used_rss)
                break
        return keys

    base_memory = 12_000_000_000  # 12 GB
    excess_target = 0.4  # 40% difference

    client = aioredis.Redis(connection_pool=async_pool)
    keys = await find_keynum(client, base_memory)

    for _ in range(0, 150):
        used, used_rss, human, human_rss = await run(keys, 0.4)
        excess = used_rss/used-1
        print(round(excess, 3), human, human_rss)
        if excess > excess_target:
            # wait for df to cleanup mess on its own
            pass
