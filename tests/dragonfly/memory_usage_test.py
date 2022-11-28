import pytest
import asyncio
import aioredis
import random
from itertools import count, chain, repeat

from .utility import *
from . import *


@pytest.mark.skip
@dfly_args({"proactor_threads": 4})
@pytest.mark.asyncio
async def test_memory_usage(async_pool):
    async def get_mem(client):
        data = await client.info('memory')
        return data['used_memory'], data['used_memory_rss'], data['used_memory_human'], data['used_memory_rss_human']

    async def run(keys, del_prob):
        await client.execute_command(f"DEBUG MEMORYFUZZ {keys} {del_prob}")
        return await get_mem(client)

    async def find_keynum(client, mem_target):
        keys = 1000
        while True:
            _, used_rss, _, human_rss = await run(keys, 0)
            await client.flushall()
            print(f"Keys {keys} Mem {human_rss} Ratio {used_rss/keys}")
            if used_rss * 2 < mem_target:
                keys *= 2
            else:
                keys = int(keys * mem_target/used_rss)
                break
        return keys

    base_memory = 8_000_000_000  # 8 GB
    excess_target = 0.5  # 50% difference

    client = aioredis.Redis(connection_pool=async_pool)
    keys = await find_keynum(client, base_memory)

    for _ in range(0, 150):
        used, used_rss, human, human_rss = await run(keys, 0.6)
        excess = used_rss/used-1
        print(round(excess, 3), human, human_rss)
        if excess > excess_target:
            print("bad stats :*(")
            # wait for df to cleanup mess on its own
            pass
