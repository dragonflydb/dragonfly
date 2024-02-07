import pytest
from redis import asyncio as aioredis
from .utility import *


@pytest.mark.asyncio
@pytest.mark.slow
@pytest.mark.parametrize(
    "type, keys, val_size, elements",
    [
        ("JSON", 300_000, 100, 100),
        # ("SET", 500_000, 100, 100),
        ("HSET", 500_000, 100, 100),
        # ("ZSET", 400_000, 100, 100),
        # ("LIST", 500_000, 100, 100),
        ("STRING", 10_000_000, 1000, 1),
    ],
)
async def test_acl_setuser(async_client, type, keys, val_size, elements):
    # Create a Dragonfly and fill it up with `type` until it reaches `min_rss`, then make sure that
    # the gap between used_memory and rss is no more than `max_unaccounted`.
    min_rss = 5 * 1024 * 1024 * 1024  # 5gb
    max_unaccounted = 200 * 1024 * 1024  # 200mb

    await async_client.execute_command(
        f"DEBUG POPULATE {keys} {type} {val_size} RAND TYPE {type} ELEMENTS {elements}"
    )

    await asyncio.sleep(2)  # Wait for another RSS heartbeat update in Dragonfly

    info = await async_client.info("memory")
    print(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert info["used_memory"] > min_rss, "Weak testcase: too little used memory"
    assert info["used_memory_rss"] - info["used_memory"] < max_unaccounted
