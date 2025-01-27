import async_timeout
import asyncio
import itertools
import pytest
import random
import redis.asyncio as aioredis

from . import dfly_args
from .seeder import StaticSeeder
from .utility import info_tick_timer


BASIC_ARGS = {"port": 6379, "proactor_threads": 4, "tiered_prefix": "/tmp/tiered/backing"}


@pytest.mark.skip("Requires evaluating runner performance first")
@pytest.mark.opt_only
@dfly_args(BASIC_ARGS)
async def test_basic_memory_usage(async_client: aioredis.Redis):
    """
    Loading 1GB of mixed size strings (256b-16kb) will keep most of them on disk and thus RAM remains almost unused
    """

    seeder = StaticSeeder(
        key_target=200_000, data_size=2048, variance=8, samples=100, types=["STRING"]
    )
    await seeder.run(async_client)

    # Wait for tiering stashes
    async for info, breaker in info_tick_timer(async_client, section="TIERED"):
        with breaker:
            assert info["tiered_entries"] > 195_000

    info = await async_client.info("ALL")
    assert info["num_entries"] == 200_000

    assert info["tiered_entries"] > 195_000  # some remain in unfilled small bins
    assert (
        info["tiered_allocated_bytes"] > 195_000 * 2048 * 0.8
    )  # 0.8 just to be sure because it fluctuates due to variance

    assert info["used_memory"] < 50 * 1024 * 1024
    assert (
        info["used_memory_rss"] < 500 * 1024 * 1024
    )  # the grown table itself takes up lots of space


@pytest.mark.exclude_epoll
@pytest.mark.opt_only
@dfly_args(
    {
        **BASIC_ARGS,
        "maxmemory": "1G",
        "tiered_offload_threshold": "0.0",
        "tiered_storage_write_depth": 1000,
    }
)
async def test_mixed_append(async_client: aioredis.Redis):
    """
    Issue conflicting mixed APPEND calls for a limited subset of keys with aggressive offloading in the background.
    Make sure no appends were lost
    """

    # Generate operations and shuffle them, key number `k` will have `k` append operations
    key_range = list(range(100, 300))
    ops = list(itertools.chain(*map(lambda k: itertools.repeat(k, k), key_range)))
    random.shuffle(ops)

    # Split list into n workers and run it
    async def run(sub_ops):
        p = async_client.pipeline(transaction=False)
        for k in sub_ops:
            p.append(f"k{k}", 10 * "x")
        await p.execute()

    n = 20
    await asyncio.gather(*(run(ops[i::n]) for i in range(n)))

    async for info, breaker in info_tick_timer(async_client, section="TIERED"):
        with breaker:
            assert info["tiered_entries"] > len(key_range) / 5

    # Verify lengths
    p = async_client.pipeline(transaction=False)
    for k in key_range:
        p.strlen(f"k{k}")
    res = await p.execute()

    assert res == [10 * k for k in key_range]
