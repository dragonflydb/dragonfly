import async_timeout
import asyncio
import itertools
import logging
import pytest
import random
import redis.asyncio as aioredis

from . import dfly_args
from .seeder import DebugPopulateSeeder
from .utility import info_tick_timer, wait_for_replicas_state
from .instance import DflyInstanceFactory

BASIC_ARGS = {"port": 6379, "proactor_threads": 4, "tiered_prefix": "/tmp/tiered/backing"}


@pytest.mark.skip("Requires evaluating runner performance first")
@pytest.mark.opt_only
@dfly_args(BASIC_ARGS)
async def test_basic_memory_usage(async_client: aioredis.Redis):
    """
    Loading 1GB of mixed size strings (256b-16kb) will keep most of them on disk and thus RAM remains almost unused
    """

    seeder = DebugPopulateSeeder(
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
        "tiered_offload_threshold": "1.0",
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


@pytest.mark.exclude_epoll
@pytest.mark.opt_only
@dfly_args(
    {
        "proactor_threads": 2,
        "tiered_prefix": "/tmp/tiered/backing_master",
        "maxmemory": "2.0G",
        "cache_mode": True,
        "tiered_offload_threshold": "0.9",
        "tiered_upload_threshold": "0.2",
        "tiered_storage_write_depth": 100,
    }
)
async def test_full_sync(async_client: aioredis.Redis, df_factory: DflyInstanceFactory):
    replica = df_factory.create(
        proactor_threads=2,
        cache_mode=True,
        maxmemory="2.0G",
        tiered_prefix="/tmp/tiered/backing_replica",
        tiered_offload_threshold="0.8",
        tiered_storage_write_depth=1000,
    )
    replica.start()
    replica_client = replica.client()
    await async_client.execute_command("debug", "populate", "1700000", "key", "2000")
    await replica_client.replicaof(
        "localhost", async_client.connection_pool.connection_kwargs["port"]
    )
    logging.info("Waiting for replica to sync")
    try:
        async with async_timeout.timeout(200):
            await wait_for_replicas_state(replica_client)
    except asyncio.TimeoutError:
        master_info = await async_client.info("ALL")
        replica_info = await replica_client.info("ALL")
        pytest.fail(
            f"Replica did not sync in time. \nmaster: {master_info} \n\nreplica: {replica_info}"
        )
