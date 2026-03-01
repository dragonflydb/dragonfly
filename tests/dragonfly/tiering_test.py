import async_timeout
import asyncio
import itertools
import logging
import pytest
import random
import redis.asyncio as aioredis

from . import dfly_args
from .seeder import DebugPopulateSeeder, Seeder as SeederV2
from .utility import (
    info_tick_timer,
    wait_for_replicas_state,
    check_all_replicas_finished,
)
from .instance import DflyInstance, DflyInstanceFactory

BASIC_ARGS = {
    "port": 6379,
    "proactor_threads": 4,
    "tiered_prefix": "/tmp/tiered/backing",
    "tiered_offload_threshold": "1.0",  # offload immediately
    "tiered_storage_write_depth": 1000,
    "maxmemory": "1G",
}


@pytest.mark.large
@pytest.mark.opt_only
@dfly_args({**BASIC_ARGS, "tiered_experimental_cooling": "false"})
async def test_basic_memory_usage(async_client: aioredis.Redis):
    """
    Loading 1GB of mixed size strings (256b-16kb) will keep most of them on disk and thus RAM remains almost unused
    """

    seeder = DebugPopulateSeeder(
        key_target=200_000, data_size=2048, variance=8, samples=100, types=["STRING"]
    )
    await seeder.run(async_client)

    # Wait for tiering stashes
    async for info, breaker in info_tick_timer(async_client, section="TIERED", timeout=60):
        with breaker:
            assert info["tiered_entries"] > 195_000

    info = await async_client.info("ALL")
    assert info["num_entries"] == 200_000

    assert (
        info["tiered_allocated_bytes"] > 195_000 * 2048 * 0.8
    )  # 0.8 just to be sure because it fluctuates due to variance

    assert info["used_memory"] < 50 * 1024 * 1024
    assert (
        info["used_memory_rss"] < 500 * 1024 * 1024
    )  # the grown table itself takes up lots of space


@pytest.mark.large
@pytest.mark.exclude_epoll
@pytest.mark.opt_only
@dfly_args(
    {
        **BASIC_ARGS,
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


@pytest.mark.skip
@pytest.mark.large
@pytest.mark.exclude_epoll
@pytest.mark.opt_only
@dfly_args(
    {
        "proactor_threads": 2,
        "tiered_prefix": "/tmp/tiered/backing_master",
        "maxmemory": "512MB",
        "cache_mode": True,
        "tiered_offload_threshold": "0.6",
        "tiered_upload_threshold": "0.2",
        "tiered_storage_write_depth": 1500,
    }
)
async def test_replication(
    async_client: aioredis.Redis, df_server: DflyInstance, df_factory: DflyInstanceFactory
):
    """
    Test replication with tiered storage for strings
    """

    # Fill master with values
    seeder = DebugPopulateSeeder(key_target=400000, data_size=2000, samples=100, types=["STRING"])
    await seeder.run(async_client)

    # Start replica
    replica = df_factory.create(
        proactor_threads=2,
        cache_mode=True,
        maxmemory="512MB",
        tiered_prefix="/tmp/tiered/backing_replica",
        tiered_offload_threshold="0.5",
        tiered_storage_write_depth=1500,
    )
    replica.start()
    replica_client = replica.client()

    # Get some keys and start tasks that append to values
    keys = await async_client.keys()

    async def fill_job():
        for i, key in enumerate(keys):
            await async_client.append(key, f":{i}:")
            await asyncio.sleep(0.005)  # limit qps

    fill_tasks = [asyncio.create_task(fill_job()) for _ in range(3)]

    # Start replication
    await replica_client.replicaof("localhost", df_server.port)
    logging.info("Waiting for replica to sync")

    # Wait for replication to finish
    try:
        async with async_timeout.timeout(500):
            await wait_for_replicas_state(replica_client)
    except asyncio.TimeoutError:
        master_info = await async_client.info("ALL")
        replica_info = await replica_client.info("ALL")
        pytest.fail(
            f"Replica did not sync in time. \nmaster: {master_info} \n\nreplica: {replica_info}"
        )

    # cancel filler and wait for replica to catch up
    for task in fill_tasks:
        task.cancel()
    await asyncio.gather(*fill_tasks, return_exceptions=True)
    await check_all_replicas_finished([replica_client], async_client, timeout=500)

    #
    # Check that everything is in sync
    hashes = await asyncio.gather(
        *(SeederV2.capture(c, types=["STRING"]) for c in [async_client, replica_client])
    )

    if len(set(hashes)) != 1:
        from itertools import zip_longest

        def batcher(iterable, n):
            args = [iter(iterable)] * n
            return zip_longest(*args, fillvalue=None)

        for keys in batcher(async_client.scan_iter(count=100), 10):
            values_master = await async_client.mget(keys)
            values_replica = await replica_client.mget(keys)
            assert values_master == values_replica
        assert False, "Inconsistency detected, but key not determined"
