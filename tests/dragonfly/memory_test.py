import pytest
from redis import asyncio as aioredis
from .utility import *
import logging
from . import dfly_args
from .instance import DflyInstance, DflyInstanceFactory


@pytest.mark.slow
@pytest.mark.opt_only
@pytest.mark.parametrize(
    "type, keys, val_size, elements",
    [
        ("JSON", 200_000, 100, 100),
        ("SET", 280_000, 100, 100),
        ("HASH", 250_000, 100, 100),
        ("ZSET", 250_000, 100, 100),
        ("LIST", 300_000, 100, 100),
        ("STRING", 3_500_000, 1000, 1),
        ("STREAM", 280_000, 100, 100),
    ],
)
# We limit to 5gb just in case to sanity check the gh runner. Otherwise, if we ask for too much
# memory it might force the gh runner to run out of memory (since OOM killer might not even
# get a chance to run).
@dfly_args({"proactor_threads": 4, "maxmemory": "5gb"})
async def test_rss_used_mem_gap(df_factory, type, keys, val_size, elements):
    dbfilename = f"dump_{tmp_file_name()}"
    instance = df_factory.create(dbfilename=dbfilename)
    instance.start()
    # Create a Dragonfly and fill it up with `type` until it reaches `min_rss`, then make sure that
    # the gap between used_memory and rss is no more than `max_unaccounted_ratio`.
    min_rss = 3 * 1024 * 1024 * 1024  # 3gb
    max_unaccounted = 200 * 1024 * 1024  # 200mb

    # There is a big rss spike when this test is ran in one the gh runners (not the self hosted)
    # and it fails. This rss spike is not observed locally or on our self host runner so
    # this adjustment is mostly for CI
    if type == "STREAM":
        max_unaccounted = max_unaccounted * 3

    client = instance.client()
    await asyncio.sleep(1)  # Wait for another RSS heartbeat update in Dragonfly

    cmd = f"DEBUG POPULATE {keys} k {val_size} RAND TYPE {type} ELEMENTS {elements}"
    logging.info(f"Running {cmd}")
    await client.execute_command(cmd)

    await asyncio.sleep(2)  # Wait for another RSS heartbeat update in Dragonfly

    async def check_memory():
        info = await client.info("memory")
        logging.info(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
        assert info["used_memory"] > min_rss, "Weak testcase: too little used memory"
        delta = info["used_memory_rss"] - info["used_memory"]
        # It could be the case that the machine is configured to use swap if this assertion fails
        assert delta > 0
        assert delta < max_unaccounted

        if type != "STRING" and type != "JSON":
            # STRINGs keep some of the data inline, so not all of it is accounted in object_used_memory
            # We have a very small over-accounting bug in JSON
            assert info["object_used_memory"] > keys * elements * val_size
            assert info["used_memory"] > info["object_used_memory"]

    await check_memory()

    await client.execute_command("SAVE", "DF")
    await client.execute_command("DFLY", "LOAD", f"{dbfilename}-summary.dfs")

    await check_memory()
    await client.execute_command("FLUSHALL")


@pytest.mark.asyncio
@dfly_args(
    {
        "maxmemory": "512mb",
        "proactor_threads": 2,
        "rss_oom_deny_ratio": 0.5,
    }
)
@pytest.mark.parametrize("admin_port", [0, 1112])
async def test_rss_oom_ratio(df_factory: DflyInstanceFactory, admin_port):
    """
    Test dragonfly rejects denyoom commands and new connections when rss memory is above maxmemory*rss_oom_deny_ratio
    Test dragonfly does not rejects when rss memory goes below threshold
    """
    df_server = df_factory.create(admin_port=admin_port)
    df_server.start()

    client = df_server.client()
    await client.execute_command("DEBUG POPULATE 10000 key 40000 RAND")

    await asyncio.sleep(1)  # Wait for another RSS heartbeat update in Dragonfly

    new_client = df_server.admin_client() if admin_port else df_server.client()
    await new_client.ping()

    info = await new_client.info("memory")
    logging.debug(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')

    reject_limit = 256 * 1024 * 1024  # 256mb
    assert info["used_memory_rss"] > reject_limit

    # get command from existing connection should not be rejected
    await client.execute_command("get x")

    # reject set due to oom
    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command("set x y")

    if admin_port:
        # new client create should also fail if admin port was set
        client = df_server.client()
        with pytest.raises(redis.exceptions.ConnectionError):
            await client.ping()

    # flush to free memory
    await new_client.flushall()

    await asyncio.sleep(2)  # Wait for another RSS heartbeat update in Dragonfly

    info = await new_client.info("memory")
    logging.debug(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert info["used_memory_rss"] < reject_limit

    # new client create shoud not fail after memory usage decrease
    client = df_server.client()
    await client.execute_command("set x y")


@pytest.mark.asyncio
@dfly_args(
    {
        "maxmemory": "512mb",
        "proactor_threads": 1,
    }
)
async def test_eval_with_oom(df_factory: DflyInstanceFactory):
    """
    Test running eval commands when dragonfly returns OOM on write commands and check rss memory
    This test was writen after detecting memory leak in script runs on OOM state
    """
    df_server = df_factory.create()
    df_server.start()

    client = df_server.client()
    await client.execute_command("DEBUG POPULATE 20000 key 40000 RAND")

    await asyncio.sleep(1)  # Wait for another RSS heartbeat update in Dragonfly

    info = await client.info("memory")
    logging.debug(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')

    reject_limit = 512 * 1024 * 1024  # 256mb
    assert info["used_memory"] > reject_limit
    rss_before_eval = info["used_memory_rss"]

    pipe = client.pipeline(transaction=False)
    MSET_SCRIPT = """
        redis.call('MSET', KEYS[1], ARGV[1], KEYS[2], ARGV[2])
    """

    for _ in range(20):
        for _ in range(8000):
            pipe.eval(MSET_SCRIPT, 2, "x1", "y1", "x2", "y2")
        # reject mset due to oom
        with pytest.raises(redis.exceptions.ResponseError):
            await pipe.execute()

    await asyncio.sleep(1)  # Wait for another RSS heartbeat update in Dragonfly

    info = await client.info("memory")
    logging.debug(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert rss_before_eval * 1.01 > info["used_memory_rss"]


@pytest.mark.skip("rss eviction disabled")
@pytest.mark.asyncio
@dfly_args(
    {
        "proactor_threads": 1,
        "cache_mode": "true",
        "maxmemory": "5gb",
        "rss_oom_deny_ratio": 0.8,
        "max_eviction_per_heartbeat": 100,
    }
)
async def test_cache_eviction_with_rss_deny_oom(
    async_client: aioredis.Redis,
):
    """
    Test to verify that cache eviction is triggered even if used memory is small but rss memory is above limit
    """

    max_memory = 5 * 1024 * 1024 * 1024  # 5G
    rss_max_memory = int(max_memory * 0.8)

    data_fill_size = int(0.9 * rss_max_memory)  # 95% of rss_max_memory

    val_size = 1024 * 5  # 5 kb
    num_keys = data_fill_size // val_size

    await async_client.execute_command("DEBUG", "POPULATE", num_keys, "key", val_size)
    # Test that used memory is less than 90% of max memory
    memory_info = await async_client.info("memory")
    assert (
        memory_info["used_memory"] < max_memory * 0.9
    ), "Used memory should be less than 90% of max memory."
    assert (
        memory_info["used_memory_rss"] > rss_max_memory * 0.9
    ), "RSS memory should be less than 90% of rss max memory (max_memory * rss_oom_deny_ratio)."

    # Get RSS memory after creating new connections
    memory_info = await async_client.info("memory")
    while memory_info["used_memory_rss"] > rss_max_memory * 0.9:
        await asyncio.sleep(1)
        memory_info = await async_client.info("memory")
        logging.info(
            f'Current rss: {memory_info["used_memory_rss"]}. rss eviction threshold: {rss_max_memory * 0.9}.'
        )
        stats_info = await async_client.info("stats")
        logging.info(f'Current evicted: {stats_info["evicted_keys"]}. Total keys: {num_keys}.')
