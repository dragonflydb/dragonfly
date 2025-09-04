import pytest
import asyncio
from redis import asyncio as aioredis
from .utility import *
import logging
from . import dfly_args
from .instance import DflyInstance, DflyInstanceFactory


def extract_fragmentation_waste(memory_arena):
    """
    Extracts the fragmentation waste from the memory arena info.
    """
    match = re.search(r"fragmentation waste:\s*([0-9.]+)%", memory_arena)
    assert match.group(1) is not None
    return float(match.group(1))


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
async def test_rss_used_mem_gap(df_factory, type, keys, val_size, elements):
    dbfilename = f"dump_{tmp_file_name()}"
    instance = df_factory.create(
        proactor_threads=2,
        maxmemory="5gb",
        dbfilename=dbfilename,
        compression_mode=0,
        serialization_max_chunk_size=8192,
        num_shards=2,
    )
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
        assert delta > 0, info
        assert delta < max_unaccounted, info

        if type != "STRING" and type != "JSON":
            # STRINGs keep some of the data inline, so not all of it is accounted in object_used_memory
            # We have a very small over-accounting bug in JSON
            assert info["object_used_memory"] > keys * elements * val_size
            assert info["used_memory"] > info["object_used_memory"]

    await check_memory()

    assert await client.execute_command("SAVE", "DF") == True
    assert await client.execute_command("DFLY", "LOAD", f"{dbfilename}-summary.dfs") == "OK"
    #
    await check_memory()
    await client.execute_command("FLUSHALL")


#


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


async def test_eviction_on_rss_treshold(df_factory: DflyInstanceFactory):
    max_memory = 1024 * 1024**2  # 10242mb

    df_server = df_factory.create(
        proactor_threads=3,
        cache_mode="yes",
        maxmemory=max_memory,
        enable_heartbeat_eviction="false",
    )
    df_server.start()
    client = df_server.client()

    data_fill_size = int(0.80 * max_memory)  # 85% of max_memory

    val_size = 1024 * 5  # 5 kb
    num_keys = data_fill_size // val_size

    await client.execute_command("DEBUG", "POPULATE", num_keys, "key", val_size)

    # Names choosen so that are found on different shards
    for name in ["mylist_1", "mylist_2"]:
        for i in range(1, 1000):
            rand_str = "".join(random.choices(string.ascii_letters, k=val_size))
            await client.execute_command(f"LPUSH {name} {rand_str}")

    await client.execute_command(f"STICK mylist_1")
    await client.execute_command(f"STICK mylist_2")

    await client.execute_command("CONFIG SET enable_heartbeat_eviction true")

    memory_info_before = await client.info("memory")

    # This will increase only RSS memory above treshold (and also above rss_memory_limit)
    p = client.pipeline()
    for _ in range(50):
        p.execute_command("LRANGE mylist_1 0 -1")
        p.execute_command("LRANGE mylist_2 0 -1")
    await p.execute()

    # Wait for some time
    await asyncio.sleep(3)
    memory_info_after = await client.info("memory")

    assert memory_info_after["used_memory"] < memory_info_before["used_memory"]
    assert memory_info_after["used_memory_rss"] < memory_info_before["used_memory_rss"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proactor_threads_param, maxmemory_param",
    [(1, 6 * (1024**3)), (4, 6 * (1024**3))],
)
async def test_cache_eviction_with_rss_deny_oom_simple_case(
    df_factory: DflyInstanceFactory,
    proactor_threads_param,
    maxmemory_param,
):
    """
    Test to verify that cache eviction is triggered even if used memory is small but rss memory is above limit
    """
    df_server = df_factory.create(
        proactor_threads=proactor_threads_param,
        cache_mode="true",
        maxmemory=maxmemory_param,
        rss_oom_deny_ratio=0.8,
    )
    df_server.start()

    async_client = df_server.client()

    max_memory = maxmemory_param
    rss_oom_deny_ratio = 0.8
    eviction_memory_budget_threshold = 0.1  # 10% of max_memory

    rss_eviction_threshold = max_memory * (rss_oom_deny_ratio - eviction_memory_budget_threshold)

    data_fill_size = int((rss_oom_deny_ratio + 0.05) * max_memory)  # 85% of max_memory

    val_size = 1024 * 5  # 5 kb
    num_keys = data_fill_size // val_size

    await async_client.execute_command("DEBUG", "POPULATE", num_keys, "key", val_size)

    # Test that used memory is less than 90% of max memory to not to start eviction based on used_memory
    memory_info = await async_client.info("memory")
    assert (
        memory_info["used_memory"] < max_memory * 0.9
    ), "Used memory should be less than 90% of max memory to not to start eviction based on used_memory."
    assert (
        memory_info["used_memory_rss"] > max_memory * rss_oom_deny_ratio
    ), "Used RSS memory should be more than 80% of rss max memory (max_memory * rss_oom_deny_ratio) to start eviction based on rss memory usage."

    memory_info = await async_client.info("memory")
    prev_evicted_keys = 0
    evicted_keys_repeat_count = 0
    while True:
        # Wait for some time
        await asyncio.sleep(1)

        memory_info = await async_client.info("memory")
        logging.info(
            f'Current used memory: {memory_info["used_memory"]}, current used rss: {memory_info["used_memory_rss"]}, rss eviction threshold: {rss_eviction_threshold}.'
        )

        stats_info = await async_client.info("stats")
        logging.info(f'Current evicted: {stats_info["evicted_keys"]}. Total keys: {num_keys}.')

        # Check if evicted keys are not increasing
        if prev_evicted_keys == stats_info["evicted_keys"]:
            evicted_keys_repeat_count += 1
        else:
            prev_evicted_keys = stats_info["evicted_keys"]
            evicted_keys_repeat_count = 1

        if evicted_keys_repeat_count > 2:
            break

    # Wait for some time
    await asyncio.sleep(3)

    memory_arena = await async_client.execute_command("MEMORY", "ARENA")
    fragmentation_waste = extract_fragmentation_waste(memory_arena)
    logging.info(f"Memory fragmentation waste: {fragmentation_waste}")
    assert fragmentation_waste < 12.0, "Memory fragmentation waste should be less than 12%."

    # Assert that no more keys are evicted
    memory_info = await async_client.info("memory")
    stats_info = await async_client.info("stats")

    assert memory_info["used_memory"] > max_memory * (
        rss_oom_deny_ratio - eviction_memory_budget_threshold - 0.05
    ), "We should not evict all items."
    assert memory_info["used_memory"] < max_memory * (
        rss_oom_deny_ratio - eviction_memory_budget_threshold
    ), "Used memory should be smaller than threshold."
    assert memory_info["used_memory_rss"] > max_memory * (
        rss_oom_deny_ratio - eviction_memory_budget_threshold - 0.05
    ), "We should not evict all items."

    evicted_keys = stats_info["evicted_keys"]
    # We may evict slightly more than prev_evicted_keys due to gaps in RSS memory usage
    assert (
        evicted_keys > 0
        and evicted_keys >= prev_evicted_keys
        and evicted_keys <= prev_evicted_keys * 1.0015
    ), "We should not evict more items."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proactor_threads_param, maxmemory_param",
    [(1, 6 * (1024**3)), (4, 6 * (1024**3))],
)
async def test_cache_eviction_with_rss_deny_oom_two_waves(
    df_factory: DflyInstanceFactory, proactor_threads_param, maxmemory_param
):
    """
    Test to verify that cache eviction is triggered even if used memory is small but rss memory is above limit
    It is similar to the test_cache_eviction_with_rss_deny_oom_simple_case but here we have two waves of data filling:
    1. First wave fills the instance to 85% of max memory, which is above rss_oom_deny_ratio.
    2. Then we wait for eviction to happen based on rss memory usage. After eviction we should have 70% of max memory used.
    3. Second wave fills the instance to 90% of max memory, which is above rss_oom_deny_ratio.
    4. Second time eviction should happen
    """
    df_server = df_factory.create(
        proactor_threads=proactor_threads_param,
        cache_mode="true",
        maxmemory=maxmemory_param,
        rss_oom_deny_ratio=0.8,
    )
    df_server.start()

    async_client = df_server.client()

    max_memory = maxmemory_param
    rss_oom_deny_ratio = 0.8
    eviction_memory_budget_threshold = 0.1  # 10% of max_memory

    rss_eviction_threshold = max_memory * (rss_oom_deny_ratio - eviction_memory_budget_threshold)

    # first wave fills 85% of max memory
    # second wave fills 17% of max memory
    data_fill_size = [
        int((rss_oom_deny_ratio + 0.05) * max_memory),
        int((1 - rss_oom_deny_ratio - 0.03) * max_memory),
    ]

    val_size = 1024 * 5  # 5 kb

    for i in range(2):
        if i > 0:
            await asyncio.sleep(2)

        num_keys = data_fill_size[i] // val_size
        logging.info(
            f"Populating data for wave {i}. Data fill size: {data_fill_size[i]}. Number of keys: {num_keys}."
        )
        await async_client.execute_command("DEBUG", "POPULATE", num_keys, f"key{i}", val_size)

        # Test that used memory is less than 90% of max memory to not to start eviction based on used_memory
        memory_info = await async_client.info("memory")
        assert (
            memory_info["used_memory"] < max_memory * 0.9
        ), "Used memory should be less than 90% of max memory to not to start eviction based on used_memory."
        assert (
            memory_info["used_memory_rss"] > max_memory * rss_oom_deny_ratio
        ), "Used RSS memory should be more than 80% of rss max memory (max_memory * rss_oom_deny_ratio) to start eviction based on rss memory usage."

        memory_info = await async_client.info("memory")
        prev_evicted_keys = 0
        evicted_keys_repeat_count = 0
        while True:
            # Wait for some time
            await asyncio.sleep(1)

            memory_info = await async_client.info("memory")
            logging.info(
                f'Current used memory: {memory_info["used_memory"]}, current used rss: {memory_info["used_memory_rss"]}, rss eviction threshold: {rss_eviction_threshold}.'
            )

            stats_info = await async_client.info("stats")
            logging.info(f'Current evicted: {stats_info["evicted_keys"]}. Total keys: {num_keys}.')

            # Check if evicted keys are not increasing
            if prev_evicted_keys == stats_info["evicted_keys"]:
                evicted_keys_repeat_count += 1
            else:
                prev_evicted_keys = stats_info["evicted_keys"]
                evicted_keys_repeat_count = 1

            if evicted_keys_repeat_count > 2:
                break

        # Wait for some time
        await asyncio.sleep(3)

        memory_arena = await async_client.execute_command("MEMORY", "ARENA")
        fragmentation_waste = extract_fragmentation_waste(memory_arena)
        logging.info(f"Memory fragmentation waste: {fragmentation_waste}")
        assert fragmentation_waste < 12.0, "Memory fragmentation waste should be less than 12%."

        # Assert that no more keys are evicted
        memory_info = await async_client.info("memory")
        stats_info = await async_client.info("stats")

        assert memory_info["used_memory"] > max_memory * (
            rss_oom_deny_ratio - eviction_memory_budget_threshold - 0.05
        ), "We should not evict all items."
        assert memory_info["used_memory"] < max_memory * (
            rss_oom_deny_ratio - eviction_memory_budget_threshold + 0.08
        ), "Used memory should be smaller than threshold."
        assert memory_info["used_memory_rss"] > max_memory * (
            rss_oom_deny_ratio - eviction_memory_budget_threshold - 0.05
        ), "We should not evict all items."

        evicted_keys = stats_info["evicted_keys"]
        # We may evict slightly more than prev_evicted_keys due to gaps in RSS memory usage
        assert (
            evicted_keys > 0
            and evicted_keys >= prev_evicted_keys
            and evicted_keys <= prev_evicted_keys * 1.0015
        ), "We should not evict more items."


@pytest.mark.asyncio
async def test_throttle_on_commands_squashing_replies_bytes(df_factory: DflyInstanceFactory):
    df = df_factory.create(
        proactor_threads=2,
        squashed_reply_size_limit=100_000_000,
        vmodule="dragonfly_connection=5",
    )
    df.start()

    client = df.client()
    # 100mb
    await client.execute_command("debug populate 64 test 3125 rand type hash elements 500")

    async def poll():
        # At any point we should not cross this limit
        # 2x the reply_size_limit, 200mb
        assert df.rss < 200_000_000
        cl = df.client()
        pipe = cl.pipeline(transaction=False)
        for i in range(64):
            pipe.execute_command(f"hgetall test:{i}")

        await pipe.execute()

    tasks = []
    for i in range(20):
        tasks.append(asyncio.create_task(poll()))

    for task in tasks:
        await task

    df.stop()
    found = df.find_in_logs("Commands squashing current reply size is overlimit")
    assert len(found) > 0
