import pytest
import asyncio
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
async def test_rss_used_mem_gap(df_factory: DflyInstanceFactory, type, keys, val_size, elements):
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

    await check_memory()

    # FLUSHALL sync waits for flush to finish and decommit memory, so send INFO immediately after
    p = client.pipeline(transaction=False)
    p.execute_command("FLUSHALL", "SYNC")  # flushall(asynchronous=False) will just issue FLUSHALL$
    p.info("memory")

    info = (await p.execute())[-1]
    assert info["used_memory"] < 2 * 1_000_000  # Table memory
    assert info["used_memory_rss"] < min_rss / 10  # RSS must have been freed


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


@pytest.mark.parametrize("heartbeat_rss_eviction", [True, False])
async def test_eviction_on_rss_treshold(df_factory: DflyInstanceFactory, heartbeat_rss_eviction):
    max_memory = 1024 * 1024**2  # 10242mb

    df_server = df_factory.create(
        proactor_threads=3,
        cache_mode="yes",
        maxmemory=max_memory,
        enable_heartbeat_eviction="false",
        enable_heartbeat_rss_eviction=heartbeat_rss_eviction,
    )
    df_server.start()
    client = df_server.client()

    data_fill_size = int(0.70 * max_memory)  # 70% of max_memory

    val_size = 1024 * 5  # 5 kb
    num_keys = data_fill_size // val_size

    await client.execute_command("DEBUG", "POPULATE", num_keys, "key", val_size)

    # Create huge list which can be used with LRANGE to increase RSS memory only
    for name in ["list_1", "list_2"]:
        for i in range(1, 1000):
            rand_str = "".join(random.choices(string.ascii_letters, k=val_size))
            await client.execute_command(f"LPUSH {name} {rand_str}")

    # Make them STICK so we don't evict them
    await client.execute_command(f"STICK list_1")
    await client.execute_command(f"STICK list_2")

    await client.execute_command("CONFIG SET enable_heartbeat_eviction true")

    memory_info_before = await client.info("memory")

    # This will increase only RSS memory above treshold
    p = client.pipeline()
    for _ in range(50):
        p.execute_command("LRANGE list_1 0 -1")
        p.execute_command("LRANGE list_2 0 -1")
    await p.execute()

    # Wait for some time
    await asyncio.sleep(3)
    memory_info_after = await client.info("memory")
    stats_info_after = await client.info("stats")

    if heartbeat_rss_eviction:
        # We should see used memory deacrease and number of some number of evicted keys
        assert memory_info_after["used_memory"] < memory_info_before["used_memory"]
        assert stats_info_after["evicted_keys"]
    else:
        # If heartbeat rss eviction is disabled there should be no chage
        assert memory_info_after["used_memory"] == memory_info_before["used_memory"]
        assert stats_info_after["evicted_keys"] == 0


# Github issue #5891
async def test_no_rss_eviction_overflow_on_expired_keys(df_factory: DflyInstanceFactory):
    max_memory = 256 * 1024**2  # 256MB
    df_server = df_factory.create(
        proactor_threads=1, cache_mode="yes", maxmemory=max_memory, vmodule="engine_shard=2"
    )
    df_server.start()
    client = df_server.client()

    data_fill_size = int(0.20 * max_memory)  # 20% of max_memory

    val_size = 1024 * 50  # 50 kb for key
    num_keys = data_fill_size // val_size

    for i in range(0, 5):
        pipe = client.pipeline(transaction=False)
        step_keys = num_keys + i * 10
        await pipe.execute_command("DEBUG", "POPULATE", step_keys, "key_1", val_size)
        await pipe.execute_command("DEBUG", "POPULATE", step_keys + i * 10, "key_2", val_size)
        for i in range(step_keys):
            if i % 2 == 0:
                await pipe.execute_command(f"EXPIRE key_1:{i} 1")
            else:
                await pipe.execute_command(f"EXPIRE key_2:{i} 1")
        await pipe.execute()
        await asyncio.sleep(2)

    await client.execute_command("FLUSHALL")

    # New keys should be added
    await client.execute_command("DEBUG", "POPULATE", num_keys, "key", val_size)
    # Wait so heartbeat eviction
    await asyncio.sleep(5)

    keyspace_info = await client.info("keyspace")
    assert keyspace_info["db0"]["keys"] == num_keys


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


@pytest.mark.asyncio
async def test_delete_doc_on_eviction(df_factory):
    max_memory = 256 * 1024**2  # 256MB
    df_server = df_factory.create(
        proactor_threads=1,
        cache_mode="yes",
        maxmemory=max_memory,
        vmodule="engine_shard=2",
        eviction_memory_budget_threshold=0.99,
        enable_heartbeat_rss_eviction="no",
    )
    df_server.start()
    client = df_server.client()

    await client.execute_command(
        "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "v", "TEXT"
    )

    i = 0
    while True:
        random_string = "".join(random.choices(string.ascii_letters + string.digits, k=1_000))
        await client.execute_command("HSET", f"doc:{i}", "v", random_string)
        stats_info = await client.info("stats")
        # Done when see at least 50 evictions
        if stats_info["evicted_keys"] > 50:
            break
        i = i + 1

    # Give some time to eviction stabilize
    await asyncio.sleep(1)

    # Get number of docs in index
    index_info = await client.execute_command(f"FT.INFO idx")
    index_info_num_docs = index_info[9]

    # Get number of keys in datbase
    keyspace_info = await client.info("keyspace")
    keyspace_keys = keyspace_info["db0"]["keys"]

    assert index_info_num_docs == keyspace_keys
