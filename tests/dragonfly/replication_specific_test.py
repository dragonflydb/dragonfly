import asyncio
import logging
import random
import time

import async_timeout
import pymemcache
import pytest
from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstanceFactory
from .seeder import DebugPopulateSeeder, HnswSearchSeeder
from .seeder import Seeder as SeederV2
from .replication_utils import (
    setup_replication,
    start_replication,
)
from .utility import (
    assert_eventually,
    check_all_replicas_finished,
    extract_int_after_prefix,
    is_saving,
    tmp_file_name,
    wait_available_async,
    wait_for_replicas_state,
)

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

M_OPT = [pytest.mark.opt_only]
M_SLOW = [pytest.mark.large]


async def test_search(df_factory):
    master, [replica], c_master, [c_replica] = await setup_replication(
        df_factory,
        master_args={"proactor_threads": 4},
        replica_args={"proactor_threads": 4},
        connect=False,
    )

    # First, create an index on replica
    await c_replica.execute_command("FT.CREATE", "idx-r", "SCHEMA", "f1", "numeric")
    for i in range(0, 10):
        await c_replica.hset(f"k{i}", mapping={"f1": i})
    assert (await c_replica.ft("idx-r").search("@f1:[5 9]")).total == 5

    # Second, create an index on master
    await c_master.execute_command("FT.CREATE", "idx-m", "SCHEMA", "f2", "numeric")
    for i in range(0, 10):
        await c_master.hset(f"k{i}", mapping={"f2": i * 2})
    assert (await c_master.ft("idx-m").search("@f2:[6 10]")).total == 3

    # Replicate
    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await wait_available_async(c_replica)

    # Check master index was picked up and original index was deleted
    assert (await c_replica.execute_command("FT._LIST")) == ["idx-m"]

    # Check query from master runs on replica
    assert (await c_replica.ft("idx-m").search("@f2:[6 10]")).total == 3

    # Set a new key
    await c_master.hset("kNEW", mapping={"f2": 100})
    await asyncio.sleep(0.1)

    assert (await c_replica.ft("idx-m").search("@f2:[100 100]")).docs[0].id == "kNEW"

    # Create a new aux index on master
    await c_master.execute_command("FT.CREATE", "idx-m2", "SCHEMA", "f2", "numeric", "sortable")
    await asyncio.sleep(0.1)

    from redis.commands.search.query import Query

    assert (await c_replica.ft("idx-m2").search(Query("*").sort_by("f2").paging(0, 1))).docs[
        0
    ].id == "k0"


@dfly_args({"proactor_threads": 4})
async def test_search_with_stream(df_factory: DflyInstanceFactory):
    master, [replica], c_master, [c_replica] = await setup_replication(df_factory, connect=False)

    # fill master with hsets and create index
    p = c_master.pipeline(transaction=False)
    for i in range(10_000):
        p.hset(f"k{i}", mapping={"name": f"name of {i}"})
    await p.execute()

    await c_master.execute_command("FT.CREATE i1 SCHEMA name text")

    # start replication and issue one add command and delete commands on master in parallel
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await c_master.hset("secret-key", mapping={"name": "new-secret"})
    for i in range(1_000):
        await c_master.delete(f"k{i}")

    # expect replica to see only 10k - 1k + 1 = 9001 keys in it's index
    await wait_available_async(c_replica)
    await check_all_replicas_finished([c_replica], c_master)
    assert await c_replica.execute_command("FT.SEARCH i1 * LIMIT 0 0") == [9_001]
    assert await c_replica.execute_command('FT.SEARCH i1 "secret"') == [
        1,
        "secret-key",
        ["name", "new-secret"],
    ]


async def test_heartbeat_eviction_propagation(df_factory):
    master, [replica], c_master, [c_replica] = await setup_replication(
        df_factory,
        master_args={
            "proactor_threads": 1,
            "cache_mode": "true",
            "maxmemory": "256mb",
            "enable_heartbeat_eviction": "false",
        },
        replica_args={"proactor_threads": 1},
        connect=False,
    )

    # fill the master to use about 233mb > 256mb * 0.9, which will trigger heartbeat eviction.
    await c_master.execute_command("DEBUG POPULATE 233 size 1048576")
    await start_replication(c_replica, master.port)

    # now enable heart beat eviction
    await c_master.execute_command("CONFIG SET enable_heartbeat_eviction true")

    while True:
        info = await c_master.info("stats")
        evicted_1 = info["evicted_keys"]
        time.sleep(2)
        info = await c_master.info("stats")
        evicted_2 = info["evicted_keys"]
        if evicted_2 == evicted_1:
            break
        else:
            print("waiting for eviction to finish...", end="\r", flush=True)

    await check_all_replicas_finished([c_replica], c_master)
    keys_master = await c_master.execute_command("keys *")
    keys_replica = await c_replica.execute_command("keys *")
    assert set(keys_master) == set(keys_replica)


async def test_policy_based_eviction_propagation(df_factory, df_seeder_factory):
    master, [replica], c_master, [c_replica] = await setup_replication(
        df_factory,
        master_args={
            "proactor_threads": 2,
            "cache_mode": "true",
            "maxmemory": "512mb",
            "enable_heartbeat_eviction": "false",
            "rss_oom_deny_ratio": 1.3,
        },
        replica_args={"proactor_threads": 2},
        connect=False,
    )

    await c_master.execute_command("DEBUG POPULATE 6000 size 88000")

    await start_replication(c_replica, master.port)

    seeder = df_seeder_factory.create(
        port=master.port, keys=600, val_size=1000, stop_on_failure=False
    )
    await seeder.run(target_deviation=0.1)

    info = await c_master.info("stats")
    assert (
        info["evicted_keys"] > 0
    ), f"Weak testcase: policy based eviction was not triggered. {await c_master.info()}"

    await check_all_replicas_finished([c_replica], c_master)

    # KEYS may trigger lazy expiry on master, generating DELs not yet received by replica.
    # Fetch master keys first, then re-sync to ensure replica applies any resulting DELs.
    keys_master = await c_master.execute_command("keys k*")
    await check_all_replicas_finished([c_replica], c_master)
    keys_replica = await c_replica.execute_command("keys k*")

    assert set(keys_replica).difference(keys_master) == set()
    assert set(keys_master).difference(keys_replica) == set()


@pytest.mark.parametrize(
    "action_during_save",
    [
        pytest.param("disconnect", marks=pytest.mark.large),
        "start_replicating",
    ],
)
async def test_save_with_replication(df_factory, action_during_save):
    if action_during_save == "disconnect":
        master = df_factory.create(proactor_threads=1)
        replica = df_factory.create(proactor_threads=1, dbfilename=f"dump_{tmp_file_name()}")
    else:
        master = df_factory.create(proactor_threads=4)
        replica = df_factory.create(proactor_threads=4, dbfilename=f"dump_{tmp_file_name()}")

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    if action_during_save == "disconnect":
        await c_master.execute_command("DEBUG POPULATE 100000 key 4048 RAND")
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        await wait_available_async(c_replica)
    else:
        await c_replica.execute_command("DEBUG POPULATE 100000 key 4096 RAND")

    async def save_replica():
        await c_replica.execute_command("save")

    save_task = asyncio.create_task(save_replica())
    while not await is_saving(c_replica):  # wait for replica start saving
        assert "rdb_changes_since_last_success_save:0" not in await c_replica.execute_command(
            "info persistence"
        ), "Weak test case, finished saving too quickly"
        await asyncio.sleep(0.1)

    if action_during_save == "disconnect":
        await c_replica.execute_command("replicaof no one")
    else:
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    assert await is_saving(c_replica)
    await save_task
    assert not await is_saving(c_replica)


async def test_replicating_mc_flags(df_factory):
    master = df_factory.create(memcached_port=11211, proactor_threads=1)
    replica = df_factory.create(
        memcached_port=11212, proactor_threads=1, dbfilename=f"dump_{tmp_file_name()}"
    )
    df_factory.start_all([master, replica])

    c_mc_master = pymemcache.Client(f"127.0.0.1:{master.mc_port}", default_noreply=False)

    c_replica = replica.client()

    assert c_mc_master.set("key1", "value0", noreply=True)
    assert c_mc_master.set("key2", "value2", noreply=True, expire=3600, flags=123456)
    assert c_mc_master.replace("key1", "value1", expire=4000, flags=2, noreply=True)

    c_master = master.client()
    for i in range(3, 100):
        await c_master.set(f"key{i}", f"value{i}")

    await start_replication(c_replica, master.port)

    c_mc_replica = pymemcache.Client(f"127.0.0.1:{replica.mc_port}", default_noreply=False)

    async def check_flag(key, flag):
        res = c_mc_replica.raw_command("get " + key, "END\r\n").split()
        # workaround sometimes memcached_client.raw_command returns empty str
        if len(res) > 2:
            assert res[2].decode() == str(flag)

    await check_flag("key1", 2)
    await check_flag("key2", 123456)

    for i in range(1, 100):
        assert c_mc_replica.get(f"key{i}") == str.encode(f"value{i}")


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 1})
async def test_memory_on_big_string_loading(df_factory):
    """
    In this test we want to make sure there is no spike in rss while loading big string value
    1. insert 1 big value to master
    2. replicate master
    3. check rss peak memory on replica node
    """
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    logging.debug("Populate with one big string")
    await c_master.execute_command("DEBUG POPULATE 1 key 200000000 RAND")

    async def get_memory(client, field):
        info = await client.info("memory")
        return info[field]

    logging.debug("Start replication and wait for full sync")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    await c_replica.execute_command("memory decommit")
    await asyncio.sleep(1)
    replica_peak_memory = await get_memory(c_replica, "used_memory_peak_rss")
    replica_used_memory = await get_memory(c_replica, "used_memory_rss")

    logging.info(f"Replica Used memory {replica_used_memory}, peak memory {replica_peak_memory}")
    assert replica_peak_memory < 1.1 * replica_used_memory

    # Check replica data consistent
    replica_data = await DebugPopulateSeeder.capture(c_replica)
    master_data = await DebugPopulateSeeder.capture(c_master)
    assert master_data == replica_data


@pytest.mark.exclude_epoll
@pytest.mark.parametrize(
    "element_size, elements_number",
    [(16, 30000), (30000, 16)],
)
@dfly_args({"proactor_threads": 1})
async def test_big_containers(df_factory, element_size, elements_number):
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    logging.debug("Fill master with test data")
    seeder = DebugPopulateSeeder(
        key_target=50,
        data_size=element_size * elements_number,
        collection_size=elements_number,
        variance=1,
        samples=1,
        types=["LIST", "SET", "ZSET", "HASH", "STREAM"],
    )
    await seeder.run(c_master)

    async def get_memory(client, field):
        info = await client.info("memory")
        return info[field]

    await asyncio.sleep(1)  # wait for heartbeat to update rss memory
    used_memory = await get_memory(c_master, "used_memory_rss")

    logging.debug("Start replication and wait for full sync")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    peak_memory = await get_memory(c_master, "used_memory_peak_rss")

    logging.info(f"Used memory {used_memory}, peak memory {peak_memory}")
    assert peak_memory < 1.1 * used_memory

    await c_replica.execute_command("memory decommit")
    await asyncio.sleep(1)
    replica_peak_memory = await get_memory(c_replica, "used_memory_peak_rss")
    replica_used_memory = await get_memory(c_replica, "used_memory_rss")

    logging.info(f"Replica Used memory {replica_used_memory}, peak memory {replica_peak_memory}")
    assert replica_peak_memory < 1.1 * replica_used_memory

    # Check replica data consistent
    replica_data = await DebugPopulateSeeder.capture(c_replica)
    master_data = await DebugPopulateSeeder.capture(c_master)
    assert master_data == replica_data


async def test_master_too_big(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=2, maxmemory="600mb")

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()
    await c_master.execute_command("DEBUG POPULATE 1000000 key 1000 RAND")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    # We should never sync due to used memory too high during full sync
    with pytest.raises(TimeoutError):
        await wait_available_async(c_replica, timeout=10)


@dfly_args({"proactor_threads": 4})
async def test_stream_approximate_trimming(df_factory):
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    # Step 1: Populate master with 100 streams, each containing 200 entries
    num_streams = 100
    entries_per_stream = 200

    for i in range(num_streams):
        stream_name = f"stream{i}"
        for j in range(entries_per_stream):
            await c_master.execute_command("XADD", stream_name, "*", f"field{j}", f"value{j}")

    # Step 2: Trim each stream to a random size between 70 and 200
    for i in range(num_streams):
        stream_name = f"stream{i}"
        trim_size = random.randint(70, entries_per_stream)
        await c_master.execute_command("XTRIM", stream_name, "MAXLEN", "~", trim_size)

    # Wait for replica sync
    await check_all_replicas_finished([c_replica], c_master)

    # Check replica data consistent
    master_data = await DebugPopulateSeeder.capture(c_master)
    replica_data = await DebugPopulateSeeder.capture(c_replica)
    assert master_data == replica_data

    # Step 3: Trim all streams to 0
    for i in range(num_streams):
        stream_name = f"stream{i}"
        await c_master.execute_command("XTRIM", stream_name, "MAXLEN", "0")

    # Wait for replica sync
    await check_all_replicas_finished([c_replica], c_master)

    # Check replica data consistent
    master_data = await DebugPopulateSeeder.capture(c_master)
    replica_data = await DebugPopulateSeeder.capture(c_replica)
    assert master_data == replica_data


@pytest.mark.large
async def test_preempt_in_atomic_section_of_heartbeat(df_factory: DflyInstanceFactory):
    master = df_factory.create(proactor_threads=1, serialization_max_chunk_size=100000000000)
    replicas = [df_factory.create(proactor_threads=1) for i in range(2)]

    # Start instances and connect clients
    df_factory.start_all([master] + replicas)
    c_master = master.client()
    c_replicas = [replica.client() for replica in replicas]

    total = 100000
    await c_master.execute_command(f"DEBUG POPULATE {total} tmp 100 TYPE SET ELEMENTS 100")

    thresehold = 50000
    for i in range(thresehold):
        rand = random.randint(1, 10)
        await c_master.execute_command(f"EXPIRE tmp:{i} {rand} NX")

    seeder = SeederV2(key_target=10_000)
    fill_task = asyncio.create_task(seeder.run(master.client()))

    for replica in c_replicas:
        await replica.execute_command(f"REPLICAOF LOCALHOST {master.port}")

    async with async_timeout.timeout(240):
        await wait_for_replicas_state(*c_replicas)

    await fill_task


@pytest.mark.large
async def test_bug_in_json_memory_tracking(df_factory: DflyInstanceFactory):
    """
    This test reproduces a bug in the JSON memory tracking.
    """
    random.seed(42)

    master = df_factory.create(
        proactor_threads=2,
        serialization_max_chunk_size=1,
        vmodule="replica=2,dflycmd=2,snapshot=1,rdb_save=1,rdb_load=1,journal_slice=2",
    )
    replicas = [df_factory.create(proactor_threads=2) for i in range(2)]

    # Start instances and connect clients
    df_factory.start_all([master] + replicas)
    c_master = master.client()
    c_replicas = [replica.client() for replica in replicas]

    total = 100000
    await c_master.execute_command(f"DEBUG POPULATE {total} tmp 1000 TYPE SET ELEMENTS 100")

    threshold = 25000
    for i in range(threshold):
        rand = random.randint(1, 4)
        await c_master.execute_command(f"EXPIRE tmp:{i} {rand} NX")

    seeder = SeederV2(key_target=50_000)
    fill_task = asyncio.create_task(seeder.run(master.client()))
    await asyncio.sleep(0.2)

    for replica in c_replicas:
        await replica.execute_command(f"REPLICAOF LOCALHOST {master.port}")

    async with async_timeout.timeout(240):
        await wait_for_replicas_state(*c_replicas)

    await seeder.stop(c_master)
    await fill_task


@pytest.mark.skip("fails, investigating")
@pytest.mark.large
@pytest.mark.opt_only
@pytest.mark.parametrize("tagged_chunks", [True, False])
@dfly_args({"proactor_threads": 2, "serialization_max_chunk_size": 5000, "compression_mode": "0"})
async def test_big_huge_streaming_restart(df_factory: DflyInstanceFactory, tagged_chunks):
    """
    Restart replicating instance with huge values. Tests that interrupting the streaming process doesn't hinder retrying replication
    """

    master, replica = df_factory.create(
        serialization_tagged_chunks=tagged_chunks
    ), df_factory.create(proactor_threads=1)
    df_factory.start_all([master, replica])
    c_master, c_replica = master.client(), replica.client()

    # Create huge values
    await c_master.execute_command(
        "debug", "populate", "2", "test", "1000", "rand", "type", "zset", "elements", "1000000"
    )

    done = False

    async def ticker(c):
        while not done:
            await c_master.incr(f"key-{c}")

    tickers = [asyncio.create_task(ticker(c)) for c in range(3)]

    # Restart replication a few times
    for _ in range(3):
        print("Loop")
        assert await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        await asyncio.sleep(random.random() + 0.5)

    # Wait for it to finish finally
    async with async_timeout.timeout(75):
        await wait_for_replicas_state(c_replica)

    done = True
    for t in tickers:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    await check_all_replicas_finished([c_replica], c_master)

    # Check that everything is in sync
    hashes = await asyncio.gather(*(SeederV2.capture(c) for c in [c_master, c_replica]))
    assert len(set(hashes)) == 1

    # No in-between errors occured
    replica.stop()
    lines = replica.find_in_logs("Duplicate zset fields detected")
    assert len(lines) == 0


@pytest.mark.large
async def test_replica_snapshot_with_big_values_while_seeding(df_factory: DflyInstanceFactory):
    proactors = 4
    master = df_factory.create(proactor_threads=proactors, dbfilename="")
    replica = df_factory.create(proactor_threads=proactors, dbfilename="")
    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    # 50% big values
    seeder_config = dict(key_target=8_000, huge_value_target=4_000)
    # Fill instance with test data
    seeder = SeederV2(**seeder_config)
    await seeder.run(c_master, target_deviation=0.01)

    assert await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    async with async_timeout.timeout(60):
        await wait_for_replicas_state(c_replica)

    # Start data stream
    stream_task = asyncio.create_task(seeder.run(c_master))
    await asyncio.sleep(1)

    file_name = tmp_file_name()
    assert await c_replica.execute_command(f"SAVE DF {file_name}") == "OK"
    await seeder.stop(c_master)
    await stream_task

    await check_all_replicas_finished([c_replica], c_master)

    # Check that everything is in sync
    hashes = await asyncio.gather(*(SeederV2.capture(c) for c in [c_master, c_replica]))
    assert len(set(hashes)) == 1

    replica.stop()
    lines = replica.find_in_logs("Exit SnapshotSerializer")
    assert len(lines) == (proactors - 1)
    for line in lines:
        # We test the serializtion path of command execution
        side_saved = extract_int_after_prefix("side saved ", line)
        assert side_saved > 0

    # Check that the produced rdb is loaded correctly
    node = df_factory.create(dbfilename=file_name)
    node.start()
    c_node = node.client()
    await wait_available_async(c_node)
    assert await c_node.execute_command("dbsize") > 0
    await c_node.execute_command("FLUSHALL")


async def test_replicate_hset_with_expiry(df_factory: DflyInstanceFactory):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)

    master.start()
    replica.start()

    cm = master.client()
    await cm.execute_command("HSETEX key 86400 name 1234")

    cr = replica.client()
    await cr.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(cr)

    result = await cr.hgetall("key")

    assert "name" in result
    assert result["name"] == "1234"


async def test_bug_5221(df_factory):
    master = df_factory.create(
        proactor_threads=1,
        cache_mode="true",
        maxmemory="256mb",
        enable_heartbeat_eviction="true",
        eviction_memory_budget_threshold=0.9,
    )
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()
    await c_replica.execute_command(f"replicaof localhost {master.port}")

    # Fill master with test data
    seeder = SeederV2(key_target=22000, data_size=1000)
    await seeder.run(c_master, target_deviation=0.01)
    await asyncio.sleep(1)
    await seeder.run(c_master, target_deviation=0.01)
    res = await c_master.execute_command("dbsize")
    assert res > 0


async def test_mc_gat_replication(df_factory):
    master = df_factory.create(memcached_port=11211, proactor_threads=1)
    replica = df_factory.create(memcached_port=11212, proactor_threads=1)
    df_factory.start_all([master, replica])

    cm = pymemcache.Client(f"127.0.0.1:{master.mc_port}", default_noreply=False)

    key = "foo"
    value = b"bar"
    not_found = b"NOTFOUND"
    assert cm.set(key, value, noreply=True)

    async with replica.client() as cl:
        await cl.execute_command(f"REPLICAOF localhost {master.port}")
        await wait_available_async(cl)

    async def state_transitioned_stable(
        init: bytes,
        expected: bytes,
        duration_sec=5,
        sleep_sec=1,
    ):
        """
        Asserts that the state goes from initial to expected and then stays at expected, observing state for duration_sec
        """
        _start = time.time()
        transitioned = False
        state = None
        while time.time() - _start < duration_sec:
            state = cr.get(key, not_found)
            if not transitioned and state == expected:
                transitioned = True
            if transitioned:
                assert (
                    state == expected
                ), f"state moved back to initial after transition {state=} {init=} {expected=}"
            else:
                assert state == init, f"unexpected state: {state=} {init=}"
            await asyncio.sleep(sleep_sec)
        return state == expected

    cr = pymemcache.Client(f"127.0.0.1:{replica.mc_port}", default_noreply=False)

    assert await state_transitioned_stable(not_found, value)

    # Force the key to be removed by setting expiry in the past. Memcache treats expiry > 1 month as absolute from
    # epoch, so 1 month + 1 second expires the key
    month_plus_one = 60 * 60 * 24 * 30 + 1

    # GAT|GATS are not directly exposed in the python client API
    assert cm._fetch_cmd(b"gat", [str(month_plus_one), key], expect_cas=False) == {}

    # The replica should eventually sync the delete operation
    assert await state_transitioned_stable(value, not_found)

    assert cm.set(key, value, noreply=True)
    # expiry is set as now + 1000 seconds, which ensures the key will remain for the duration of the test
    assert cm._fetch_cmd(b"gat", [str(1000), key], expect_cas=False) == {key: value}

    # once the value is synced to the replica, assert that it remains stable and is not removed by setting expiry
    assert await state_transitioned_stable(not_found, value)

    result = cm._fetch_cmd(b"gats", [str(1000), key], expect_cas=True)
    assert len(result) == 1 and key in result, f"missing expected key: {result=}"
    expected_cas_ver = b"0"
    assert result[key] == (value, expected_cas_ver), f"unexpected result for key: {result=}"


@pytest.mark.skip("Fails constantly on CI")
@pytest.mark.large
@pytest.mark.parametrize("serialization_max_size", [1, 64000])
async def test_replication_onmove_flow(df_factory, serialization_max_size):
    master = df_factory.create(
        proactor_threads=2,
        cache_mode=True,
        point_in_time_snapshot=False,
        serialization_max_chunk_size=serialization_max_size,
    )
    replica = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    key_target = 100000
    # Fill master with test data
    await c_master.execute_command(f"DEBUG POPULATE {key_target} key 32 RAND TYPE hash ELEMENTS 10")
    logging.debug("finished populate")

    stop_event = asyncio.Event()

    async def get_keys():
        while not stop_event.is_set():
            pipe = c_master.pipeline(transaction=False)
            for _ in range(50):
                id = random.randint(0, key_target)
                pipe.hlen(f"key:{id}")
            await pipe.execute()

    get_task = asyncio.create_task(get_keys())
    await asyncio.sleep(0.1)

    # Start replication and wait for full sync
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    info = await c_master.info("stats")
    assert info["bump_ups"] >= 100

    await check_all_replicas_finished([c_replica], c_master)
    stop_event.set()
    await get_task

    # Check replica data consisten
    hash1, hash2 = await asyncio.gather(*(SeederV2.capture(c) for c in (c_master, c_replica)))
    assert hash1 == hash2

    master.stop()
    lines = master.find_in_logs("Exit SnapshotSerializer")
    assert len(lines) > 0
    for line in lines:
        # We test the full sync on moved path execution
        moved_saved = extract_int_after_prefix("moved_saved ", line)
        logging.debug(f"Moved saves {moved_saved}")
        assert moved_saved > 0


@pytest.mark.large
@dfly_args({"proactor_threads": 1})
async def test_big_strings(df_factory):
    master = df_factory.create(
        proactor_threads=1, serialization_max_chunk_size=1, vmodule="snapshot=1"
    )
    replica = df_factory.create(proactor_threads=1)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    # 200kb
    value_size = 200_000

    async def get_memory(client, field):
        info = await client.info("memory")
        return info[field]

    capacity = await get_memory(c_master, "prime_capacity")

    seeder = DebugPopulateSeeder(
        key_target=int(capacity * 0.7),
        data_size=value_size,
        collection_size=1,
        variance=1,
        samples=1,
        types=["STRING"],
    )
    await seeder.run(c_master)

    # sanity
    capacity = await get_memory(c_master, "prime_capacity")
    assert capacity < 8000

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    # Check if replica data is consistent
    replica_data = await DebugPopulateSeeder.capture(c_replica)
    master_data = await DebugPopulateSeeder.capture(c_master)
    assert master_data == replica_data

    replica.stop()
    master.stop()

    lines = master.find_in_logs("Serialization peak bytes: ")
    assert len(lines) == 1
    # We test the serializtion path of command execution
    line = lines[0]
    peak_bytes = extract_int_after_prefix("Serialization peak bytes: ", line)
    assert peak_bytes < value_size


@dfly_args({"proactor_threads": 2})
@pytest.mark.replication
async def test_xreadgroup_replication(replication):
    _, _, c_master, [c_replica] = replication

    async def compare_group_info(stream_key, expected_pending, expected_entries_read):
        master_info = await c_master.execute_command(f"XINFO GROUPS {stream_key}")
        replica_info = await c_replica.execute_command(f"XINFO GROUPS {stream_key}")

        # Parse group info (format: [name, consumers, pending, last-delivered-id, entries-read, lag])
        assert len(master_info) == len(replica_info)

        for m_group, r_group in zip(master_info, replica_info):
            m_dict = dict(zip(m_group[::2], m_group[1::2]))
            r_dict = dict(zip(r_group[::2], r_group[1::2]))

            assert m_dict["last-delivered-id"] == r_dict["last-delivered-id"]
            assert m_dict["entries-read"] == r_dict["entries-read"]
            assert m_dict["entries-read"] == expected_entries_read
            assert m_dict["pending"] == r_dict["pending"]
            assert m_dict["pending"] == expected_pending
            assert m_dict["consumers"] == r_dict["consumers"]

    # Case 1: Non-blocking path, NOACK
    await c_master.execute_command("XGROUP CREATE mystream mygroup $ MKSTREAM")
    await c_master.execute_command("XADD mystream * tmp tmp")
    await c_master.execute_command("XREADGROUP GROUP mygroup worker1 NOACK STREAMS mystream >")

    await check_all_replicas_finished([c_replica], c_master)
    await compare_group_info("mystream", 0, 1)

    # Case 2: Non-blocking path, with PEL
    await c_master.execute_command("XADD mystream * tmp tmp")
    await c_master.execute_command("XADD mystream * tmp tmp")
    await c_master.execute_command("XREADGROUP GROUP mygroup worker1 STREAMS mystream >")

    await check_all_replicas_finished([c_replica], c_master)
    await compare_group_info("mystream", 2, 3)

    # Case 3: Blocking path, NOACK

    # Start blocking XREADGROUP in background
    read_task = asyncio.create_task(
        c_master.execute_command(
            "XREADGROUP GROUP mygroup worker1 NOACK BLOCK 0 STREAMS mystream >"
        )
    )
    # Let the blocking command start
    await asyncio.sleep(0.1)
    await c_master.execute_command("XADD mystream * tmp tmp")

    await read_task

    await check_all_replicas_finished([c_replica], c_master)
    await compare_group_info("mystream", 2, 4)

    # Case 4: Blocking path, with PEL

    # Start blocking XREADGROUP in background
    read_task = asyncio.create_task(
        c_master.execute_command("XREADGROUP GROUP mygroup worker1 BLOCK 0 STREAMS mystream >")
    )

    await asyncio.sleep(0.1)
    await c_master.execute_command("XADD mystream * tmp tmp")
    await read_task

    await check_all_replicas_finished([c_replica], c_master)
    await compare_group_info("mystream", 3, 5)

    await c_master.execute_command("flushall")
    # Create consumer
    await c_master.execute_command("XGROUP CREATE mystream mygroup $ MKSTREAM")
    await c_master.execute_command("XADD mystream 2000-0 tmp tmp")
    # Add to PEL but don't ack
    await c_master.execute_command("XREADGROUP GROUP mygroup worker1 STREAMS mystream >")
    await c_master.execute_command("XREADGROUP GROUP mygroup worker2 STREAMS mystream 2000-0")

    await check_all_replicas_finished([c_replica], c_master)
    await compare_group_info("mystream", 1, 1)


"""
Regression test for SIGSEGV when XREAD BLOCK unblocks with replication active.
JournalXReadGroupIfNeeded accessed sitem.group->entries_read without checking that
group is non-null (group is always null for XREAD, only set for XREADGROUP).
"""


@dfly_args({"proactor_threads": 2})
async def test_xread_block_replication_crash_6975(df_factory):
    master = df_factory.create()
    replica = df_factory.create()

    master.start()
    replica.start()

    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    await c_master.execute_command("XADD mystream * tmp tmp")

    # Start blocking XREAD - this will crash the server on unblock with replication active
    # because JournalXReadGroupIfNeeded dereferences a null group pointer
    read_task = asyncio.create_task(c_master.execute_command("XREAD BLOCK 0 STREAMS mystream $"))
    await asyncio.sleep(0.1)
    assert not read_task.done(), "XREAD BLOCK should still be blocking"

    await c_master.execute_command("XADD mystream * field value")
    result = await read_task

    assert result is not None
    await check_all_replicas_finished([c_replica], c_master)


# BF.RESERVE with error_rate=0.00001 and capacity=1e9 creates a single bloom filter
# of exactly 2^32 bytes (4 GiB). The chunked RDB loader used `unsigned` for the total
# filter size, which silently overflowed to 0 and broke the RDB stream.
# Verify that chunked replication of a large bloom filter works and the replicated filter
# contains the expected items.
@pytest.mark.large
async def test_sbf_chunked_replication(df_factory: DflyInstanceFactory):
    master = df_factory.create(
        proactor_threads=1,
        maxmemory="6G",
    )
    replica = df_factory.create(
        proactor_threads=1,
        maxmemory="6G",
    )

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("BF.RESERVE", "bf", "0.00001", "1000000000")
    await c_master.execute_command("BF.ADD", "bf", "hello")

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    async with async_timeout.timeout(240):
        await wait_for_replicas_state(c_replica)

    await check_all_replicas_finished([c_replica], c_master)
    assert await c_replica.execute_command("BF.EXISTS", "bf", "hello") == 1


# Verify chunked replication of a moderately-sized bloom filter (~1 GB) doesn't exceed
# the max chunk size and correctly replicates all added items.
@pytest.mark.large
async def test_sbf_chunked_replication_chunk_size(df_factory: DflyInstanceFactory):
    master = df_factory.create(
        proactor_threads=1,
        maxmemory="4G",
    )
    replica = df_factory.create(
        proactor_threads=1,
        maxmemory="4G",
    )

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("BF.RESERVE", "bf", "0.011", "400000000")

    bf_size = await c_master.execute_command("MEMORY USAGE", "bf")
    assert bf_size > 2**30, f"Bloom filter should be >1GB, got {bf_size}"

    # Fix seed to have reproducible test results and log the seed for debugging
    random_seed = random.getrandbits(64)
    logging.info(f"Using random seed {random_seed} for test reproducibility")
    test_rng = random.Random(random_seed)

    num_items = 20000
    random_items = [f"item:{i}" for i in test_rng.sample(range(1_000_000), num_items)]
    await c_master.execute_command("BF.MADD", "bf", *random_items)

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    async with async_timeout.timeout(240):
        await wait_for_replicas_state(c_replica)

    await check_all_replicas_finished([c_replica], c_master)

    # Verify all added items exist on the replica
    replica_results = await c_replica.execute_command("BF.MEXISTS", "bf", *random_items)
    assert all(
        replica_results
    ), f"{replica_results.count(0)} of {len(random_items)} items missing on replica"

    # Verify some non-added item doesn't exist on the replica
    assert await c_replica.execute_command("BF.EXISTS", "bf", "not-added") == 0

    master.stop()
    replica.stop()

    # We have set MAX_SBF_CHUNK_SIZE to 1 << 26. Double peak bytes size to be safe because of
    # potential overhead that could be added.
    MAX_SBF_CHUNK_SIZE = 2**27

    lines = master.find_in_logs("Serialization peak bytes: ")
    assert len(lines) == 1
    line = lines[0]
    peak_bytes = extract_int_after_prefix("Serialization peak bytes: ", line)
    assert peak_bytes < MAX_SBF_CHUNK_SIZE


# Per-sync timeouts share the test-level budget so individual waits don't
# trip earlier than pytest.mark.timeout on slower CI runners.
HNSW_LARGE_TEST_TIMEOUT_SEC = 900
HNSW_LARGE_MARKS = [pytest.mark.large, pytest.mark.timeout(HNSW_LARGE_TEST_TIMEOUT_SEC)]


def _hnsw_sync_timeout(num_docs: int) -> int:
    return HNSW_LARGE_TEST_TIMEOUT_SEC if num_docs >= 100_000 else 120


@pytest.mark.parametrize(
    "master_threads, replica_threads, num_dims, num_docs",
    [
        pytest.param(3, 4, 4, 500, id="3t-4t-copied"),
        pytest.param(4, 4, 4, 500, id="4t-4t-copied"),
        pytest.param(4, 3, 4, 500, id="4t-3t-copied"),
        pytest.param(4, 4, 256, 100, id="4t-4t-external"),
        pytest.param(4, 4, 8, 100_000, id="4t-4t-100k", marks=HNSW_LARGE_MARKS),
    ],
)
async def test_hnsw_search_replication_with_network_disruptions(
    df_factory: DflyInstanceFactory,
    master_threads: int,
    replica_threads: int,
    num_dims: int,
    num_docs: int,
    proxy_factory,
):
    """
    Test HNSW search index replication under continuous traffic and a network disruption.

    Creates a master with an HNSW vector index, starts concurrent write traffic and
    search queries, replicates through a proxy, and drops the connection at a random
    moment within the first 10 seconds (may hit full sync or stable sync).

    When num_dims=256, vector data is 1024 bytes (>= default max_listpack_map_bytes),
    forcing StringMap encoding and external HNSW vector pointers (copy_vector=false).
    """
    master = df_factory.create(proactor_threads=master_threads)
    replica = df_factory.create(proactor_threads=replica_threads)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    seeder = HnswSearchSeeder(num_initial_docs=num_docs, num_dims=num_dims)
    await seeder.create_index(c_master)
    await seeder.seed_initial_docs(c_master)

    proxy = await proxy_factory(master.port)
    traffic_task = asyncio.create_task(seeder.run_traffic(c_master))
    search_task = asyncio.create_task(seeder.run_search_queries(c_master))
    await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

    sync_timeout = _hnsw_sync_timeout(num_docs)

    # Wait for initial sync before running search queries on replica.
    # During HNSW graph restoration with external vectors, nodes have
    # uninitialized data pointers — search queries could dereference them.
    await wait_available_async(c_replica, timeout=sync_timeout)
    replica_search_task = asyncio.create_task(seeder.run_search_queries(c_replica))

    try:
        await asyncio.sleep(random.uniform(0, 10))
        proxy.drop_connection()

        # Give time to detect dropped connection and reconnect
        await asyncio.sleep(1.0)

        await wait_available_async(c_replica, timeout=sync_timeout)
        seeder.stop()
        await traffic_task
        await search_task
        await replica_search_task

        # Log replica FT.INFO for debugging if assertion fails later
        info = await c_replica.execute_command("FT.INFO", seeder.index_name)
        logging.info(f"Replica FT.INFO: {info}")

        await check_all_replicas_finished([c_replica], c_master, timeout=sync_timeout)
        await seeder.verify(c_master, c_replica)

    finally:
        seeder.stop()
        traffic_task.cancel()
        search_task.cancel()
        replica_search_task.cancel()


@pytest.mark.parametrize(
    "document_type, num_docs",
    [
        pytest.param("HASH", 300, id="HASH"),
        pytest.param("JSON", 300, id="JSON"),
        pytest.param("HASH", 100_000, id="HASH-100k", marks=HNSW_LARGE_MARKS),
    ],
)
async def test_hnsw_failover_chain(
    df_factory: DflyInstanceFactory, document_type: str, num_docs: int
):
    """
    Primary → replica1 → REPLTAKEOVER → attach replica2 to promoted node.
    The promoted node must still serve KNN, and a freshly attached replica
    must rebuild the HNSW index from the promoted node's data.
    """
    master = df_factory.create(proactor_threads=2)
    replica1 = df_factory.create(proactor_threads=2)
    replica2 = df_factory.create(proactor_threads=2)
    df_factory.start_all([master, replica1, replica2])

    c_master = master.client()
    c1 = replica1.client()
    c2 = replica2.client()

    sync_timeout = _hnsw_sync_timeout(num_docs)
    seeder = HnswSearchSeeder(num_initial_docs=num_docs, num_dims=8, document_type=document_type)
    await seeder.create_index(c_master)
    await seeder.seed_initial_docs(c_master)

    await c1.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c1, timeout=sync_timeout)
    await check_all_replicas_finished([c1], c_master, timeout=sync_timeout)
    await seeder.verify(c_master, c1)

    # Promote replica1. The master exits after REPLTAKEOVER completes.
    await c1.execute_command("REPLTAKEOVER 5")
    assert (await c1.execute_command("role"))[0] == "master"

    # Attach replica2 to the promoted node and verify it rebuilds HNSW.
    await c2.execute_command(f"REPLICAOF localhost {replica1.port}")
    await wait_available_async(c2, timeout=sync_timeout)
    await check_all_replicas_finished([c2], c1, timeout=sync_timeout)
    await seeder.verify(c1, c2)


@pytest.mark.parametrize("document_type", ["HASH", "JSON"])
@pytest.mark.parametrize("data_type", ["INT8", "FLOAT16"])
async def test_hnsw_search_replication_dtypes(
    df_factory: DflyInstanceFactory, document_type: str, data_type: str
):
    """A non-float32 HNSW index replicates: the replica rebuilds it at the field's native
    width and serves KNN identical to the master."""
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    seeder = HnswSearchSeeder(
        num_initial_docs=200, num_dims=8, document_type=document_type, data_type=data_type
    )
    await seeder.create_index(c_master)
    await seeder.seed_initial_docs(c_master)

    await start_replication(c_replica, master.port)
    await check_all_replicas_finished([c_replica], c_master)
    await seeder.verify(c_master, c_replica)


async def test_rm_replication(df_factory: DflyInstanceFactory):
    """Test that RM command propagates deletions to replica and is rejected on replica."""
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)

    master.start()
    replica.start()

    c_master = master.client()
    c_replica = replica.client()

    # Populate master with keys before replication starts
    for i in range(20):
        await c_master.set(f"key:{i}", f"val{i}")
    for i in range(5):
        await c_master.set(f"other:{i}", f"val{i}")

    # Set up replication
    await start_replication(c_replica, master.port)

    # Verify replica has all keys
    assert await c_replica.dbsize() == 25
    logging.info("Replica has all keys")

    # Run RM on master with a MATCH filter to delete only "key:*" keys
    cursor = 0
    while True:
        result = await c_master.execute_command("RM", cursor, "MATCH", "key:*")
        cursor = int(result[0])
        if cursor == 0:
            break

    # Master should have only "other:*" keys left
    assert await c_master.dbsize() == 5

    # Wait for replication to propagate
    await check_all_replicas_finished([c_replica], c_master)

    # Replica should reflect deletions
    assert await c_replica.dbsize() == 5
    for i in range(5):
        assert await c_replica.exists(f"other:{i}") == 1
    for i in range(20):
        assert await c_replica.exists(f"key:{i}") == 0

    # RM must be rejected on replica (it's a write command)
    with pytest.raises((aioredis.ResponseError, aioredis.ReadOnlyError)):
        await c_replica.execute_command("RM", 0)


@pytest.mark.parametrize(
    "trigger_cmd, members, extra_setup",
    [
        # Originally from test_set_member_expiry_replication
        (["SMEMBERS", "myset"], ["a", "b", "c"], []),
        (["SUNION", "myset", "other"], ["a", "b", "c"], []),
        (["SDIFF", "myset", "other"], ["a", "b", "c"], []),
        (["SMISMEMBER", "myset", "a", "b", "c"], ["a", "b", "c"], []),
        (["SSCAN", "myset", "0"], ["a", "b", "c"], []),
        # Originally from test_set_member_expiry_replication_with_setup
        (["SISMEMBER", "myset", "a"], ["a"], []),
        (["SMOVE", "myset", "dst", "a"], ["a"], [["SADD", "dst", "x"]]),
        (["SINTER", "myset", "other"], ["a", "b", "c"], [["SADD", "other", "a", "b", "c"]]),
        (["FIELDEXPIRE", "myset", "100", "a"], ["a"], []),
        (["FIELDTTL", "myset", "a"], ["a"], []),
    ],
    ids=[
        "smembers",
        "sunion",
        "sdiff",
        "smismember",
        "sscan",
        "sismember",
        "smove",
        "sinter",
        "fieldexpire",
        "fieldttl",
    ],
)
async def test_set_member_expiry_replication(
    df_factory: DflyInstanceFactory, trigger_cmd, members, extra_setup
):
    """
    Verify that lazy set-member expiry on master is replicated to the replica.

    When all members of a StringSet expire via lazy expiry (triggered by a read),
    DeleteSetIfEmpty removes the key on master but does not journal the deletion.
    This causes the replica to retain a stale key that no longer exists on master.

    SISMEMBER/SMOVE are point lookups that only expire entries in the accessed bucket,
    so a single member is used to guarantee the set becomes empty.
    SINTER needs a second set to exist for the intersection code path to iterate.
    """
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Set up replication before writing data to avoid race with TTL
    await start_replication(c_replica, master.port)

    await c_master.execute_command("SADDEX", "myset", "1", *members)
    for cmd in extra_setup:
        await c_master.execute_command(*cmd)

    num_members = len(members)
    assert await c_master.scard("myset") == num_members

    await check_all_replicas_finished([c_replica], c_master)

    assert await c_replica.scard("myset") == num_members

    # Wait for members to expire
    await asyncio.sleep(1)

    # Trigger lazy expiry on master via a read command.
    # Each variant exercises a different DeleteSetIfEmpty call site.
    await c_master.execute_command(*trigger_cmd)

    # The key should be gone on master
    assert await c_master.exists("myset") == 0

    await check_all_replicas_finished([c_replica], c_master)

    assert await c_replica.exists("myset") == 0, (
        f"Replica still has 'myset' after lazy expiry triggered by {trigger_cmd[0]}. "
        "DeleteSetIfEmpty must journal the deletion so the replica stays in sync."
    )


@dfly_args({"proactor_threads": 4})
async def test_hnsw_external_vector_replication_crash(df_factory: DflyInstanceFactory):
    """
    Minimal reproducer for SIGSEGV during HNSW replication with external vectors.

    The HNSW graph is global (shared across shards) but is_restoring_vectors_ is per-shard.
    When one shard finishes RestoreGlobalVectorIndices and starts accepting new HSETs,
    addPoint() traverses the global graph and may dereference nullptr data pointers
    from nodes belonging to shards that haven't finished restoration yet.
    """
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # dim=256 forces external vectors (copy_vector=false).
    # 500 docs creates a larger HNSW graph, widening the restoration window.
    seeder = HnswSearchSeeder(num_initial_docs=500, num_dims=256)
    await seeder.create_index(c_master)
    await seeder.seed_initial_docs(c_master)

    # Start heavy traffic on master BEFORE replication begins.
    # This ensures journal events (HSETs) arrive on the replica while HNSW graph
    # is being restored, triggering addPoint() with nullptr data pointers.
    traffic_task = asyncio.create_task(seeder.run_traffic(c_master, sleep_interval=0.001))

    await start_replication(c_replica, master.port)

    seeder.stop()
    await traffic_task

    await check_all_replicas_finished([c_replica], c_master, timeout=60)


async def _ft_num_docs(client, index_name: str) -> int:
    info = await client.execute_command("FT.INFO", index_name)
    info_map = dict(zip(info[::2], info[1::2]))
    return int(info_map["num_docs"])


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize(
    "docs_keep",
    [
        pytest.param(2000, id="small"),
        pytest.param(100_000, id="100k", marks=HNSW_LARGE_MARKS),
    ],
)
async def test_hnsw_multi_replica_with_concurrent_index_ops(
    df_factory: DflyInstanceFactory, docs_keep: int
):
    """
    Fan-out HNSW replication with a pre-existing index dropped and a new index
    created shortly after the replicas connect. Verifies doc-count parity and
    KNN coverage across master and every replica.

    Covers next:
      - Multiple replicas (3 fan-out)
      - FT.DROPINDEX of a pre-existing index, 0.5-5s after replicas connect
      - FT.CREATE of a new index, 0.5-5s after replicas connect
      - Document-count parity master/replica per surviving index
      - 100k-document HNSW main index (large variant)
    """
    master = df_factory.create()
    replicas = [df_factory.create(proactor_threads=2) for _ in range(3)]
    df_factory.start_all([master] + replicas)

    c_master = master.client()
    c_replicas = [r.client() for r in replicas]

    seeder_keep = HnswSearchSeeder(
        num_initial_docs=docs_keep, num_dims=8, index_name="idx_keep", prefix="keep:"
    )
    seeder_drop = HnswSearchSeeder(
        num_initial_docs=1000, num_dims=8, index_name="idx_drop", prefix="drop:"
    )
    seeder_new = HnswSearchSeeder(
        num_initial_docs=500, num_dims=8, index_name="idx_new", prefix="new:"
    )

    await seeder_keep.create_index(c_master)
    await seeder_keep.seed_initial_docs(c_master)
    await seeder_drop.create_index(c_master)
    await seeder_drop.seed_initial_docs(c_master)

    traffic_task = asyncio.create_task(seeder_keep.run_traffic(c_master))
    sync_timeout = _hnsw_sync_timeout(docs_keep)

    try:
        await asyncio.gather(
            *(c.execute_command(f"REPLICAOF localhost {master.port}") for c in c_replicas)
        )

        # Drop the pre-existing index and create the new one, each at an
        # independent random offset in the (0.5, 5)s window after REPLICAOF.
        # The randomness is intentional: it spreads the ops across the replica's
        # full-sync window to exercise the indexing race. Log the offsets so a
        # failure is reproducible.
        drop_delay = random.uniform(0.5, 5)
        create_delay = random.uniform(0.5, 5)
        logging.info(f"drop_delay={drop_delay:.2f}s create_delay={create_delay:.2f}s")

        async def drop_after_delay():
            await asyncio.sleep(drop_delay)
            await c_master.execute_command("FT.DROPINDEX", "idx_drop")

        async def create_after_delay():
            await asyncio.sleep(create_delay)
            await seeder_new.create_index(c_master)
            await seeder_new.seed_initial_docs(c_master)

        await asyncio.gather(drop_after_delay(), create_after_delay())

        master_indices = set(await c_master.execute_command("FT._LIST"))
        assert master_indices == {
            "idx_keep",
            "idx_new",
        }, f"Master indices unexpected: {master_indices}"

        await wait_available_async(c_replicas, timeout=sync_timeout)
        seeder_keep.stop()
        await traffic_task
        await check_all_replicas_finished(c_replicas, c_master, timeout=sync_timeout)

        for c in c_replicas:
            idx_list = set(await c.execute_command("FT._LIST"))
            assert idx_list == {"idx_keep", "idx_new"}, f"Unexpected indices: {idx_list}"

        # HNSW background indexing on the replica can lag the journal LSN, so
        # poll until num_docs converges. Fan out across all (replica × index) pairs.
        @assert_eventually(timeout=sync_timeout)
        async def _converged(client, index_name: str, expected: int):
            actual = await _ft_num_docs(client, index_name)
            assert actual == expected, f"{index_name}: replica={actual} master={expected}"

        index_to_expected = {
            name: await _ft_num_docs(c_master, name) for name in ("idx_keep", "idx_new")
        }
        await asyncio.gather(
            *(
                _converged(c, name, expected)
                for c in c_replicas
                for name, expected in index_to_expected.items()
            )
        )

        await asyncio.gather(*(seeder_keep.verify(c_master, c) for c in c_replicas))
        await asyncio.gather(*(seeder_new.verify(c_master, c) for c in c_replicas))

    finally:
        seeder_keep.stop()
        traffic_task.cancel()
        try:
            await traffic_task
        except (asyncio.CancelledError, Exception):
            pass


async def test_snapshot_load_replication(df_factory: DflyInstanceFactory):
    dbfilename = f"dump_{tmp_file_name()}"

    master = df_factory.create()
    replica = df_factory.create()
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Populate initial data and save a snapshot.
    seeder = DebugPopulateSeeder(key_target=1000, data_size=100)
    await seeder.run(c_master)
    await c_master.execute_command("SAVE", "DF", dbfilename)
    await c_master.execute_command("FLUSHALL")

    await c_replica.execute_command("REPLICAOF", "localhost", str(master.port))
    await wait_available_async(c_replica)

    # Stream writes during DFLY LOAD to exercise the race between journal
    # writes and the load that bypasses the journal. LOADING state rejects
    # seeder Lua scripts, so the seeder task may fail.
    stream_seeder = SeederV2(key_target=500)
    seed_task = asyncio.create_task(stream_seeder.run(c_master, target_deviation=0.1))
    await asyncio.sleep(
        0.5
    )  # Let the seeder start and write some data before we load the snapshot.

    await c_master.execute_command("DFLY", "LOAD", f"{dbfilename}-summary.dfs")

    await asyncio.sleep(0.5)  # Let the seeder fail because of the loading state.
    seed_task.cancel()
    try:
        await seed_task
    except (asyncio.CancelledError, Exception):
        pass

    # Wait for the replica to complete the new full sync.
    await wait_for_replicas_state(c_replica)
    await check_all_replicas_finished([c_replica], c_master)

    master_capture = await DebugPopulateSeeder.capture(c_master)
    replica_capture = await DebugPopulateSeeder.capture(c_replica)
    assert master_capture == replica_capture

    await c_replica.execute_command("REPLICAOF", "NO", "ONE")


@pytest.mark.asyncio
async def test_bgsave_during_stable_sync(df_factory: DflyInstanceFactory):
    """
    shard_stable_sync_read when a BGSAVE is running on the replica concurrently
    """
    master, [replica], c_master, [c_replica] = await setup_replication(
        df_factory,
        master_args={"proactor_threads": 8, "num_shards": 8},
        replica_args={
            "proactor_threads": 8,
            "num_shards": 8,
            "dir": "{DRAGONFLY_TMP}/",
            "dbfilename": "test-replica-bgsave",
        },
        connect=False,
    )
    del_clients = [master.client() for _ in range(16)]

    await c_master.execute_command("DEBUG", "POPULATE", "100000", "k", "100")

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)

    stop_event = asyncio.Event()

    async def del_loop(client, slot):
        i = slot
        while not stop_event.is_set():
            try:
                pipe = client.pipeline(transaction=False)
                for j in range(50):
                    pipe.delete(f"k:{(i + j) % 100000}")
                    pipe.set(f"k:{(i + j) % 100000}", f"v{i + j}")
                await pipe.execute()
                i += 50
            except Exception:
                stop_event.set()
                break

    async def bgsave_loop():
        for _ in range(20):
            if stop_event.is_set():
                break
            try:
                await c_replica.execute_command("BGSAVE")
                while await is_saving(c_replica):
                    await asyncio.sleep(0)
            except Exception:
                stop_event.set()
                break
        stop_event.set()

    await asyncio.gather(
        bgsave_loop(),
        *[del_loop(c, i * 6250) for i, c in enumerate(del_clients)],
        return_exceptions=True,
    )

    for c in del_clients:
        await c.aclose()

    await c_master.aclose()
    await c_replica.aclose()
    master.stop()
    replica.stop()

    bad_lines = replica.find_in_logs("Unexpected fiber")
    assert not bad_lines, (
        "DbSlice change callback fired from shard_stable_sync_read during BGSAVE "
        "(SerializerBase::OnChangeBlocking invariant violated).\n" + "\n".join(bad_lines)
    )


# Verify chunked CF replication stays within the max chunk size budget.
@pytest.mark.large
async def test_cf_chunked_replication_chunk_size(df_factory: DflyInstanceFactory):
    master = df_factory.create(proactor_threads=1, maxmemory="512m")
    replica = df_factory.create(proactor_threads=1, maxmemory="512m")

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("CF.RESERVE", "cf", "100000000")

    cf_size = await c_master.execute_command("MEMORY USAGE", "cf")
    assert cf_size > 2**26, f"Cuckoo filter should be >64MB, got {cf_size}"

    random_seed = random.getrandbits(64)
    logging.info(f"Using random seed {random_seed} for test reproducibility")
    test_rng = random.Random(random_seed)

    num_items = 200
    random_items = [f"item:{i}" for i in test_rng.sample(range(1_000_000), num_items)]
    async with c_master.pipeline(transaction=False) as pipe:
        for item in random_items:
            pipe.execute_command("CF.ADD", "cf", item)
        await pipe.execute()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    async with async_timeout.timeout(240):
        await wait_for_replicas_state(c_replica)

    replica_results = await asyncio.gather(
        *(c_replica.execute_command("CF.EXISTS", "cf", item) for item in random_items)
    )
    missing = sum(1 for r in replica_results if not r)
    assert missing == 0, f"{missing} of {num_items} items missing on replica"

    master.stop()
    replica.stop()

    # kFilterChunkSize = 1 << 26 (64MB); peak should stay under 2x that.
    max_cf_chunk_size = 2**27
    lines = master.find_in_logs("Serialization peak bytes: ")
    assert len(lines) == 1
    peak_bytes = extract_int_after_prefix("Serialization peak bytes: ", lines[0])
    assert peak_bytes < max_cf_chunk_size
