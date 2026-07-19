import asyncio
import logging
import random
import re
import time

import async_timeout
import pytest
import redis
from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstanceFactory
from .seeder import DebugPopulateSeeder
from .seeder import Seeder as SeederV2
from .replication_utils import (
    ADMIN_PORT,
    assert_replica_data_matches,
    get_metric_value,
    master_role_reply,
    replica_role_reply,
    setup_replication,
    start_replication,
)
from .utility import (
    assert_eventually,
    batch_fill_data,
    check_all_replicas_finished,
    gen_test_data,
    parse_client_list,
    wait_available_async,
    wait_for_replicas_state,
)

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

M_OPT = [pytest.mark.opt_only]
M_SLOW = [pytest.mark.large]


M_STRESS = [pytest.mark.large, pytest.mark.opt_only]
M_NOT_EPOLL = [pytest.mark.exclude_epoll]


"""
Test full replication pipeline. Test full sync with streaming changes and stable state streaming.
"""


@pytest.mark.parametrize(
    "t_master, t_replicas, seeder_config, stream_target",
    [
        # Quick general test that replication is working
        (1, 3 * [1], dict(key_target=1_000), 500),
        (1, 2 * [1], dict(key_target=2_000, types=["LIST"]), 500),
        (1, 2 * [1], dict(key_target=2_000, types=["LIST"]), 2000),
        # A lot of huge values
        (2, 2 * [1], dict(key_target=5_000, huge_value_target=30), 500),
        (4, [4, 4], dict(key_target=10_000), 1_000),
        pytest.param(6, [6, 6, 6], dict(key_target=100_000), 20_000, marks=M_OPT),
        # Skewed tests with different thread ratio
        pytest.param(8, 6 * [1], dict(key_target=5_000), 2_000, marks=M_SLOW),
        pytest.param(2, [8, 8], dict(key_target=10_000), 2_000, marks=M_SLOW),
        # Everything is big because data size is 10k
        pytest.param(
            2, [2], dict(key_target=1_000, data_size=10_000, huge_value_target=0), 100, marks=M_SLOW
        ),
        # Stress test
        # Single replica process might have additional optimizations that need to be verified
        pytest.param(4, [4], dict(key_target=500_000, types=["STRING"]), 100_000, marks=M_STRESS),
        pytest.param(8, [8, 8], dict(key_target=1_000_000, units=16), 50_000, marks=M_STRESS),
    ],
)
@pytest.mark.parametrize("mode", [({}), ({"cache_mode": "true"})])
@pytest.mark.parametrize("tagged_chunks", [True, False])
async def test_replication_all(
    df_factory: DflyInstanceFactory,
    t_master,
    t_replicas,
    seeder_config,
    stream_target,
    mode,
    tagged_chunks,
):
    args = {}
    if mode:
        args["cache_mode"] = "true"
        args["maxmemory"] = str(t_master * 256) + "mb"

    args["serialization_tagged_chunks"] = tagged_chunks

    master = df_factory.create(
        admin_port=ADMIN_PORT,
        proactor_threads=t_master,
        **args,
    )
    replicas = [
        df_factory.create(admin_port=ADMIN_PORT + i + 1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    from_admin_port = random.choice([True, False])

    # Start instances and connect clients
    df_factory.start_all([master] + replicas)
    c_master = master.client()
    c_replicas = [replica.client() for replica in replicas]

    # Fill master with test data
    seeder = SeederV2(**seeder_config, huge_value_add_only=True)
    await seeder.run(c_master, target_deviation=0.01)

    # Start data stream
    stream_task = asyncio.create_task(seeder.run(c_master))
    await asyncio.sleep(0.0)

    # Start replication
    master_port = master.port if not from_admin_port else master.admin_port
    await asyncio.gather(
        *(
            asyncio.create_task(c.execute_command("REPLICAOF localhost " + str(master_port)))
            for c in c_replicas
        )
    )

    # Wait for all replicas to transition into stable sync
    async with async_timeout.timeout(240):
        await wait_for_replicas_state(*c_replicas)

    # Stop streaming data once every replica is in stable sync
    await seeder.stop(c_master)
    await stream_task

    # Check data after full sync
    async def check():
        await assert_replica_data_matches(c_master, c_replicas)

    await check()
    # Stream more data in stable state
    await seeder.run(c_master, target_ops=stream_target)

    # Check data after stable state stream
    await check()

    info = await c_master.info()
    preemptions = info["big_value_preemptions"]
    key_capacity = info["prime_capacity"]
    compressed_blobs = info["compressed_blobs"]
    logging.debug(
        f"Compressed blobs {compressed_blobs} .Capacity {key_capacity}. Preemptions {preemptions}"
    )

    if len(replicas) > 1:
        assert preemptions >= seeder.huge_value_target * 0.5
    assert compressed_blobs > 0
    # Because data size could be 10k and for that case there will be almost a preemption
    # per bucket.
    if seeder.data_size < 1000:
        # We care that we preempt less times than the total buckets such that we can be
        # sure that we test both flows (with and without preemptions). Preemptions on 3%
        # of buckets seems like a big number but that depends on a few parameters like
        # the size of the hug value and the serialization max chunk size. For the test cases here,
        # it's usually close to 1% but there are some that are close to 3.
        assert preemptions <= (key_capacity * 0.03)

    if len(replicas) == 1 and seeder_config["key_target"] > 100_000:
        print("total omits", info["total_journal_omits"])
        # assert info["total_journal_omits"] > 0 # TODO: Even with large key number it doesn't fire sometimes

    # Assert select calls are properly optimized
    for replica in c_replicas:
        select_calls = (await replica.info("ALL"))["cmdstat_select"]["calls"]
        assert select_calls < 16


"""
Regression test for the double-apply bug during full sync.

When a bucket is mutated during snapshot traversal *before* the snapshot loop visits it,
SliceSnapshot serializes the mutation twice into the full sync stream:
  1. OnChange fires -> bucket (post-mutation state) serialized into RDB
  2. ConsumeJournalChange fires -> the same command also written as a journal blob

The replica applies both: loads the bucket state, then re-executes the journal command
on top. For additive operations (LPUSH/RPUSH) this doubles the list contents; for
removals (LPOP/RPOP) it removes one extra element.

The race is most pronounced with background_snapshotting=False (normal priority snapshot
fiber): the snapshot holds the CPU for a full time-slice, causing queued mutations to
burst against a large batch of not-yet-visited buckets the moment it yields.
serialization_max_chunk_size=500 forces frequent yields within that burst.
"""


async def test_replication_list_double_apply(df_factory: DflyInstanceFactory):
    """Regression test for double-apply of LIST mutations during full sync."""
    master = df_factory.create(
        proactor_threads=2,
        num_shards=1,
        serialization_max_chunk_size=500,
    )
    replica = df_factory.create(proactor_threads=1)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Pre-fill master with LIST keys so the snapshot has real data to traverse.
    seeder = SeederV2(
        key_target=2000, types=["LIST"], data_size=100, huge_value_size=70000, huge_value_target=6
    )
    await seeder.run(c_master, target_deviation=0.01)

    # Stream LIST mutations concurrently with full sync. add_list (LPUSH) and mod_list
    # (LPUSH/RPUSH/LPOP/RPOP) both hit the race: additive ops produce doubles, removals
    # produce under-counts.
    stream_task = asyncio.create_task(seeder.run(c_master))
    await asyncio.sleep(0.0)

    # Trigger full sync while mutations are in flight.
    await c_replica.execute_command("REPLICAOF localhost " + str(master.port))

    async with async_timeout.timeout(120):
        await wait_for_replicas_state(c_replica)

    await seeder.stop(c_master)
    await stream_task

    await assert_replica_data_matches(c_master, [c_replica])


"""
Test flushall command. Set data to master send flashall and set more data.
Check replica keys at the end.
"""


@pytest.mark.replication(master_args={"proactor_threads": 4}, replica_args={"proactor_threads": 2})
async def test_flushall(replication):
    _, _, c_master, [c_replica] = replication

    n_keys = 1000

    def gen_test_data(start, end):
        for i in range(start, end):
            yield f"key-{i}", f"value-{i}"

    pipe = c_master.pipeline(transaction=False)
    # Set simple keys 0..n_keys on master
    batch_fill_data(client=pipe, gen=gen_test_data(0, n_keys), batch_size=3)
    # flushall
    pipe.flushall()
    # Set simple keys n_keys..n_keys*2 on master
    batch_fill_data(client=pipe, gen=gen_test_data(n_keys, n_keys * 2), batch_size=3)

    await pipe.execute()
    # Check replica finished executing the replicated commands
    await check_all_replicas_finished([c_replica], c_master)

    # Check replica keys 0..n_keys-1 dont exist
    pipe = c_replica.pipeline(transaction=False)
    for i in range(n_keys):
        pipe.get(f"key-{i}")
    vals = await pipe.execute()
    assert all(v is None for v in vals)

    # Check replica keys n_keys..n_keys*2-1 exist
    for i in range(n_keys, n_keys * 2):
        pipe.get(f"key-{i}")
    vals = await pipe.execute()
    assert all(v is not None for v in vals)


"""
Test journal rewrites.
"""


@dfly_args({"proactor_threads": 4})
@pytest.mark.replication
async def test_rewrites(replication):
    CLOSE_TIMESTAMP = int(time.time()) + 100
    CLOSE_TIMESTAMP_MS = CLOSE_TIMESTAMP * 1000

    master, [replica], c_master, [c_replica] = replication

    # Create monitor and bind utility functions
    m_replica = c_replica.monitor()

    async def get_next_command():
        mcmd = (await m_replica.next_command())["command"]
        # skip select command
        while mcmd == "SELECT 0" or mcmd.startswith("CLIENT SETINFO"):
            mcmd = (await m_replica.next_command())["command"]
        print("Got:", mcmd)
        return mcmd

    async def is_match_rsp(rx):
        mcmd = await get_next_command()
        print(mcmd, rx)
        return re.match(rx, mcmd)

    async def skip_cmd():
        await is_match_rsp(r".*")

    async def skip_cmds(n):
        for _ in range(n):
            await skip_cmd()

    async def check(cmd, rx):
        await c_master.execute_command(cmd)
        match = await is_match_rsp(rx)
        assert match

    async def check_list(cmd, rx_list):
        print("master cmd:", cmd)
        await c_master.execute_command(cmd)
        for rx in rx_list:
            match = await is_match_rsp(rx)
            assert match

    async def check_list_ooo(cmd, rx_list):
        print("master cmd:", cmd)
        await c_master.execute_command(cmd)
        expected_cmds = len(rx_list)
        for i in range(expected_cmds):
            mcmd = await get_next_command()
            # check command matches one regex from list
            match_rx = list(filter(lambda rx: re.match(rx, mcmd), rx_list))
            assert len(match_rx) == 1
            rx_list.remove(match_rx[0])

    async def check_expire(key):
        ttl1 = await c_master.ttl(key)
        ttl2 = await c_replica.ttl(key)
        await skip_cmd()
        assert abs(ttl1 - ttl2) <= 1

    async with m_replica:
        # CHECK EXPIRE, PEXPIRE, PEXPIRE turn into EXPIREAT
        await c_master.set("k-exp", "v")
        await skip_cmd()
        await check("EXPIRE k-exp 100", r"PEXPIREAT k-exp (.*?)")
        await check_expire("k-exp")
        await check("PEXPIRE k-exp 50000", r"PEXPIREAT k-exp (.*?)")
        await check_expire("k-exp")
        await check(f"EXPIREAT k-exp {CLOSE_TIMESTAMP}", rf"PEXPIREAT k-exp {CLOSE_TIMESTAMP_MS}")

        # Check SPOP turns into SREM or SDEL
        await c_master.sadd("k-set", "v1", "v2", "v3")
        await skip_cmd()
        await check("SPOP k-set 1", r"SREM k-set (v1|v2|v3)")
        await check("SPOP k-set 2", r"DEL k-set")

        # Check SET + {EX/PX/EXAT} + {XX/NX/GET} arguments turns into SET PXAT
        await check("SET k v EX 100 NX GET", r"SET k v PXAT (.*?)")
        await check_expire("k")
        await check("SET k v PX 50000", r"SET k v PXAT (.*?)")
        await check_expire("k")
        # Exact expiry is skewed
        await check(f"SET k v XX EXAT {CLOSE_TIMESTAMP}", r"SET k v PXAT (.*?)")
        await check_expire("k")

        # Check SET + KEEPTTL doesn't loose KEEPTTL
        await check("SET k v KEEPTTL", r"SET k v KEEPTTL")

        # Check SETEX/PSETEX turn into SET PXAT
        await check("SETEX k 100 v", r"SET k v PXAT (.*?)")
        await check_expire("k")
        await check("PSETEX k 500000 v", r"SET k v PXAT (.*?)")
        await check_expire("k")

        # Check GETEX turns into PEXPIREAT or PERSIST
        await check("GETEX k PERSIST", r"PERSIST k")
        await check_expire("k")
        await check("GETEX k EX 100", r"PEXPIREAT k (.*?)")
        await check_expire("k")

        # Bare GETEX (no EX/PX/EXAT/PXAT/PERSIST) is read-only and must not journal.
        # If it did, the next SET would not be the next replicated command.
        await c_master.execute_command("GETEX k")
        await check("SET marker after-bare-getex", r"SET marker after-bare-getex")

        # Check SDIFFSTORE turns into DEL and SADD
        await c_master.sadd("set1", "v1", "v2", "v3")
        await c_master.sadd("set2", "v1", "v2")
        await skip_cmd()
        await skip_cmd()
        await check_list("SDIFFSTORE k set1 set2", [r"DEL k", r"SADD k v3"])

        # Check SINTERSTORE turns into DEL and SADD
        await check_list("SINTERSTORE k set1 set2", [r"DEL k", r"SADD k (.*?)"])

        # Check SMOVE turns into SREM and SADD
        await check_list_ooo("SMOVE set1 set2 v3", [r"SREM set1 v3", r"SADD set2 v3"])

        # Check SUNIONSTORE turns into DEL and SADD
        await check_list_ooo("SUNIONSTORE k set1 set2", [r"DEL k", r"SADD k (.*?)"])

        # Check ZDIFFSTORE turns into DEL and ZADD
        await c_master.execute_command("zadd zet1 1 v1 2 v2 3 v3")
        await c_master.execute_command("zadd zet2 1 v1 2 v2")
        await skip_cmd()
        await skip_cmd()
        await check_list("ZDIFFSTORE k 2 zet1 zet2", [r"DEL k", r"ZADD k 3 v3"])

        # Check ZINTERSTORE turns into DEL and ZADD
        await check_list("ZINTERSTORE k 2 zet1 zet2", [r"DEL k", r"ZADD k (.*?)"])

        # Check ZRANGESTORE turns into SREM and ZADD
        await check_list_ooo("ZRANGESTORE k zet1 2 -1", [r"DEL k", r"ZADD k 3 v3"])

        # Check ZUNIONSTORE turns into DEL and ZADD
        await check_list_ooo("ZUNIONSTORE k 2 zet1 zet2", [r"DEL k", r"ZADD k (.*?)"])

        await c_master.set("k1", "1000")
        await c_master.set("k2", "1100")
        await skip_cmd()
        await skip_cmd()
        # Check BITOP turns into SET
        await check("BITOP OR kdest k1 k2", r"SET kdest 1100")
        # See gh issue #3528
        await c_master.execute_command("HSET foo bar val")
        await skip_cmd()
        await check("BITOP NOT foo tmp", r"DEL foo")
        await c_master.execute_command("HSET foo bar val")
        await skip_cmd()
        await c_master.set("k3", "-")
        await skip_cmd()
        await check("BITOP NOT foo k3", r"SET foo \\xd2")

        # Check there is no rewrite for LMOVE on single shard
        await c_master.lpush("list", "v1", "v2", "v3", "v4")
        await skip_cmd()
        # Check LMOVE/BLMOVE turns into POP PUSH
        await check_list_ooo("LMOVE list list LEFT RIGHT", [r"LPOP list", r"RPUSH list v4"])
        await check_list_ooo("BLMOVE list list RIGHT LEFT 0", [r"RPOP list", r"LPUSH list v4"])

        # Check RPOPLPUSH turns into RPOP LPUSH
        await check_list_ooo("RPOPLPUSH list list", [r"RPOP list", r"LPUSH list v1"])
        # Check BRPOPLPUSH turns into RPOP LPUSH
        await check_list_ooo("BRPOPLPUSH list list 0", [r"RPOP list", r"LPUSH list v2"])
        # Check BLPOP turns into LPOP
        await check("BLPOP list list1 0", r"LPOP list")
        # Check BRPOP turns into RPOP
        await check("BRPOP list 0", r"RPOP list")

        await c_master.lpush("list1s", "v1", "v2", "v3", "v4")
        await skip_cmd()
        # Check LMOVE turns into LPUSH LPOP on multi shard
        await check_list_ooo("LMOVE list1s list2s LEFT LEFT", [r"LPUSH list2s v4", r"LPOP list1s"])
        # Check RPOPLPUSH turns into LPUSH RPOP on multi shard
        await check_list_ooo("RPOPLPUSH list1s list2s", [r"LPUSH list2s v1", r"RPOP list1s"])
        # Check BRPOPLPUSH turns into LPUSH RPOP on multi shard
        await check_list_ooo("BRPOPLPUSH list1s list2s 0", [r"LPUSH list2s v2", r"RPOP list1s"])

        await check("LMPOP 2 list list1s LEFT", r"LPOP list")
        await check("BLMPOP 0 2 list1s list RIGHT", r"RPOP list1s")

        # MOVE runs as global command, check only one journal entry is sent
        await check("MOVE list2s 2", r"MOVE list2s 2")

        await c_master.set("renamekey", "1000", px=50000)
        await skip_cmd()
        # Check RENAME turns into DEL and RESTORE
        await check_list_ooo(
            "RENAME renamekey renamed",
            [r"DEL renamekey", r"RESTORE renamed (.*?) (.*?) REPLACE ABSTTL"],
        )
        await check_expire("renamed")
        # Check RENAMENX turns into DEL and RESTORE
        await check_list_ooo(
            "RENAMENX renamed renamekey",
            [r"DEL renamed", r"RESTORE renamekey (.*?) (.*?) REPLACE ABSTTL"],
        )
        await check_expire("renamekey")

        # Test autojournaling in the multi-mode
        await c_master.execute_command("XADD k-stream * field value")
        await c_master.execute_command("SADD k-one-element-set value1 value2")
        sha = await c_master.script_load(
            "redis.call('XTRIM', KEYS[1], 'MINID', '0'); return redis.call('SPOP', KEYS[2]);"
        )
        await skip_cmds(3)
        # The first call to XTRIM triggers autojournaling.
        # The SPOP command is executed with CO::NO_AUTOJOURNALING.
        # This test ensures that the SPOP command is still properly replicated
        await check_list_ooo(
            f"EVALSHA {sha} 2 k-stream k-one-element-set",
            [r"XTRIM k-stream MINID 0", r"SREM k-one-element-set value[12]"],
        )

        # TODO next Z-tests won't work with no-point-in-time replication
        # check BZMPOP turns into ZPOPMAX and ZPOPMIN command
        await c_master.zadd("key", {"a": 1, "b": 2, "c": 3})
        await skip_cmd()
        await check("BZMPOP 0 3 key3 key2 key MAX COUNT 3", r"ZPOPMAX key 3")

        await c_master.zadd("key", {"a": 1, "b": 2, "c": 3})
        await skip_cmd()
        await check("BZMPOP 0 3 key3 key2 key MIN", r"ZPOPMIN key 1")

        # Check ZMPOP turns into ZPOPMAX and ZPOPMIN commands
        await c_master.zadd("key", {"a": 1, "b": 2, "c": 3})
        await skip_cmd()
        await check("ZMPOP 3 key3 key2 key MIN COUNT 3", r"ZPOPMIN key 3")

        await c_master.zadd("key", {"a": 1, "b": 2, "c": 3})
        await skip_cmd()
        await check("ZMPOP 3 key3 key2 key MAX", r"ZPOPMAX key 1")

        # Check XREADGROUP turns into XGROUP SETID + XCLAIM (for non-NOACK)
        await c_master.execute_command("XGROUP CREATE mystream mygroup $ MKSTREAM")
        await skip_cmd()
        await c_master.execute_command("XADD mystream * field1 value1")
        await skip_cmd()
        # XREADGROUP without NOACK should journal XCLAIM + XGROUP SETID
        await c_master.execute_command("XREADGROUP GROUP mygroup consumer1 STREAMS mystream >")
        # Consumer creation
        assert await is_match_rsp("XGROUP CREATECONSUMER mystream mygroup consumer1")
        # Expect XCLAIM for the message + XGROUP SETID with ENTRIESREAD
        assert await is_match_rsp(
            r"XCLAIM mystream mygroup consumer1 0 (.*?) TIME \d+ RETRYCOUNT 1 FORCE JUSTID LASTID (.*?)"
        )
        assert await is_match_rsp(r"XGROUP SETID mystream mygroup (.*?) ENTRIESREAD 1")

        # Check XREADGROUP with NOACK only journals XGROUP SETID
        await c_master.execute_command("XADD mystream * field2 value2")
        await skip_cmd()
        await c_master.execute_command(
            "XREADGROUP GROUP mygroup consumer1 NOACK STREAMS mystream >"
        )
        # With NOACK, only XGROUP SETID should be journaled (no XCLAIM)
        assert await is_match_rsp(r"XGROUP SETID mystream mygroup (.*?) ENTRIESREAD 2")


"""
Test automatic replication of expiry.
"""


@dfly_args({"proactor_threads": 4})
@pytest.mark.replication
async def test_expiry(replication, n_keys=1000):
    master, [replica], c_master, [c_replica] = replication

    # Set keys
    pipe = c_master.pipeline(transaction=False)
    batch_fill_data(pipe, gen_test_data(n_keys))
    await pipe.execute()

    # Check replica finished executing the replicated commands
    await check_all_replicas_finished([c_replica], c_master)
    # Check keys are on replica
    res = await c_replica.mget(k for k, _ in gen_test_data(n_keys))
    assert all(v is not None for v in res)

    # Set key different expries times in ms
    pipe = c_master.pipeline(transaction=True)
    for k, _ in gen_test_data(n_keys):
        ms = random.randint(20, 500)
        pipe.pexpire(k, ms)
    await pipe.execute()

    # send more traffic for differnt dbs while keys are expired
    for i in range(8):
        is_multi = i % 2
        async with aioredis.Redis(port=master.port, db=i) as c_master_db:
            pipe = c_master_db.pipeline(transaction=is_multi)
            # Set simple keys n_keys..n_keys*2 on master
            start_key = n_keys * (i + 1)
            end_key = start_key + n_keys
            batch_fill_data(client=pipe, gen=gen_test_data(end_key, start_key), batch_size=20)

            await pipe.execute()

    # Wait for master to expire keys
    await asyncio.sleep(3.0)

    # Check all keys with expiry have been deleted
    res = await c_master.mget(k for k, _ in gen_test_data(n_keys))
    assert all(v is None for v in res)

    # Check replica finished executing the replicated commands
    await check_all_replicas_finished([c_replica], c_master)
    res = await c_replica.mget(k for k, _ in gen_test_data(n_keys))
    assert all(v is None for v in res)

    # Set expired keys again
    pipe = c_master.pipeline(transaction=False)
    batch_fill_data(pipe, gen_test_data(n_keys))
    for k, _ in gen_test_data(n_keys):
        pipe.pexpire(k, 500)
    await pipe.execute()
    await asyncio.sleep(1.0)
    # Disconnect from master
    await c_replica.execute_command("REPLICAOF NO ONE")
    # Check replica expires keys on its own
    await asyncio.sleep(1.0)
    res = await c_replica.mget(k for k, _ in gen_test_data(n_keys))
    assert all(v is None for v in res)


@dfly_args({"proactor_threads": 4, "replica_delete_expired": "true"})
@pytest.mark.replication(master_args={"hz": 0}, replica_args={"hz": 0})
async def test_expiry_on_replica(replication):
    """
    Test that expired keys on a replica are proactively deleted (not just hidden) on the read
    path when replica_delete_expired=true. Without the fix, replicas kept stale data in memory
    and ExpireIfNeeded returned valid iterators, causing GET to return stale values and TTL to
    return huge unsigned values (~UINT64_MAX).
    """
    _, _, c_master, [c_replica] = replication

    # Set keys with short TTL on master
    n_keys = 50
    for i in range(n_keys):
        await c_master.execute_command("SET", f"exptest-{i}", f"value-{i}", "PX", 2000)

    # Set some keys without TTL to verify they are unaffected
    for i in range(n_keys):
        await c_master.execute_command("SET", f"persist-{i}", f"pvalue-{i}")

    await check_all_replicas_finished([c_replica], c_master)

    # Verify keys exist on replica before expiry
    for i in range(n_keys):
        val = await c_replica.get(f"exptest-{i}")
        assert val is not None, f"exptest-{i} should exist before expiry"

    # Wait for keys to expire (hz=0 disables background sweep)
    await asyncio.sleep(3.0)

    # Check expired keys on replica WITHOUT touching them on master first.
    # Before the fix, GET returned stale values and TTL returned ~UINT64_MAX.
    for i in range(n_keys):
        val = await c_replica.get(f"exptest-{i}")
        assert val is None, f"exptest-{i} should be nil on replica after expiry"
        ttl = await c_replica.execute_command("TTL", f"exptest-{i}")
        assert ttl == -2, f"exptest-{i} TTL should be -2 on replica, got {ttl}"

    # Verify non-expiry keys are unaffected
    for i in range(n_keys):
        val = await c_replica.get(f"persist-{i}")
        assert val == f"pvalue-{i}", f"persist-{i} should still have its value"
        ttl = await c_replica.execute_command("TTL", f"persist-{i}")
        assert ttl == -1

    # Verify replication still works after reading expired keys
    await c_master.execute_command("SET", "after-expiry", "works")
    await check_all_replicas_finished([c_replica], c_master)
    val = await c_replica.get("after-expiry")
    assert val == "works", "Replication should still be healthy"

    # Verify SET via replication can overwrite an expired key on replica
    await c_master.execute_command("SET", "exptest-0", "new-value")
    await check_all_replicas_finished([c_replica], c_master)
    val = await c_replica.get("exptest-0")
    assert val == "new-value", "Replication SET should overwrite expired key on replica"


@dfly_args({"proactor_threads": 4})
@pytest.mark.replication(replicas=2)
async def test_simple_scripts(replication):
    _, _, c_master, c_replicas = replication

    await check_all_replicas_finished(c_replicas, c_master)

    # Generate some scripts and run them
    keys = ["a", "b", "c", "d", "e"]
    for i in range(len(keys) + 1):
        script = ""
        subkeys = keys[:i]
        for key in subkeys:
            script += f"redis.call('INCR', '{key}')"
            script += f"redis.call('INCR', '{key}')"

        await c_master.eval(script, len(subkeys), *subkeys)

    # Wait for replicas
    await check_all_replicas_finished(c_replicas, c_master)

    for c_replica in c_replicas:
        assert (await c_replica.mget(keys)) == ["10", "8", "6", "4", "2"]


"""
Test script replication.

Fill multiple lists with values and rotate them one by one with LMOVE until they're at the same place again.
"""

# t_master, t_replicas, num_ops, num_keys, num_parallel, flags
script_cases = [
    (4, [4, 4, 4], 50, 5, 5, ""),
    (4, [4, 4, 4], 50, 5, 5, "disable-atomicity"),
]

script_test_s1 = """
{flags}
local N = ARGV[1]

-- fill each list with its k value
for i, k in pairs(KEYS) do
  for j = 1, N do
    redis.call('LPUSH', k, i-1)
  end
end

-- rotate #KEYS times
for l = 1, #KEYS do
  for j = 1, N do
    for i, k in pairs(KEYS) do
      redis.call('LMOVE', k, KEYS[i%#KEYS+1], 'LEFT', 'RIGHT')
    end
  end
end


return 'OK'
"""


@pytest.mark.parametrize("t_master, t_replicas, num_ops, num_keys, num_par, flags", script_cases)
async def test_scripts(df_factory, t_master, t_replicas, num_ops, num_keys, num_par, flags):
    _, _, c_master, c_replicas = await setup_replication(
        df_factory,
        master_args={"proactor_threads": t_master},
        replicas=[{"proactor_threads": t} for t in t_replicas],
    )

    script = script_test_s1.format(flags=f"--!df flags={flags}" if flags else "")
    sha = await c_master.script_load(script)

    key_sets = [[f"{i}-{j}" for j in range(num_keys)] for i in range(num_par)]

    rsps = await asyncio.gather(
        *(c_master.evalsha(sha, len(keys), *keys, num_ops) for keys in key_sets)
    )
    assert rsps == ["OK"] * num_par

    await check_all_replicas_finished(c_replicas, c_master)

    for c_replica in c_replicas:
        for key_set in key_sets:
            for j, k in enumerate(key_set):
                l = await c_replica.lrange(k, 0, -1)
                assert l == [f"{j}"] * num_ops


SCRIPT_TEMPLATE = "return {}"


@dfly_args({"proactor_threads": 2})
async def test_script_transfer(df_factory):
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Load some scripts into master ahead
    scripts = []
    for i in range(0, 10):
        sha = await c_master.script_load(SCRIPT_TEMPLATE.format(i))
        scripts.append(sha)

    await start_replication(c_replica, master.port)

    # transfer in stable state
    for i in range(10, 20):
        sha = await c_master.script_load(SCRIPT_TEMPLATE.format(i))
        scripts.append(sha)

    await check_all_replicas_finished([c_replica], c_master)
    await c_replica.execute_command("REPLICAOF NO ONE")

    for i, sha in enumerate(scripts):
        assert await c_replica.evalsha(sha, 0) == i
    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


@dfly_args({"proactor_threads": 4})
async def test_role_command(df_factory, n_keys=20):
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    assert await c_master.execute_command("role") == master_role_reply([])
    await start_replication(c_replica, master.port)

    # It may take a bit more time to actually propagate the role change
    # See https://github.com/dragonflydb/dragonfly/pull/2111
    await asyncio.sleep(1)

    assert await c_master.execute_command("role") == master_role_reply(replica)
    assert await c_replica.execute_command("role") == replica_role_reply(master)

    # This tests that we react fast to socket shutdowns and don't hang on
    # things like the ACK or execution fibers.
    master.stop()
    await asyncio.sleep(0.1)
    assert await c_replica.execute_command("role") == replica_role_reply(master, state="connecting")

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


def parse_lag(replication_info: str):
    lags = re.findall("lag=([0-9]+)\r\n", replication_info)
    assert len(lags) == 1
    return int(lags[0])


async def assert_lag_condition(inst, client, condition):
    """
    Since lag is a bit random, and we want stable tests, we check
    10 times in quick succession and validate that the condition
    is satisfied at least once.
    We check both `INFO REPLICATION` redis protocol and the `/metrics`
    prometheus endpoint.
    """
    for _ in range(10):
        lag = await get_metric_value(inst, "dragonfly_connected_replica_lag_records")
        if condition(lag):
            break
        print("current prometheus lag =", lag)
        await asyncio.sleep(0.05)
    else:
        assert False, "Lag from prometheus metrics has never satisfied condition!"
    for _ in range(10):
        lag = parse_lag(await client.execute_command("info replication"))
        if condition(lag):
            break
        print("current lag =", lag)
        await asyncio.sleep(0.05)
    else:
        assert False, "Lag has never satisfied condition!"


@dfly_args({"proactor_threads": 2})
@pytest.mark.replication(replica_args={"replication_acks_interval": 100})
async def test_replication_info(replication, df_seeder_factory, n_keys=2000):
    master, [replica], c_master, [c_replica] = replication

    await assert_lag_condition(master, c_master, lambda lag: lag == 0)

    seeder = df_seeder_factory.create(port=master.port, keys=n_keys, dbcount=2)
    fill_task = asyncio.create_task(seeder.run(target_ops=3000))
    await assert_lag_condition(master, c_master, lambda lag: lag > 30)
    seeder.stop()

    await fill_task
    await wait_available_async(c_replica)
    await assert_lag_condition(master, c_master, lambda lag: lag == 0)

    # Replica should expose replication metrics
    replica_metrics = await replica.metrics()
    assert replica_metrics["dragonfly_master_link_status"].samples[0].value == 1
    assert replica_metrics["dragonfly_master_sync_in_progress"].samples[0].value == 0
    assert replica_metrics["dragonfly_master_last_io_seconds_ago"].samples[0].value >= 0
    assert "dragonfly_slave_repl_offset" in replica_metrics

    # Master should not expose replica-side metrics
    master_metrics = await master.metrics()
    assert "dragonfly_master_link_status" not in master_metrics
    assert "dragonfly_slave_repl_offset" not in master_metrics

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


"""
Test flushall command that's invoked while in full sync mode.
This can cause an issue because it will be executed on each shard independently.
More details in https://github.com/dragonflydb/dragonfly/issues/1231
"""


@pytest.mark.large
@pytest.mark.exclude_epoll
async def test_flushall_in_full_sync(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    # Fill master with test data
    seeder = DebugPopulateSeeder(key_target=100_000)
    await seeder.run(c_master)

    # Start replication and wait for full sync
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    async with async_timeout.timeout(3):
        await wait_for_replicas_state(c_replica, state="full_sync", timeout=0.05)

    syncid, _ = await c_replica.execute_command("DEBUG REPLICA OFFSET")

    # Issue FLUSHALL and record replica role at the same instant
    _, role = await asyncio.gather(c_master.execute_command("FLUSHALL"), c_replica.role())

    # Print warning if replication was too quick
    if role[3] != "full_sync":
        logging.error("!!! Full sync finished too fast. Adjust test parameters !!!")
        return

    # Run a few more commands on top
    post_seeder = SeederV2(key_target=100)
    await post_seeder.run(c_master, target_deviation=0.1)

    await check_all_replicas_finished([c_replica], c_master)

    # Check replica data consisten
    hash1, hash2 = await asyncio.gather(*(SeederV2.capture(c) for c in (c_master, c_replica)))
    assert hash1 == hash2

    # Make sure that a new sync ID is present, meaning replication restarted following FLUSHALL.
    new_syncid, _ = await c_replica.execute_command("DEBUG REPLICA OFFSET")
    assert new_syncid != syncid


"""
Test read-only scripts work with replication. EVAL_RO and the 'no-writes' flags are currently not supported.
"""

READONLY_SCRIPT = """
redis.call('GET', 'A')
redis.call('EXISTS', 'B')
return redis.call('GET', 'WORKS')
"""

WRITE_SCRIPT = """
redis.call('SET', 'A', 'ErrroR')
"""


async def test_readonly_script(df_factory):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.set("WORKS", "YES")

    await start_replication(c_replica, master.port)

    await c_replica.eval(READONLY_SCRIPT, 3, "A", "B", "WORKS") == "YES"

    with pytest.raises(aioredis.ResponseError):
        await c_replica.eval(WRITE_SCRIPT, 1, "A")


# @pytest.mark.large
@pytest.mark.replication(master_args={"proactor_threads": 4}, replica_args={"proactor_threads": 4})
async def test_client_pause_with_replica(replication, df_seeder_factory):
    master, [replica], c_master, [c_replica] = replication

    seeder = df_seeder_factory.create(port=master.port)

    fill_task = asyncio.create_task(seeder.run())

    # Give the seeder a bit of time.
    await asyncio.sleep(1)
    # block the seeder for 4 seconds
    await c_master.execute_command("client pause 4000 write")
    stats = await c_master.info("CommandStats")
    await asyncio.sleep(0.5)
    stats_after_sleep = await c_master.info("CommandStats")
    # Check no commands are executed except info and replconf called from replica
    for cmd, cmd_stats in stats_after_sleep.items():
        if cmd in ["cmdstat_info", "cmdstat_replconf", "cmdstat_multi"]:
            continue
        assert stats[cmd] == cmd_stats, cmd

    await asyncio.sleep(6)
    seeder.stop()
    await fill_task
    stats_after_pause_finish = await c_master.info("CommandStats")
    more_exeuted = False
    for cmd, cmd_stats in stats_after_pause_finish.items():
        if "cmdstat_info" != cmd and "cmdstat_replconf" != cmd_stats and stats[cmd] != cmd_stats:
            more_exeuted = True
    assert more_exeuted

    capture = await seeder.capture(port=master.port)
    assert await seeder.compare(capture, port=replica.port)


async def test_replica_of_self(async_client):
    port = async_client.connection_pool.connection_kwargs["port"]
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command(f"replicaof localhost {port}")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command(f"replicaof 127.0.0.1 {port}")


@dfly_args({"replicaof_no_one_start_journal": True, "proactor_threads": 2})
async def test_repl_offset(df_factory):
    master = df_factory.create()
    replica1 = df_factory.create()
    replica2 = df_factory.create()
    replica3 = df_factory.create()

    df_factory.start_all([master, replica1, replica2, replica3])
    c_master = master.client()
    c_replica1 = replica1.client()
    c_replica2 = replica2.client()
    c_replica3 = replica3.client()

    seeder = DebugPopulateSeeder(key_target=50)
    await seeder.run(c_master)

    await c_replica1.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica1)
    await c_replica2.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica2)
    await c_replica3.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica3)

    seeder = SeederV2(key_target=50)
    await seeder.run(c_master, target_deviation=0.01)

    # Wait for all journal changes propagate to replicas
    await check_all_replicas_finished([c_replica1, c_replica2, c_replica3], c_master)

    # Promote first replica to master
    await c_replica1.execute_command("REPLTAKEOVER 5")

    # issue 4183
    async def with_timeout_link_down(client):
        async with async_timeout.timeout(2):
            while True:
                info = await client.info("replication")
                if info["master_link_status"] == "down":
                    assert info["slave_repl_offset"] > 0
                    break
                await asyncio.sleep(0.1)

    await with_timeout_link_down(c_replica2)
    assert "OK" == await c_replica2.execute_command("replicaof no one")

    # Partial sync here
    await c_replica3.execute_command(f"REPLICAOF localhost {replica2.port}")
    # Full sync here
    await c_replica1.execute_command(f"REPLICAOF localhost {replica2.port}")

    await check_all_replicas_finished([c_replica1, c_replica3], c_replica2)

    info = await c_replica3.info("replication")
    # 1 repl flow per proactor.
    proactors = 2
    # if `replicaof no one` on `c_replica2` does not preserve the journal offsets,
    # then the assertion below shall fail. In that case, replicas perform a full sync first
    # and as there are no journal changes the slave offsets are 2 (1 per shard).
    assert info["slave_repl_offset"] > proactors
    assert info["psync_successes"] == 1

    await c_replica1.execute_command("REPLTAKEOVER 5")
    await with_timeout_link_down(c_replica3)


async def test_client_list_replication_types(df_factory: DflyInstanceFactory):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=1)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    assert parse_client_list(await c_replica.execute_command("CLIENT LIST TYPE master")) == []

    await c_replica.execute_command("REPLICAOF", "localhost", str(master.port))
    await wait_available_async(c_replica)

    replicas_on_master = parse_client_list(
        await c_master.execute_command("CLIENT LIST TYPE replica")
    )
    assert len(replicas_on_master) >= 1
    normal_on_master = parse_client_list(await c_master.execute_command("CLIENT LIST TYPE normal"))
    normal_ids = {c["id"] for c in normal_on_master}
    assert str(await c_master.execute_command("CLIENT ID")) in normal_ids
    assert not ({c["id"] for c in replicas_on_master} & normal_ids)

    masters_on_replica = parse_client_list(
        await c_replica.execute_command("CLIENT LIST TYPE master")
    )
    assert len(masters_on_replica) == 1
    entry = masters_on_replica[0]
    assert entry["addr"].endswith(f":{master.port}")
    assert entry["laddr"] != "-"
    assert int(entry["fd"]) >= 0
    assert entry["flags"] == "M"
    assert int(entry["age"]) >= 0
    assert int(entry["idle"]) >= 0
    assert entry["repl-phase"] == "stable_sync"
    assert "phase" not in entry  # master link reports replication state, not lifecycle phase

    all_on_replica = parse_client_list(await c_replica.execute_command("CLIENT LIST"))
    assert any(c.get("flags") == "M" for c in all_on_replica)
    all_ids = [c["id"] for c in all_on_replica]
    assert len(all_ids) == len(set(all_ids)), f"duplicate ids: {all_ids}"

    by_id = parse_client_list(await c_replica.execute_command("CLIENT LIST ID", entry["id"]))
    assert [c["id"] for c in by_id] == [entry["id"]]
    assert by_id[0]["flags"] == "M"

    # The master-link id is stable across successive CLIENT LIST calls.
    again = parse_client_list(await c_replica.execute_command("CLIENT LIST TYPE master"))
    assert again[0]["id"] == entry["id"]

    # CLIENT KILL ID against the master-link id must be rejected explicitly.
    with pytest.raises(aioredis.ResponseError) as exc:
        await c_replica.execute_command("CLIENT", "KILL", "ID", entry["id"])
    assert "REPLICAOF NO ONE" in str(exc.value)

    await c_replica.execute_command("REPLICAOF", "NO", "ONE")

    @assert_eventually(timeout=5)
    async def link_gone():
        assert parse_client_list(await c_replica.execute_command("CLIENT LIST TYPE master")) == []

    await link_gone()


@dfly_args({"proactor_threads": 2})
async def test_wait_with_replica(df_factory: DflyInstanceFactory):
    master = df_factory.create()
    replica = df_factory.create()
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.set("k", "v")
    # No replicas yet: should return 0 without blocking for the full timeout.
    start = time.time()
    result = await c_master.execute_command("WAIT", 1, 200)
    elapsed_ms = (time.time() - start) * 1000
    assert result == 0
    # Should return quickly (well under 200ms) because there is nothing to wait for.
    assert elapsed_ms < 150, f"WAIT took {elapsed_ms:.0f}ms with no replicas"

    await start_replication(c_replica, master.port)
    await wait_for_replicas_state(c_replica)

    # Write some keys and wait for 1 replica to acknowledge.
    await c_master.mset({f"key:{i}": f"val:{i}" for i in range(100)})
    result = await c_master.execute_command("WAIT", 1, 5000)
    assert result == 1, f"Expected 1 replica to ack, got {result}"

    # WAIT 0 must still report the actual acked count (a non-blocking probe), not force 0 just
    # because the requested threshold was already trivially satisfied.
    result = await c_master.execute_command("WAIT", 0, 100)
    assert result == 1, f"WAIT 0 should report the real acked count, got {result}"

    # Requesting more replicas than are tracked can never be satisfied (the replica snapshot is
    # fixed at call time), so this should return the actual count quickly instead of burning the
    # full timeout.
    start = time.time()
    result = await c_master.execute_command("WAIT", 2, 5000)
    elapsed_ms = (time.time() - start) * 1000
    assert result == 1, f"Expected 1 replica even though 2 were requested, got {result}"
    assert (
        elapsed_ms < 1000
    ), f"WAIT should give up early once unsatisfiable, took {elapsed_ms:.0f}ms"

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


@dfly_args({"proactor_threads": 2})
@pytest.mark.replication
async def test_wait_semantics(replication):
    """
    Exercises WAIT edge cases against real Valkey semantics:
    1. WAIT on a replica instance is rejected with an error.
    2. WAIT inside MULTI/EXEC returns immediately instead of blocking.
    3. A long/blocked WAIT on the master doesn't stall a concurrent REPLTAKEOVER.
    """
    master, [replica], c_master, [c_replica] = replication

    await wait_for_replicas_state(c_replica)

    # 1. WAIT is rejected on a replica instance.
    with pytest.raises(redis.exceptions.ResponseError, match="WAIT cannot be used"):
        await c_replica.execute_command("WAIT", 0, 0)

    # 2. Inside MULTI/EXEC, WAIT must not block even if it can never be satisfied.
    pipe = c_master.pipeline(transaction=True)
    pipe.execute_command("SET", "multi_wait_key", "v")
    pipe.execute_command("WAIT", 5, 5000)  # only 1 replica exists; would block ~5s outside MULTI.
    start = time.time()
    set_result, wait_result = await pipe.execute()
    elapsed_ms = (time.time() - start) * 1000
    assert elapsed_ms < 1000, f"WAIT blocked inside MULTI/EXEC for {elapsed_ms:.0f}ms"
    assert isinstance(wait_result, int) and wait_result <= 1

    # 3. A WAIT that can never be satisfied (2 replicas requested, only 1 exists) must not
    # block a concurrent REPLTAKEOVER: the poll loop should notice the global state change
    # and return well before its own (much larger) timeout.
    c_wait = master.client()

    async def long_wait():
        start = time.time()
        result = await c_wait.execute_command("WAIT", 2, 60000)
        elapsed = time.time() - start
        assert elapsed < 10, f"WAIT blocked the takeover for {elapsed:.1f}s"
        return result

    async def delayed_takeover():
        await asyncio.sleep(0.5)
        await c_replica.execute_command("REPLTAKEOVER 5")

    await asyncio.gather(long_wait(), delayed_takeover())
    assert await c_replica.execute_command("role") == master_role_reply([])

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()
    await c_wait.connection_pool.disconnect()
