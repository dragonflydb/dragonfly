import random

from itertools import chain, repeat
import re
import pytest
import asyncio
import async_timeout
import platform
import pymemcache
import logging
import tarfile
import urllib.request
import shutil
from redis import asyncio as aioredis
from .utility import *
from .instance import DflyInstanceFactory, DflyInstance
from .seeder import Seeder as SeederV2
from . import dfly_args
from .proxy import Proxy
from .seeder import StaticSeeder

ADMIN_PORT = 1211

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

M_OPT = [pytest.mark.opt_only]
M_SLOW = [pytest.mark.slow]
M_STRESS = [pytest.mark.slow, pytest.mark.opt_only]
M_NOT_EPOLL = [pytest.mark.exclude_epoll]


async def wait_for_replicas_state(*clients, state="online", node_role="slave", timeout=0.05):
    """Wait until all clients (replicas) reach passed state"""
    while len(clients) > 0:
        await asyncio.sleep(timeout)
        roles = await asyncio.gather(*(c.role() for c in clients))
        clients = [c for c, role in zip(clients, roles) if role[0] != node_role or role[3] != state]


"""
Test full replication pipeline. Test full sync with streaming changes and stable state streaming.
"""


@pytest.mark.parametrize(
    "t_master, t_replicas, seeder_config, stream_target",
    [
        # Quick general test that replication is working
        (1, 3 * [1], dict(key_target=1_000), 500),
        # A lot of huge values
        (2, 2 * [1], dict(key_target=1_000, huge_value_target=30), 500),
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
        pytest.param(8, [8, 8], dict(key_target=1_000_000, units=16), 50_000, marks=M_STRESS),
    ],
)
@pytest.mark.parametrize("mode", [({}), ({"cache_mode": "true"})])
async def test_replication_all(
    df_factory: DflyInstanceFactory,
    t_master,
    t_replicas,
    seeder_config,
    stream_target,
    mode,
):
    args = {}
    if mode:
        args["cache_mode"] = "true"
        args["maxmemory"] = str(t_master * 256) + "mb"

    master = df_factory.create(admin_port=ADMIN_PORT, proactor_threads=t_master, **args)
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
    seeder = SeederV2(**seeder_config)
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
        await check_all_replicas_finished(c_replicas, c_master)
        hashes = await asyncio.gather(*(SeederV2.capture(c) for c in [c_master] + c_replicas))
        assert len(set(hashes)) == 1

    await check()
    # Stream more data in stable state
    await seeder.run(c_master, target_ops=stream_target)

    # Check data after stable state stream
    await check()

    info = await c_master.info()
    preemptions = info["big_value_preemptions"]
    total_buckets = info["num_buckets"]
    compressed_blobs = info["compressed_blobs"]
    logging.debug(
        f"Compressed blobs {compressed_blobs} .Buckets {total_buckets}. Preemptions {preemptions}"
    )

    assert preemptions >= seeder.huge_value_target * 0.5
    assert compressed_blobs > 0
    # Because data size could be 10k and for that case there will be almost a preemption
    # per bucket.
    if seeder.data_size < 1000:
        # We care that we preempt less times than the total buckets such that we can be
        # sure that we test both flows (with and without preemptions). Preemptions on 30%
        # of buckets seems like a big number but that depends on a few parameters like
        # the size of the hug value and the serialization max chunk size. For the test cases here,
        # it's usually close to 10% but there are some that are close to 30.
        assert preemptions <= (total_buckets * 0.3)


async def check_replica_finished_exec(c_replica: aioredis.Redis, m_offset):
    role = await c_replica.role()
    if role[0] != "slave" or role[3] != "online":
        return False
    syncid, r_offset = await c_replica.execute_command("DEBUG REPLICA OFFSET")

    logging.debug(f"  offset {syncid} {r_offset} {m_offset}")
    return r_offset == m_offset


async def check_all_replicas_finished(c_replicas, c_master, timeout=20):
    logging.debug("Waiting for replicas to finish")

    waiting_for = list(c_replicas)
    start = time.time()
    while (time.time() - start) < timeout:
        if not waiting_for:
            return
        await asyncio.sleep(0.2)
        m_offset = await c_master.execute_command("DFLY REPLICAOFFSET")
        finished_list = await asyncio.gather(
            *(check_replica_finished_exec(c, m_offset) for c in waiting_for)
        )

        # Remove clients that finished from waiting list
        waiting_for = [c for (c, finished) in zip(waiting_for, finished_list) if not finished]

    first_r: aioredis.Redis = waiting_for[0]
    logging.error("Replica not finished, role %s", await first_r.role())
    raise RuntimeError("Not all replicas finished in time!")


"""
Test disconnecting replicas during different phases while constantly streaming changes to master.

This test is targeted at the master cancellation mechanism that should qickly stop operations for a
disconnected replica.

Three types are tested:
1. Replicas crashing during full sync state
2. Replicas crashing during stable sync state
3. Replicas disconnecting normally with REPLICAOF NO ONE during stable state
"""

# 1. Number of master threads
# 2. Number of threads for each replica that crashes during full sync
# 3. Number of threads for each replica that crashes during stable sync
# 4. Number of threads for each replica that disconnects normally
# 5. Number of distinct keys that are constantly streamed
disconnect_cases = [
    # balanced
    (8, [4, 4], [4, 4], [4], 4_000),
    (4, [2] * 4, [2] * 4, [2, 2], 2_000),
    # full sync heavy
    (8, [4] * 4, [], [], 4_000),
    # stable state heavy
    (8, [], [4] * 4, [], 4_000),
    # disconnect only
    (8, [], [], [4] * 4, 4_000),
]


@pytest.mark.parametrize("t_master, t_crash_fs, t_crash_ss, t_disonnect, n_keys", disconnect_cases)
async def test_disconnect_replica(
    df_factory: DflyInstanceFactory,
    df_seeder_factory,
    t_master,
    t_crash_fs,
    t_crash_ss,
    t_disonnect,
    n_keys,
):
    master = df_factory.create(
        proactor_threads=t_master, vmodule="replica=2,dflycmd=2,server_family=2"
    )
    replicas = [
        (
            df_factory.create(proactor_threads=t, vmodule="replica=2,dflycmd=2,server_family=2"),
            crash_fs,
        )
        for i, (t, crash_fs) in enumerate(
            chain(
                zip(t_crash_fs, repeat(DISCONNECT_CRASH_FULL_SYNC)),
                zip(t_crash_ss, repeat(DISCONNECT_CRASH_STABLE_SYNC)),
                zip(t_disonnect, repeat(DISCONNECT_NORMAL_STABLE_SYNC)),
            )
        )
    ]

    logging.debug("Start master")
    master.start()
    c_master = master.client(single_connection_client=True)

    logging.debug("Start replicas and create clients")
    df_factory.start_all([replica for replica, _ in replicas])

    c_replicas = [(replica, replica.client(), crash_type) for replica, crash_type in replicas]

    def replicas_of_type(tfunc):
        return [args for args in c_replicas if tfunc(args[2])]

    logging.debug("Start data fill loop")
    seeder = df_seeder_factory.create(port=master.port, keys=n_keys, dbcount=2)
    fill_task = asyncio.create_task(seeder.run())

    logging.debug("Run full sync")

    async def full_sync(replica: DflyInstance, c_replica, crash_type):
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
        if crash_type == 0:
            await asyncio.sleep(random.random() / 100 + 0.01)
            await c_replica.aclose()
            replica.stop(kill=True)
        else:
            await wait_available_async(c_replica)

    await asyncio.gather(*(full_sync(*args) for args in c_replicas))

    # Wait for master to stream a bit more
    await asyncio.sleep(0.1)

    # Check master survived full sync crashes
    assert await c_master.ping()

    # Check phase-2 replicas survived
    for _, c_replica, _ in replicas_of_type(lambda t: t > 0):
        assert await c_replica.ping()

    logging.debug("Run stable state crashes")

    async def stable_sync(replica, c_replica, crash_type):
        await asyncio.sleep(random.random() / 100)
        await c_replica.aclose()
        replica.stop(kill=True)

    await asyncio.gather(*(stable_sync(*args) for args in replicas_of_type(lambda t: t == 1)))

    # Check master survived all crashes
    assert await c_master.ping()

    # Check phase 3 replica survived
    for _, c_replica, _ in replicas_of_type(lambda t: t > 1):
        assert await c_replica.ping()

    logging.debug("Check master survived all crashes")
    assert await c_master.ping()

    # Check disconnects
    async def disconnect(replica, c_replica, crash_type):
        await asyncio.sleep(random.random() / 100)
        await c_replica.execute_command("REPLICAOF NO ONE")

    logging.debug("disconnect replicas")
    await asyncio.gather(*(disconnect(*args) for args in replicas_of_type(lambda t: t == 2)))

    await asyncio.sleep(0.5)

    logging.debug("Check phase 3 replica survived")
    for replica, c_replica, _ in replicas_of_type(lambda t: t == 2):
        assert await c_replica.ping()
        await c_replica.aclose()

    logging.debug("Stop streaming")
    seeder.stop()
    await fill_task

    logging.debug("Check master survived all disconnects")
    assert await c_master.ping()


"""
Test stopping master during different phases.

This test is targeted at the replica cancellation mechanism that should quickly abort a failed operation
and revert to connection retry state.

Three types are tested:
1. Master crashing during full sync state
2. Master crashing in a random state.
3. Master crashing during stable sync state

"""

# 1. Number of master threads
# 2. Number of threads for each replica
# 3. Number of times a random crash happens
# 4. Number of keys transferred (the more, the higher the propability to not miss full sync)
master_crash_cases = [
    (6, [6], 3, 2_000),
    (4, [4, 4, 4], 3, 2_000),
]


@pytest.mark.slow
@pytest.mark.parametrize("t_master, t_replicas, n_random_crashes, n_keys", master_crash_cases)
async def test_disconnect_master(
    df_factory, df_seeder_factory, t_master, t_replicas, n_random_crashes, n_keys
):
    master = df_factory.create(port=1111, proactor_threads=t_master)
    replicas = [df_factory.create(proactor_threads=t) for i, t in enumerate(t_replicas)]

    df_factory.start_all(replicas)
    c_replicas = [replica.client() for replica in replicas]

    seeder = df_seeder_factory.create(port=master.port, keys=n_keys, dbcount=2)

    async def crash_master_fs():
        await asyncio.sleep(random.random() / 10)
        master.stop(kill=True)

    async def start_master():
        await asyncio.sleep(0.2)
        master.start()
        async with master.client() as c_master:
            assert await c_master.ping()
            seeder.reset()
            await seeder.run(target_deviation=0.1)

    await start_master()

    # Crash master during full sync, but with all passing initial connection phase
    await asyncio.gather(
        *(
            c_replica.execute_command("REPLICAOF localhost " + str(master.port))
            for c_replica in c_replicas
        )
    )
    await crash_master_fs()

    await asyncio.sleep(1 + len(replicas) * 0.5)

    for _ in range(n_random_crashes):
        await start_master()
        await asyncio.sleep(random.random() + len(replicas) * random.random() / 10)
        # Crash master in some random state for each replica
        master.stop(kill=True)

    await start_master()
    await asyncio.sleep(1 + len(replicas) * 0.5)  # Replicas check every 500ms.
    capture = await seeder.capture()
    for replica, c_replica in zip(replicas, c_replicas):
        await wait_available_async(c_replica)
        assert await seeder.compare(capture, port=replica.port)

    # Crash master during stable state
    master.stop(kill=True)

    await start_master()
    await asyncio.sleep(1 + len(replicas) * 0.5)
    capture = await seeder.capture()
    for c_replica in c_replicas:
        await wait_available_async(c_replica)
        assert await seeder.compare(capture, port=replica.port)


"""
Test re-connecting replica to different masters.
"""

rotating_master_cases = [(4, [4, 4, 4, 4], dict(keys=2_000, dbcount=4))]


@pytest.mark.slow
@pytest.mark.parametrize("t_replica, t_masters, seeder_config", rotating_master_cases)
async def test_rotating_masters(df_factory, df_seeder_factory, t_replica, t_masters, seeder_config):
    replica = df_factory.create(proactor_threads=t_replica)
    masters = [df_factory.create(proactor_threads=t) for i, t in enumerate(t_masters)]
    df_factory.start_all([replica] + masters)

    seeders = [df_seeder_factory.create(port=m.port, **seeder_config) for m in masters]

    c_replica = replica.client()

    await asyncio.gather(*(seeder.run(target_deviation=0.1) for seeder in seeders))

    fill_seeder = None
    fill_task = None

    for master, seeder in zip(masters, seeders):
        if fill_task is not None:
            fill_seeder.stop()
            fill_task.cancel()

        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        await wait_available_async(c_replica)

        capture = await seeder.capture()
        assert await seeder.compare(capture, port=replica.port)

        fill_task = asyncio.create_task(seeder.run())
        fill_seeder = seeder

    if fill_task is not None:
        fill_seeder.stop()
        fill_task.cancel()


@pytest.mark.slow
async def test_cancel_replication_immediately(df_factory, df_seeder_factory: DflySeederFactory):
    """
    Issue 100 replication commands. This checks that the replication state
    machine can handle cancellation well.
    We assert that at least one command was cancelled.
    After we finish the 'fuzzing' part, replicate the first master and check that
    all the data is correct.
    """
    COMMANDS_TO_ISSUE = 100

    replica = df_factory.create()
    master = df_factory.create()
    df_factory.start_all([replica, master])

    seeder = df_seeder_factory.create(port=master.port)
    c_replica = replica.client(socket_timeout=80)

    await seeder.run(target_deviation=0.1)

    async def ping_status():
        while True:
            await c_replica.info()
            await asyncio.sleep(0.05)

    async def replicate():
        try:
            await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
            return True
        except redis.exceptions.ResponseError as e:
            assert e.args[0] == "replication cancelled"
            return False

    ping_job = asyncio.create_task(ping_status())
    replication_commands = [asyncio.create_task(replicate()) for _ in range(COMMANDS_TO_ISSUE)]

    num_successes = 0
    for result in asyncio.as_completed(replication_commands, timeout=80):
        num_successes += await result

    logging.info(f"succeses: {num_successes}")
    assert COMMANDS_TO_ISSUE > num_successes, "At least one REPLICAOF must be cancelled"

    await wait_available_async(c_replica)
    capture = await seeder.capture()
    logging.info(f"number of items captured {len(capture)}")
    assert await seeder.compare(capture, replica.port)

    ping_job.cancel()


"""
Test flushall command. Set data to master send flashall and set more data.
Check replica keys at the end.
"""


async def test_flushall(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=2)

    master.start()
    replica.start()

    # Connect replica to master
    c_replica = replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    n_keys = 1000

    def gen_test_data(start, end):
        for i in range(start, end):
            yield f"key-{i}", f"value-{i}"

    c_master = master.client()
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
async def test_rewrites(df_factory):
    CLOSE_TIMESTAMP = int(time.time()) + 100
    CLOSE_TIMESTAMP_MS = CLOSE_TIMESTAMP * 1000

    master = df_factory.create()
    replica = df_factory.create()

    master.start()
    replica.start()

    # Connect clients, connect replica to master
    c_master = master.client()
    c_replica = replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

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
        await check(f"SET k v EX 100 NX GET", r"SET k v PXAT (.*?)")
        await check_expire("k")
        await check(f"SET k v PX 50000", r"SET k v PXAT (.*?)")
        await check_expire("k")
        # Exact expiry is skewed
        await check(f"SET k v XX EXAT {CLOSE_TIMESTAMP}", rf"SET k v PXAT (.*?)")
        await check_expire("k")

        # Check SET + KEEPTTL doesn't loose KEEPTTL
        await check(f"SET k v KEEPTTL", r"SET k v KEEPTTL")

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

        await c_master.set("k1", "1000")
        await c_master.set("k2", "1100")
        await skip_cmd()
        await skip_cmd()
        # Check BITOP turns into SET
        await check("BITOP OR kdest k1 k2", r"SET kdest 1100")
        # See gh issue #3528
        await c_master.execute_command(f"HSET foo bar val")
        await skip_cmd()
        await check("BITOP NOT foo tmp", r"DEL foo")
        await c_master.execute_command(f"HSET foo bar val")
        await skip_cmd()
        await c_master.set("k3", "-")
        await skip_cmd()
        await check("BITOP NOT foo k3", r"SET foo \\xd2")

        # Check there is no rewrite for LMOVE on single shard
        await c_master.lpush("list", "v1", "v2", "v3", "v4")
        await skip_cmd()
        await check("LMOVE list list LEFT RIGHT", r"LMOVE list list LEFT RIGHT")

        # Check there is no rewrite for RPOPLPUSH on single shard
        await check("RPOPLPUSH list list", r"RPOPLPUSH list list")
        # Check BRPOPLPUSH on single shard turns into LMOVE
        await check("BRPOPLPUSH list list 0", r"LMOVE list list RIGHT LEFT")
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


"""
Test automatic replication of expiry.
"""


@dfly_args({"proactor_threads": 4})
async def test_expiry(df_factory: DflyInstanceFactory, n_keys=1000):
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Connect replica to master
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

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


@dfly_args({"proactor_threads": 4})
async def test_simple_scripts(df_factory: DflyInstanceFactory):
    master = df_factory.create()
    replicas = [df_factory.create() for _ in range(2)]
    df_factory.start_all([master] + replicas)

    c_replicas = [replica.client() for replica in replicas]
    c_master = master.client()

    # Connect replicas and wait for sync to finish
    for c_replica in c_replicas:
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await check_all_replicas_finished([c_replica], c_master)

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
    await check_all_replicas_finished([c_replica], c_master)

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
    master = df_factory.create(proactor_threads=t_master)
    replicas = [df_factory.create(proactor_threads=t) for i, t in enumerate(t_replicas)]

    df_factory.start_all([master] + replicas)

    c_master = master.client()
    c_replicas = [replica.client() for replica in replicas]
    for c_replica in c_replicas:
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        await wait_available_async(c_replica)

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


@dfly_args({"proactor_threads": 4})
async def test_auth_master(df_factory, n_keys=20):
    masterpass = "requirepass"
    replicapass = "replicapass"
    master = df_factory.create(requirepass=masterpass)
    replica = df_factory.create(logtostdout=True, masterauth=masterpass, requirepass=replicapass)

    df_factory.start_all([master, replica])

    c_master = master.client(password=masterpass)
    c_replica = replica.client(password=replicapass)

    # Connect replica to master
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    # Set keys
    pipe = c_master.pipeline(transaction=False)
    batch_fill_data(pipe, gen_test_data(n_keys))
    await pipe.execute()

    # Check replica finished executing the replicated commands
    await check_all_replicas_finished([c_replica], c_master)
    # Check keys are on replica
    res = await c_replica.mget(k for k, _ in gen_test_data(n_keys))
    assert all(v is not None for v in res)
    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


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

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

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

    assert await c_master.execute_command("role") == ["master", []]
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    # It may take a bit more time to actually propagate the role change
    # See https://github.com/dragonflydb/dragonfly/pull/2111
    await asyncio.sleep(1)

    assert await c_master.execute_command("role") == [
        "master",
        [["127.0.0.1", str(replica.port), "online"]],
    ]
    assert await c_replica.execute_command("role") == [
        "slave",
        "localhost",
        str(master.port),
        "online",
    ]

    # This tests that we react fast to socket shutdowns and don't hang on
    # things like the ACK or execution fibers.
    master.stop()
    await asyncio.sleep(0.1)
    assert await c_replica.execute_command("role") == [
        "slave",
        "localhost",
        str(master.port),
        "connecting",
    ]

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


def parse_lag(replication_info: str):
    lags = re.findall("lag=([0-9]+)\r\n", replication_info)
    assert len(lags) == 1
    return int(lags[0])


async def get_metric_value(inst, metric_name, sample_index=0):
    return (await inst.metrics())[metric_name].samples[sample_index].value


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


async def get_replica_reconnects_count(replica_inst):
    return await get_metric_value(replica_inst, "dragonfly_replica_reconnect_count")


async def assert_replica_reconnections(replica_inst, initial_reconnects_count):
    """
    Asserts that the replica has attempted to reconnect at least once.
    """
    reconnects_count = await get_replica_reconnects_count(replica_inst)
    if reconnects_count > initial_reconnects_count:
        return

    assert (
        False
    ), f"Expected reconnect count to increase by at least 1, but it did not. Initial dragonfly_replica_reconnect_count: {initial_reconnects_count}, current count: {reconnects_count}"


@dfly_args({"proactor_threads": 2})
async def test_replication_info(df_factory: DflyInstanceFactory, df_seeder_factory, n_keys=2000):
    master = df_factory.create()
    replica = df_factory.create(replication_acks_interval=100)
    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)
    await assert_lag_condition(master, c_master, lambda lag: lag == 0)

    seeder = df_seeder_factory.create(port=master.port, keys=n_keys, dbcount=2)
    fill_task = asyncio.create_task(seeder.run(target_ops=3000))
    await assert_lag_condition(master, c_master, lambda lag: lag > 30)
    seeder.stop()

    await fill_task
    await wait_available_async(c_replica)
    await assert_lag_condition(master, c_master, lambda lag: lag == 0)

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


"""
Test flushall command that's invoked while in full sync mode.
This can cause an issue because it will be executed on each shard independently.
More details in https://github.com/dragonflydb/dragonfly/issues/1231
"""


@pytest.mark.slow
async def test_flushall_in_full_sync(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    # Fill master with test data
    seeder = StaticSeeder(key_target=100_000)
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

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    await c_replica.eval(READONLY_SCRIPT, 3, "A", "B", "WORKS") == "YES"

    try:
        await c_replica.eval(WRITE_SCRIPT, 1, "A")
        assert False
    except aioredis.ResponseError as roe:
        assert "READONLY " in str(roe)


take_over_cases = [
    [2, 2],
    [2, 4],
    [4, 2],
    [8, 8],
]


@pytest.mark.parametrize("master_threads, replica_threads", take_over_cases)
async def test_take_over_counters(df_factory, master_threads, replica_threads):
    master = df_factory.create(proactor_threads=master_threads)
    replica1 = df_factory.create(proactor_threads=replica_threads)
    replica2 = df_factory.create(proactor_threads=replica_threads)
    replica3 = df_factory.create(proactor_threads=replica_threads)
    df_factory.start_all([master, replica1, replica2, replica3])
    c_master = master.client()
    c1 = replica1.client()
    c_blocking = master.client()
    c2 = replica2.client()
    c3 = replica3.client()

    await c1.execute_command(f"REPLICAOF localhost {master.port}")
    await c2.execute_command(f"REPLICAOF localhost {master.port}")
    await c3.execute_command(f"REPLICAOF localhost {master.port}")

    await wait_available_async(c1)

    async def counter(key):
        value = 0
        await c_master.execute_command(f"SET {key} 0")
        start = time.time()
        while time.time() - start < 20:
            try:
                value = await c_master.execute_command(f"INCR {key}")
            except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError) as e:
                break
        else:
            assert False, "The incrementing loop should be exited with a connection error"
        return key, value

    async def block_during_takeover():
        "Add a blocking command during takeover to make sure it doesn't block it."
        start = time.time()
        # The command should just be canceled
        assert await c_blocking.execute_command("BLPOP BLOCKING_KEY1 BLOCKING_KEY2 100") is None
        # And it should happen in reasonable amount of time.
        assert time.time() - start < 10

    async def delayed_takeover():
        await asyncio.sleep(0.3)
        await c1.execute_command(f"REPLTAKEOVER 5")

    _, _, *results = await asyncio.gather(
        delayed_takeover(), block_during_takeover(), *[counter(f"key{i}") for i in range(16)]
    )
    assert await c1.execute_command("role") == ["master", []]

    for key, client_value in results:
        replicated_value = await c1.get(key)
        assert client_value == int(replicated_value)


@pytest.mark.parametrize("master_threads, replica_threads", take_over_cases)
async def test_take_over_seeder(
    request, df_factory, df_seeder_factory, master_threads, replica_threads
):
    master = df_factory.create(
        proactor_threads=master_threads, dbfilename=f"dump_{tmp_file_name()}", admin_port=ADMIN_PORT
    )
    replica = df_factory.create(proactor_threads=replica_threads)
    df_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(port=master.port, keys=1000, dbcount=5, stop_on_failure=False)

    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.admin_port}")
    await wait_available_async(c_replica)

    fill_task = asyncio.create_task(seeder.run())

    stop_info = False

    async def info_replication():
        my_client = replica.client()
        while not stop_info:
            await my_client.info("replication")
            await asyncio.sleep(0.5)

    info_task = asyncio.create_task(info_replication())

    # Give the seeder a bit of time.
    await asyncio.sleep(3)
    logging.debug("running repltakover")
    await c_replica.execute_command(f"REPLTAKEOVER 10 SAVE")
    logging.debug("after running repltakover")
    seeder.stop()
    await fill_task

    assert await c_replica.execute_command("role") == ["master", []]
    stop_info = True
    await info_task

    @assert_eventually
    async def assert_master_exists():
        assert master.proc.poll() == 0, "Master process did not exit correctly."

    await assert_master_exists()

    master.start()
    c_master = master.client()
    await wait_available_async(c_master)

    capture = await seeder.capture(port=master.port)
    assert await seeder.compare(capture, port=replica.port)


@pytest.mark.parametrize("master_threads, replica_threads", [[4, 4]])
async def test_take_over_read_commands(df_factory, master_threads, replica_threads):
    master = df_factory.create(proactor_threads=master_threads)
    replica = df_factory.create(proactor_threads=replica_threads)
    df_factory.start_all([master, replica])

    c_master = master.client()
    await c_master.execute_command("SET foo bar")

    c_replica = replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    async def prompt():
        client = replica.client()
        for i in range(50):
            # TODO remove try block when we no longer shut down master after take over
            try:
                res = await c_master.execute_command("GET foo")
                assert res == "bar"
                res = await c_master.execute_command("CONFIG SET aclfile myfile")
                assert res == "OK"
            except:
                pass
            res = await client.execute_command("GET foo")
            assert res == "bar"

    promt_task = asyncio.create_task(prompt())
    await c_replica.execute_command(f"REPLTAKEOVER 5")

    assert await c_replica.execute_command("role") == ["master", []]
    await promt_task


async def test_take_over_timeout(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)
    df_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(port=master.port, keys=1000, dbcount=5, stop_on_failure=False)

    c_master = master.client()
    c_replica = replica.client()

    logging.debug(f"PORTS ARE:  {master.port} {replica.port}")

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    fill_task = asyncio.create_task(seeder.run(target_ops=3000))

    # Give the seeder a bit of time.
    await asyncio.sleep(1)
    try:
        await c_replica.execute_command(f"REPLTAKEOVER 0")
    except redis.exceptions.ResponseError as e:
        assert str(e) == "Couldn't execute takeover"
    else:
        assert False, "Takeover should not succeed."
    seeder.stop()
    await fill_task

    assert await c_master.execute_command("role") == [
        "master",
        [["127.0.0.1", str(replica.port), "online"]],
    ]
    assert await c_replica.execute_command("role") == [
        "slave",
        "localhost",
        str(master.port),
        "online",
    ]


# 1. Number of master threads
# 2. Number of threads for each replica
replication_cases = [(8, 8)]


@pytest.mark.parametrize("t_master, t_replica", replication_cases)
async def test_no_tls_on_admin_port(
    df_factory: DflyInstanceFactory,
    df_seeder_factory,
    t_master,
    t_replica,
    with_tls_server_args,
):
    # 1. Spin up dragonfly without tls, debug populate
    master = df_factory.create(
        no_tls_on_admin_port="true",
        admin_port=ADMIN_PORT,
        **with_tls_server_args,
        requirepass="XXX",
        proactor_threads=t_master,
    )
    master.start()
    c_master = master.admin_client(password="XXX")
    await c_master.execute_command("DEBUG POPULATE 100")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_factory.create(
        no_tls_on_admin_port="true",
        admin_port=ADMIN_PORT + 1,
        **with_tls_server_args,
        proactor_threads=t_replica,
        requirepass="XXX",
        masterauth="XXX",
    )
    replica.start()
    c_replica = replica.admin_client(password="XXX")
    res = await c_replica.execute_command("REPLICAOF localhost " + str(master.admin_port))
    assert "OK" == res
    await check_all_replicas_finished([c_replica], c_master)

    # 3. Verify that replica dbsize == debug populate key size -- replication works
    db_size = await c_replica.execute_command("DBSIZE")
    assert 100 == db_size


# 1. Number of master threads
# 2. Number of threads for each replica
# 3. Admin port
replication_cases = [(8, 8, False), (8, 8, True)]


@pytest.mark.parametrize("t_master, t_replica, test_admin_port", replication_cases)
async def test_tls_replication(
    df_factory,
    df_seeder_factory,
    t_master,
    t_replica,
    test_admin_port,
    with_ca_tls_server_args,
    with_ca_tls_client_args,
):
    # 1. Spin up dragonfly tls enabled, debug populate
    master = df_factory.create(
        tls_replication="true",
        **with_ca_tls_server_args,
        port=1111,
        admin_port=ADMIN_PORT,
        proactor_threads=t_master,
    )
    master.start()
    c_master = master.client(**with_ca_tls_client_args)
    await c_master.execute_command("DEBUG POPULATE 100")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    proxy = Proxy(
        "127.0.0.1", 1114, "127.0.0.1", master.port if not test_admin_port else master.admin_port
    )
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_factory.create(
        tls_replication="true",
        **with_ca_tls_server_args,
        proactor_threads=t_replica,
    )
    replica.start()
    c_replica = replica.client(**with_ca_tls_client_args)
    res = await c_replica.execute_command("REPLICAOF localhost " + str(proxy.port))
    assert "OK" == res
    await check_all_replicas_finished([c_replica], c_master)

    # 3. Verify that replica dbsize == debug populate key size -- replication works
    db_size = await c_replica.execute_command("DBSIZE")
    assert 100 == db_size

    # 4. Break the connection between master and replica
    await proxy.close(proxy_task)
    await asyncio.sleep(3)
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

    # Check replica gets new keys
    await c_master.execute_command("SET MY_KEY 1")
    db_size = await c_master.execute_command("DBSIZE")
    assert 101 == db_size

    await check_all_replicas_finished([c_replica], c_master)
    db_size = await c_replica.execute_command("DBSIZE")
    assert 101 == db_size

    await proxy.close(proxy_task)


@pytest.mark.exclude_epoll
async def test_ipv6_replication(df_factory: DflyInstanceFactory):
    """Test that IPV6 addresses work for replication, ::1 is 127.0.0.1 localhost"""
    master = df_factory.create(proactor_threads=1, bind="::1", port=1111)
    replica = df_factory.create(proactor_threads=1, bind="::1", port=1112)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    assert await c_master.ping()
    assert await c_replica.ping()
    assert await c_replica.execute_command("REPLICAOF", master["bind"], master["port"]) == "OK"


# busy wait for 'replica' instance to have replication status 'status'
async def wait_for_replica_status(
    replica: aioredis.Redis, status: str, wait_for_seconds=0.01, timeout=20
):
    start = time.time()
    while (time.time() - start) < timeout:
        await asyncio.sleep(wait_for_seconds)

        info = await replica.info("replication")
        if info["master_link_status"] == status:
            return
    raise RuntimeError("Client did not become available in time!")


async def test_replicaof_flag(df_factory):
    # tests --replicaof works under normal conditions
    master = df_factory.create(
        proactor_threads=2,
    )

    # set up master
    master.start()
    c_master = master.client()
    await c_master.set("KEY", "VALUE")
    db_size = await c_master.dbsize()
    assert 1 == db_size

    replica = df_factory.create(
        proactor_threads=2,
        replicaof=f"localhost:{master.port}",  # start to replicate master
    )

    # set up replica. check that it is replicating
    replica.start()
    c_replica = replica.client()

    await wait_available_async(c_replica)  # give it time to startup
    # wait until we have a connection
    await check_all_replicas_finished([c_replica], c_master)

    dbsize = await c_replica.dbsize()
    assert 1 == dbsize

    val = await c_replica.get("KEY")
    assert "VALUE" == val


async def test_replicaof_flag_replication_waits(df_factory):
    # tests --replicaof works when we launch replication before the master
    BASE_PORT = 1111
    replica = df_factory.create(
        proactor_threads=2,
        replicaof=f"localhost:{BASE_PORT}",  # start to replicate master
    )

    # set up replica first
    replica.start()
    c_replica = replica.client()
    await wait_for_replica_status(c_replica, status="down")

    # check that it is in replica mode, yet status is down
    info = await c_replica.info("replication")
    assert info["role"] == "slave"
    assert info["master_host"] == "localhost"
    assert info["master_port"] == BASE_PORT
    assert info["master_link_status"] == "down"

    # set up master
    master = df_factory.create(
        port=BASE_PORT,
        proactor_threads=2,
    )

    master.start()
    c_master = master.client()
    await c_master.set("KEY", "VALUE")
    db_size = await c_master.dbsize()
    assert 1 == db_size

    # check that replication works now
    await wait_for_replica_status(c_replica, status="up")
    await check_all_replicas_finished([c_replica], c_master)

    dbsize = await c_replica.dbsize()
    assert 1 == dbsize

    val = await c_replica.get("KEY")
    assert "VALUE" == val


async def test_replicaof_flag_disconnect(df_factory):
    # test stopping replication when started using --replicaof
    master = df_factory.create(
        proactor_threads=2,
    )

    # set up master
    master.start()
    c_master = master.client()
    await wait_available_async(c_master)

    await c_master.set("KEY", "VALUE")
    db_size = await c_master.dbsize()
    assert 1 == db_size

    replica = df_factory.create(
        proactor_threads=2,
        replicaof=f"localhost:{master.port}",  # start to replicate master
    )

    # set up replica. check that it is replicating
    replica.start()

    c_replica = replica.client()
    await wait_available_async(c_replica)
    await check_all_replicas_finished([c_replica], c_master)

    dbsize = await c_replica.dbsize()
    assert 1 == dbsize

    val = await c_replica.get("KEY")
    assert "VALUE" == val

    await c_replica.replicaof("no", "one")  # disconnect

    role = await c_replica.role()
    assert role[0] == "master"


async def test_df_crash_on_memcached_error(df_factory):
    master = df_factory.create(
        memcached_port=11211,
        proactor_threads=2,
    )

    replica = df_factory.create(
        memcached_port=master.mc_port + 1,
        proactor_threads=2,
    )

    master.start()
    replica.start()

    c_master = master.client()
    await wait_available_async(c_master)

    c_replica = replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    memcached_client = pymemcache.Client(f"127.0.0.1:{replica.mc_port}")

    with pytest.raises(pymemcache.exceptions.MemcacheServerError):
        memcached_client.set("key", "data", noreply=False)


async def test_df_crash_on_replicaof_flag(df_factory):
    master = df_factory.create(
        proactor_threads=2,
    )
    master.start()

    replica = df_factory.create(proactor_threads=2, replicaof=f"127.0.0.1:{master.port}")
    replica.start()

    c_master = master.client()
    c_replica = replica.client()

    await wait_available_async(c_master)
    await wait_available_async(c_replica)

    res = await c_replica.execute_command("SAVE DF myfile")
    assert "OK" == res

    res = await c_replica.execute_command("DBSIZE")
    assert res == 0


async def test_network_disconnect(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=6)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([replica, master])
    seeder = df_seeder_factory.create(port=master.port)

    async with replica.client() as c_replica:
        await seeder.run(target_deviation=0.1)

        proxy = Proxy("127.0.0.1", 1111, "127.0.0.1", master.port)
        await proxy.start()
        task = asyncio.create_task(proxy.serve())
        try:
            await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

            for _ in range(10):
                await asyncio.sleep(random.randint(0, 10) / 10)
                proxy.drop_connection()

            # Give time to detect dropped connection and reconnect
            await asyncio.sleep(1.0)
            await wait_available_async(c_replica)

            capture = await seeder.capture()
            assert await seeder.compare(capture, replica.port)
        finally:
            await proxy.close(task)


async def test_network_disconnect_active_stream(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=4, shard_repl_backlog_len=4000)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([replica, master])
    seeder = df_seeder_factory.create(port=master.port)

    async with replica.client() as c_replica, master.client() as c_master:
        await seeder.run(target_deviation=0.1)

        proxy = Proxy("127.0.0.1", 1112, "127.0.0.1", master.port)
        await proxy.start()
        task = asyncio.create_task(proxy.serve())
        try:
            await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

            fill_task = asyncio.create_task(seeder.run(target_ops=4000))

            for _ in range(3):
                await asyncio.sleep(random.randint(10, 20) / 10)
                proxy.drop_connection()

            seeder.stop()
            await fill_task

            # Give time to detect dropped connection and reconnect
            await asyncio.sleep(1.0)
            await wait_available_async(c_replica)

            logging.debug(await c_replica.execute_command("INFO REPLICATION"))
            logging.debug(await c_master.execute_command("INFO REPLICATION"))

            capture = await seeder.capture()
            assert await seeder.compare(capture, replica.port)
        finally:
            await proxy.close(task)


async def test_network_disconnect_small_buffer(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=4, shard_repl_backlog_len=1)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([replica, master])
    seeder = df_seeder_factory.create(port=master.port)

    async with replica.client() as c_replica, master.client() as c_master:
        await seeder.run(target_deviation=0.1)

        proxy = Proxy("127.0.0.1", 1113, "127.0.0.1", master.port)
        await proxy.start()
        task = asyncio.create_task(proxy.serve())

        try:
            await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

            # If this ever fails gain, adjust the target_ops
            # Df is blazingly fast, so by the time we tick a second time on
            # line 1674, DF already replicated all the data so the assertion
            # at the end of the test will always fail
            fill_task = asyncio.create_task(seeder.run())

            for _ in range(3):
                await asyncio.sleep(random.randint(5, 10) / 10)
                proxy.drop_connection()

            seeder.stop()
            await fill_task

            # Give time to detect dropped connection and reconnect
            await asyncio.sleep(1.0)
            await wait_available_async(c_replica)

            # logging.debug(await c_replica.execute_command("INFO REPLICATION"))
            # logging.debug(await c_master.execute_command("INFO REPLICATION"))
            capture = await seeder.capture()
            assert await seeder.compare(capture, replica.port)
        finally:
            await proxy.close(task)

    # Partial replication is currently not implemented so the following does not work
    # assert master.is_in_logs("Partial sync requested from stale LSN")


async def test_replica_reconnections_after_network_disconnect(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=6)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([replica, master])
    seeder = df_seeder_factory.create(port=master.port)

    async with replica.client() as c_replica:
        await seeder.run(target_deviation=0.1)

        proxy = Proxy("127.0.0.1", 1115, "127.0.0.1", master.port)
        await proxy.start()
        task = asyncio.create_task(proxy.serve())
        try:
            await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

            # Wait replica to be up and synchronized with master
            await wait_available_async(c_replica)

            initial_reconnects_count = await get_replica_reconnects_count(replica)

            # Fully drop the server
            await proxy.close(task)

            # After dropping the connection replica should try to reconnect
            await wait_for_replica_status(c_replica, status="down")
            await asyncio.sleep(2)

            # Restart the proxy
            await proxy.start()
            task = asyncio.create_task(proxy.serve())

            # Wait replica to be reconnected and synchronized with master
            await wait_available_async(c_replica)

            capture = await seeder.capture()
            assert await seeder.compare(capture, replica.port)

            # Assert replica reconnects metrics increased
            await assert_replica_reconnections(replica, initial_reconnects_count)

        finally:
            await proxy.close(task)


async def test_search(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

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
    master = df_factory.create()
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

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
    assert await c_replica.execute_command("FT.SEARCH i1 * LIMIT 0 0") == [9_001]
    assert await c_replica.execute_command('FT.SEARCH i1 "secret"') == [
        1,
        "secret-key",
        ["name", "new-secret"],
    ]


# @pytest.mark.slow
async def test_client_pause_with_replica(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(port=master.port)

    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

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


async def test_replicaof_reject_on_load(df_factory, df_seeder_factory):
    master = df_factory.create()
    replica = df_factory.create(dbfilename=f"dump_{tmp_file_name()}")
    df_factory.start_all([master, replica])

    c_replica = replica.client()
    await c_replica.execute_command(f"DEBUG POPULATE 1000 key 1000 RAND type set elements 2000")

    replica.stop()
    replica.start()
    c_replica = replica.client()

    @assert_eventually
    async def check_replica_isloading():
        persistence = await c_replica.info("PERSISTENCE")
        assert persistence["loading"] == 1

    # If this fails adjust load of DEBUG POPULATE above.
    await check_replica_isloading()

    # Check replica of not alowed while loading snapshot
    # Keep in mind that if the exception has not been raised, it doesn't mean
    # that there is a bug because it could be the case that while executing
    # INFO PERSISTENCE df is in loading state but when we call REPLICAOF df
    # is no longer in loading state and the assertion false is triggered.
    with pytest.raises(aioredis.BusyLoadingError):
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    # Check one we finish loading snapshot replicaof success
    await wait_available_async(c_replica, timeout=180)
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")


async def test_heartbeat_eviction_propagation(df_factory):
    master = df_factory.create(
        proactor_threads=1, cache_mode="true", maxmemory="256mb", enable_heartbeat_eviction="false"
    )
    replica = df_factory.create(proactor_threads=1)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # fill the master to use about 233mb > 256mb * 0.9, which will trigger heartbeat eviction.
    await c_master.execute_command("DEBUG POPULATE 233 size 1048576")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

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
    master = df_factory.create(
        proactor_threads=2,
        cache_mode="true",
        maxmemory="512mb",
        logtostdout="true",
        enable_heartbeat_eviction="false",
        rss_oom_deny_ratio=1.3,
    )
    replica = df_factory.create(proactor_threads=2)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("DEBUG POPULATE 6000 size 88000")

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    seeder = df_seeder_factory.create(
        port=master.port, keys=500, val_size=1000, stop_on_failure=False
    )
    await seeder.run(target_deviation=0.1)

    info = await c_master.info("stats")
    assert info["evicted_keys"] > 0, "Weak testcase: policy based eviction was not triggered."

    await check_all_replicas_finished([c_replica], c_master)
    keys_master = await c_master.execute_command("keys k*")
    keys_replica = await c_replica.execute_command("keys k*")

    assert set(keys_replica).difference(keys_master) == set()
    assert set(keys_master).difference(keys_replica) == set()


async def test_journal_doesnt_yield_issue_2500(df_factory, df_seeder_factory):
    """
    Issues many SETEX commands through a Lua script so that no yields are done between them.
    In parallel, connect a replica, so that these SETEX commands write their custom journal log.
    This makes sure that no Fiber context switch while inside a shard callback.
    """
    master = df_factory.create()
    replica = df_factory.create()
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    async def send_setex():
        script = """
        local charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

        local random_string = function(length)
            local str = ''
            for i=1,length do
                str = str .. charset:sub(math.random(1, #charset))
            end
            return str
        end

        for i = 1, 200 do
            -- 200 iterations to make sure SliceSnapshot dest queue is full
            -- 100 bytes string to make sure serializer is big enough
            redis.call('SETEX', KEYS[1], 1000, random_string(100))
        end
        """

        for i in range(10):
            await asyncio.gather(
                *[c_master.eval(script, 1, random.randint(0, 1_000)) for j in range(3)]
            )

    stream_task = asyncio.create_task(send_setex())
    await asyncio.sleep(0.1)

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    assert not stream_task.done(), "Weak testcase. finished sending commands before replication."

    await wait_available_async(c_replica)
    await stream_task

    await check_all_replicas_finished([c_replica], c_master)
    keys_master = await c_master.execute_command("keys *")
    keys_replica = await c_replica.execute_command("keys *")
    assert set(keys_master) == set(keys_replica)


async def test_saving_replica(df_factory):
    master = df_factory.create(proactor_threads=1)
    replica = df_factory.create(proactor_threads=1, dbfilename=f"dump_{tmp_file_name()}")
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("DEBUG POPULATE 100000 key 4048 RAND")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    async def save_replica():
        await c_replica.execute_command("save")

    save_task = asyncio.create_task(save_replica())
    while not await is_saving(c_replica):  # wait for replica start saving
        assert "rdb_changes_since_last_success_save:0" not in await c_replica.execute_command(
            "info persistence"
        ), "Weak test case, finished saving too quickly"
        await asyncio.sleep(0.1)
    await c_replica.execute_command("replicaof no one")
    assert await is_saving(c_replica)
    await save_task
    assert not await is_saving(c_replica)


async def test_start_replicating_while_save(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4, dbfilename=f"dump_{tmp_file_name()}")
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command("DEBUG POPULATE 100000 key 4096 RAND")

    async def save_replica():
        await c_replica.execute_command("save")

    save_task = asyncio.create_task(save_replica())
    while not await is_saving(c_replica):  # wait for server start saving
        assert "rdb_changes_since_last_success_save:0" not in await c_replica.execute_command(
            "info persistence"
        ), "Weak test case, finished saving too quickly"
        await asyncio.sleep(0.1)
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    assert await is_saving(c_replica)
    await save_task
    assert not await is_saving(c_replica)


async def test_user_acl_replication(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    c_master = master.client()
    await c_master.execute_command("ACL SETUSER tmp >tmp ON +ping +dfly +replconf")
    await c_master.execute_command("SET foo bar")
    assert 1 == await c_master.execute_command("DBSIZE")

    c_replica = replica.client()
    await c_replica.execute_command("CONFIG SET masteruser tmp")
    await c_replica.execute_command("CONFIG SET masterauth tmp")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    await wait_available_async(c_replica)
    assert 1 == await c_replica.execute_command("DBSIZE")

    # revoke acl's from tmp
    await c_master.execute_command("ACL SETUSER tmp -replconf")
    async for info, breaker in info_tick_timer(c_replica, section="REPLICATION"):
        with breaker:
            assert info["master_link_status"] == "down"

    await c_master.execute_command("SET bar foo")

    # reinstate and let replication continue
    await c_master.execute_command("ACL SETUSER tmp +replconf")
    await check_all_replicas_finished([c_replica], c_master, 5)
    assert 2 == await c_replica.execute_command("DBSIZE")


@pytest.mark.parametrize("break_conn", [False, True])
async def test_replica_reconnect(df_factory, break_conn):
    """
    Test replica does not connect to master if master restarted
    step1: create master and replica
    step2: stop master and start again with the same port
    step3: check replica is not replicating the restarted master
    step4: issue new replicaof command
    step5: check replica replicates master
    """
    # Connect replica to master
    master = df_factory.create(proactor_threads=1)
    replica = df_factory.create(proactor_threads=1, break_replication_on_master_restart=break_conn)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.set("k", "12345")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)
    assert (await c_replica.info("REPLICATION"))["master_link_status"] == "up"

    # kill existing master, create master with different repl_id but same port
    master_port = master.port
    master.stop()
    assert (await c_replica.info("REPLICATION"))["master_link_status"] == "down"

    master = df_factory.create(proactor_threads=1, port=master_port)
    df_factory.start_all([master])
    await asyncio.sleep(1)  # We sleep for 0.5s in replica.cc before reconnecting

    # Assert that replica did not reconnected to master with different repl_id
    if break_conn:
        assert await c_master.execute_command("get k") == None
        assert await c_replica.execute_command("get k") == "12345"
        assert await c_master.execute_command("set k 6789")
        assert await c_replica.execute_command("get k") == "12345"
        assert (await c_replica.info("REPLICATION"))["master_link_status"] == "down"
    else:
        assert await c_master.execute_command("get k") == None
        assert await c_replica.execute_command("get k") == None
        assert await c_master.execute_command("set k 6789")
        await check_all_replicas_finished([c_replica], c_master)
        assert await c_replica.execute_command("get k") == "6789"
        assert (await c_replica.info("REPLICATION"))["master_link_status"] == "up"

    # Force re-replication, assert that it worked
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)
    assert await c_replica.execute_command("get k") == "6789"


async def test_announce_ip_port(df_factory):
    master = df_factory.create()
    replica = df_factory.create(replica_announce_ip="overrode-host", announce_port="1337")

    master.start()
    replica.start()

    # Connect clients, connect replica to master
    c_master = master.client()
    c_replica = replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    role, node = await c_master.execute_command("role")
    assert role == "master"
    host, port, _ = node[0]
    assert host == "overrode-host"
    assert port == "1337"


async def test_replication_timeout_on_full_sync(df_factory: DflyInstanceFactory, df_seeder_factory):
    # setting replication_timeout to a very small value to force the replica to timeout
    master = df_factory.create(replication_timeout=100, vmodule="replica=2,dflycmd=2")
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("debug", "populate", "200000", "foo", "5000", "RAND")
    seeder = df_seeder_factory.create(port=master.port)
    seeder_task = asyncio.create_task(seeder.run())

    await asyncio.sleep(0.5)  # wait for seeder running

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    # wait for full sync
    async with async_timeout.timeout(3):
        await wait_for_replicas_state(c_replica, state="full_sync", timeout=0.05)

    await c_replica.execute_command(
        "debug replica pause"
    )  # pause replica to trigger reconnect on master

    await asyncio.sleep(1)

    await c_replica.execute_command("debug replica resume")  # resume replication

    await asyncio.sleep(1)  # replica will start resync
    seeder.stop()
    await seeder_task

    await check_all_replicas_finished([c_replica], c_master, timeout=30)
    await assert_replica_reconnections(replica, 0)


@dfly_args({"proactor_threads": 1})
async def test_master_stalled_disconnect(df_factory: DflyInstanceFactory):
    # disconnect after 1 second of being blocked
    master = df_factory.create(replication_timeout=1000)
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("debug", "populate", "200000", "foo", "500", "RAND")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    @assert_eventually
    async def check_replica_connected():
        repl_info = await c_master.info("replication")
        assert "slave0" in repl_info

    @assert_eventually
    async def check_replica_disconnected():
        repl_info = await c_master.info("replication")
        assert "slave0" not in repl_info

    await check_replica_connected()
    await c_replica.execute_command("DEBUG REPLICA PAUSE")
    await check_replica_connected()  # still connected
    await asyncio.sleep(1)  # wait for the master to recognize it's being blocked
    await check_replica_disconnected()


def download_dragonfly_release(version):
    path = f"/tmp/old_df/{version}"
    binary = f"{path}/dragonfly-x86_64"
    if os.path.isfile(binary):
        return binary

    # Cleanup in case there's partial files
    if os.path.exists(path):
        shutil.rmtree(path)

    os.makedirs(path)
    gzfile = f"{path}/dragonfly.tar.gz"
    logging.debug(f"Downloading Dragonfly release into {gzfile}...")

    # Download
    urllib.request.urlretrieve(
        f"https://github.com/dragonflydb/dragonfly/releases/download/{version}/dragonfly-x86_64.tar.gz",
        gzfile,
    )

    # Extract
    file = tarfile.open(gzfile)
    file.extractall(path)
    file.close()

    # Return path
    return binary


@pytest.mark.parametrize(
    "cluster_mode, announce_ip, announce_port",
    [
        ("", "localhost", 7000),
        ("emulated", "", 0),
        ("emulated", "localhost", 7000),
    ],
)
async def test_replicate_old_master(
    df_factory: DflyInstanceFactory, cluster_mode, announce_ip, announce_port
):
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.19.2"
    released_dfly_path = download_dragonfly_release(dfly_version)
    master = df_factory.create(
        version=1.19,
        path=released_dfly_path,
        cluster_mode=cluster_mode,
    )
    replica = df_factory.create(
        cluster_mode=cluster_mode,
        cluster_announce_ip=announce_ip,
        announce_port=announce_port,
    )

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    assert (
        f"df-{dfly_version}"
        == (await c_master.execute_command("info", "server"))["dragonfly_version"]
    )
    assert dfly_version != (await c_replica.execute_command("info", "server"))["dragonfly_version"]

    await c_master.execute_command("set", "k1", "v1")

    assert await c_replica.execute_command(f"REPLICAOF localhost {master.port}") == "OK"
    await wait_available_async(c_replica)

    assert await c_replica.execute_command("get", "k1") == "v1"


# This Test was intorduced in response to a bug when replicating empty hashmaps (encoded as
# ziplists) created with HSET, HSETEX, HDEL and then replicated 2 times.
# For more information plz refer to the issue on gh:
# https://github.com/dragonflydb/dragonfly/issues/3504
@dfly_args({"proactor_threads": 1})
async def test_empty_hash_map_replicate_old_master(df_factory):
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.21.2"
    released_dfly_path = download_dragonfly_release(dfly_version)
    # old versions
    instances = [df_factory.create(path=released_dfly_path, version=1.21) for i in range(3)]
    # new version
    instances.append(df_factory.create())

    df_factory.start_all(instances)

    old_c_master = instances[0].client()
    # Create an empty hashmap
    await old_c_master.execute_command("HSET foo a_field a_value")
    await old_c_master.execute_command("HSETEX foo 2 b_field b_value")
    await old_c_master.execute_command("HDEL foo a_field")

    @assert_eventually
    async def check_if_empty():
        assert await old_c_master.execute_command("HGETALL foo") == []

    await check_if_empty()
    assert await old_c_master.execute_command(f"EXISTS foo") == 1
    await old_c_master.aclose()

    async def assert_body(client, result=1, state="online", node_role="slave"):
        async with async_timeout.timeout(10):
            await wait_for_replicas_state(client, state=state, node_role=node_role)

        assert await client.execute_command(f"EXISTS foo") == result
        assert await client.execute_command("REPLTAKEOVER 1") == "OK"

    index = 0
    last_old_replica = 2

    # Adjacent pairs
    for a, b in zip(instances, instances[1:]):
        logging.debug(index)
        client_b = b.client()
        assert await client_b.execute_command(f"REPLICAOF localhost {a.port}") == "OK"

        if index != last_old_replica:
            await assert_body(client_b, state="stable_sync", node_role="replica")
        else:
            await assert_body(client_b, result=0)

        index = index + 1
        await client_b.aclose()


# This Test was intorduced in response to a bug when replicating empty hash maps with
# HSET, HSETEX, HDEL and then loaded via replication.
# For more information plz refer to the issue on gh:
# https://github.com/dragonflydb/dragonfly/issues/3504
@dfly_args({"proactor_threads": 1})
async def test_empty_hashmap_loading_bug(df_factory: DflyInstanceFactory):
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.21.2"
    released_dfly_path = download_dragonfly_release(dfly_version)

    master = df_factory.create(path=released_dfly_path, version=1.21)
    master.start()

    c_master = master.client()
    # Create an empty hashmap
    await c_master.execute_command("HSET foo a_field a_value")
    await c_master.execute_command("HSETEX foo 2 b_field b_value")
    await c_master.execute_command("HDEL foo a_field")

    @assert_eventually
    async def check_if_empty():
        assert await c_master.execute_command("HGETALL foo") == []

    await check_if_empty()
    assert await c_master.execute_command(f"EXISTS foo") == 1

    replica = df_factory.create()
    replica.start()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)
    assert await c_replica.execute_command(f"dbsize") == 0


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

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

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


async def test_double_take_over(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=4, dbfilename="", admin_port=ADMIN_PORT)
    replica = df_factory.create(proactor_threads=4, dbfilename="", admin_port=ADMIN_PORT + 1)
    df_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(port=master.port, keys=1000, dbcount=5, stop_on_failure=False)
    await seeder.run(target_deviation=0.1)

    capture = await seeder.capture(port=master.port)

    c_replica = replica.client()

    logging.debug("start replication")
    await c_replica.execute_command(f"REPLICAOF localhost {master.admin_port}")
    await wait_available_async(c_replica)

    logging.debug("running repltakover")
    await c_replica.execute_command(f"REPLTAKEOVER 10")
    assert await c_replica.execute_command("role") == ["master", []]

    @assert_eventually
    async def check_master_status():
        assert master.proc.poll() == 0, "Master process did not exit correctly."

    await check_master_status()

    logging.debug("restart previous master")
    master.start()
    c_master = master.client()

    logging.debug("start second replication")
    await c_master.execute_command(f"REPLICAOF localhost {replica.admin_port}")
    await wait_available_async(c_master)

    logging.debug("running second repltakover")
    await c_master.execute_command(f"REPLTAKEOVER 10")
    assert await c_master.execute_command("role") == ["master", []]

    assert await seeder.compare(capture, port=master.port)


async def test_replica_of_replica(df_factory):
    # Can't connect a replica to a replica, but OK to connect 2 replicas to the same master
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)
    replica2 = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica, replica2])

    c_replica = replica.client()
    c_replica2 = replica2.client()

    assert await c_replica.execute_command(f"REPLICAOF localhost {master.port}") == "OK"

    with pytest.raises(redis.exceptions.ResponseError):
        await c_replica2.execute_command(f"REPLICAOF localhost {replica.port}")

    assert await c_replica2.execute_command(f"REPLICAOF localhost {master.port}") == "OK"


async def test_replication_timeout_on_full_sync_heartbeat_expiry(
    df_factory: DflyInstanceFactory, df_seeder_factory
):
    # Timeout set to 3 seconds because we must first saturate the socket such that subsequent
    # writes block. Otherwise, we will break the flows before Heartbeat actually deadlocks.
    master = df_factory.create(
        proactor_threads=2, replication_timeout=3000, vmodule="replica=2,dflycmd=2"
    )
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("debug", "populate", "100000", "foo", "5000", "RAND")

    c_master = master.client()
    c_replica = replica.client()

    seeder = ExpirySeeder()
    seeder_task = asyncio.create_task(seeder.run(c_master))
    await seeder.wait_until_n_inserts(50000)
    seeder.stop()
    await seeder_task

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    # wait for full sync
    async with async_timeout.timeout(3):
        await wait_for_replicas_state(c_replica, state="full_sync", timeout=0.05)

    await c_replica.execute_command("debug replica pause")

    # Dragonfly would get stuck here without the bug fix. When replica does not read from the
    # socket, Heartbeat() will block on the journal write for the expired items and shard_handler
    # would never be called and break replication. More details on #3936.

    await asyncio.sleep(6)

    await c_replica.execute_command("debug replica resume")  # resume replication

    await asyncio.sleep(1)  # replica will start resync

    await check_all_replicas_finished([c_replica], c_master)
    await assert_replica_reconnections(replica, 0)


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
    seeder = StaticSeeder(
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

    # Check replica data consisten
    replica_data = await StaticSeeder.capture(c_replica)
    master_data = await StaticSeeder.capture(c_master)
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
