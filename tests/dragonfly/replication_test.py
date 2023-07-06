import random
from itertools import chain, repeat
import re
import pytest
import asyncio
import redis
from redis import asyncio as aioredis
from .utility import *
from . import DflyInstanceFactory, dfly_args
import logging

BASE_PORT = 1111
ADMIN_PORT = 1211

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

"""
Test full replication pipeline. Test full sync with streaming changes and stable state streaming.
"""

# 1. Number of master threads
# 2. Number of threads for each replica
# 3. Seeder config
replication_cases = [
    (8, [8], dict(keys=10_000, dbcount=4)),
    (6, [6, 6, 6], dict(keys=4_000, dbcount=4)),
    (8, [2, 2, 2, 2], dict(keys=4_000, dbcount=4)),
    (4, [8, 8], dict(keys=4_000, dbcount=4)),
    (4, [1] * 8, dict(keys=500, dbcount=2)),
    (1, [1], dict(keys=100, dbcount=2)),
]


@pytest.mark.skip("fails locally")
@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, seeder_config", replication_cases)
async def test_replication_all(df_local_factory, df_seeder_factory, t_master, t_replicas, seeder_config):
    master = df_local_factory.create(port=BASE_PORT, proactor_threads=t_master)
    replicas = [
        df_local_factory.create(port=BASE_PORT+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Start master
    master.start()
    c_master = aioredis.Redis(port=master.port)

    # Fill master with test data
    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    # Start replicas
    df_local_factory.start_all(replicas)

    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]

    # Start data stream
    stream_task = asyncio.create_task(seeder.run(target_ops=3000))
    await asyncio.sleep(0.0)

    # Start replication
    async def run_replication(c_replica):
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))

    await asyncio.gather(*(asyncio.create_task(run_replication(c))
                           for c in c_replicas))

    # Wait for streaming to finish
    assert not stream_task.done(
    ), "Weak testcase. Increase number of streamed iterations to surpass full sync"
    await stream_task

    # Check data after full sync
    await check_all_replicas_finished(c_replicas, c_master)
    await check_data(seeder, replicas, c_replicas)

    # Stream more data in stable state
    await seeder.run(target_ops=2000)

    # Check data after stable state stream
    await check_all_replicas_finished(c_replicas, c_master)
    await check_data(seeder, replicas, c_replicas)


async def check_replica_finished_exec(c_replica, c_master):
    syncid, r_offset = await c_replica.execute_command("DEBUG REPLICA OFFSET")
    m_offset = await c_master.execute_command("DFLY REPLICAOFFSET")

    print("  offset", syncid.decode(),  r_offset, m_offset)
    return r_offset == m_offset


async def check_all_replicas_finished(c_replicas, c_master):
    print("Waiting for replicas to finish")

    waiting_for = list(c_replicas)
    while len(waiting_for) > 0:
        await asyncio.sleep(1.0)

        tasks = (asyncio.create_task(check_replica_finished_exec(c, c_master))
                 for c in waiting_for)
        finished_list = await asyncio.gather(*tasks)

        # Remove clients that finished from waiting list
        waiting_for = [c for (c, finished) in zip(
            waiting_for, finished_list) if not finished]


async def check_data(seeder, replicas, c_replicas):
    capture = await seeder.capture()
    for (replica, c_replica) in zip(replicas, c_replicas):
        await wait_available_async(c_replica)
        assert await seeder.compare(capture, port=replica.port)


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
    (8, [], [], [4] * 4, 4_000)
]


@pytest.mark.skip(reason='Failing on github regression action')
@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_crash_fs, t_crash_ss, t_disonnect, n_keys", disconnect_cases)
async def test_disconnect_replica(df_local_factory: DflyInstanceFactory, df_seeder_factory, t_master, t_crash_fs, t_crash_ss, t_disonnect, n_keys):
    master = df_local_factory.create(port=BASE_PORT, proactor_threads=t_master)
    replicas = [
        (df_local_factory.create(port=BASE_PORT+i+1, proactor_threads=t), crash_fs)
        for i, (t, crash_fs) in enumerate(
            chain(
                zip(t_crash_fs, repeat(DISCONNECT_CRASH_FULL_SYNC)),
                zip(t_crash_ss, repeat(DISCONNECT_CRASH_STABLE_SYNC)),
                zip(t_disonnect, repeat(DISCONNECT_NORMAL_STABLE_SYNC))
            )
        )
    ]

    # Start master
    master.start()
    c_master = aioredis.Redis(port=master.port, single_connection_client=True)

    # Start replicas and create clients
    df_local_factory.start_all([replica for replica, _ in replicas])

    c_replicas = [
        (replica, aioredis.Redis(port=replica.port), crash_type)
        for replica, crash_type in replicas
    ]

    def replicas_of_type(tfunc):
        return [
            args for args in c_replicas
            if tfunc(args[2])
        ]

    # Start data fill loop
    seeder = df_seeder_factory.create(port=master.port, keys=n_keys, dbcount=2)
    fill_task = asyncio.create_task(seeder.run())

    # Run full sync
    async def full_sync(replica, c_replica, crash_type):
        c_replica = aioredis.Redis(port=replica.port)
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
        if crash_type == 0:
            await asyncio.sleep(random.random()/100+0.01)
            await c_replica.connection_pool.disconnect()
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

    # Run stable state crashes
    async def stable_sync(replica, c_replica, crash_type):
        await asyncio.sleep(random.random() / 100)
        await c_replica.connection_pool.disconnect()
        replica.stop(kill=True)

    await asyncio.gather(*(stable_sync(*args) for args
                           in replicas_of_type(lambda t: t == 1)))

    # Check master survived all crashes
    assert await c_master.ping()

    # Check phase 3 replica survived
    for _, c_replica, _ in replicas_of_type(lambda t: t > 1):
        assert await c_replica.ping()

    # Stop streaming
    seeder.stop()
    await fill_task

    # Check master survived all crashes
    assert await c_master.ping()

    # Check phase 3 replicas are up-to-date and there is no gap or lag
    await seeder.run(target_ops=2000)
    await asyncio.sleep(1.0)

    capture = await seeder.capture()
    for replica, _, _ in replicas_of_type(lambda t: t > 1):
        assert await seeder.compare(capture, port=replica.port)

    # Check disconnects
    async def disconnect(replica, c_replica, crash_type):
        await asyncio.sleep(random.random() / 100)
        await c_replica.execute_command("REPLICAOF NO ONE")

    await asyncio.gather(*(disconnect(*args) for args
                           in replicas_of_type(lambda t: t == 2)))

    await asyncio.sleep(0.5)

    # Check phase 3 replica survived
    for replica, c_replica, _ in replicas_of_type(lambda t: t == 2):
        assert await c_replica.ping()
        assert await seeder.compare(capture, port=replica.port)
        await c_replica.connection_pool.disconnect()

    # Check master survived all disconnects
    assert await c_master.ping()
    await c_master.close()


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


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, n_random_crashes, n_keys", master_crash_cases)
async def test_disconnect_master(df_local_factory, df_seeder_factory, t_master, t_replicas, n_random_crashes, n_keys):
    master = df_local_factory.create(port=1111, proactor_threads=t_master)
    replicas = [
        df_local_factory.create(port=BASE_PORT+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    df_local_factory.start_all(replicas)
    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]

    seeder = df_seeder_factory.create(port=master.port, keys=n_keys, dbcount=2)

    async def crash_master_fs():
        await asyncio.sleep(random.random() / 10)
        master.stop(kill=True)

    async def start_master():
        await asyncio.sleep(0.2)
        master.start()
        c_master = aioredis.Redis(port=master.port)
        assert await c_master.ping()
        seeder.reset()
        await seeder.run(target_deviation=0.1)

    await start_master()

    # Crash master during full sync, but with all passing initial connection phase
    await asyncio.gather(*(c_replica.execute_command("REPLICAOF localhost " + str(master.port))
                           for c_replica in c_replicas))
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

rotating_master_cases = [
    (4, [4, 4, 4, 4], dict(keys=2_000, dbcount=4))
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_replica, t_masters, seeder_config", rotating_master_cases)
async def test_rotating_masters(df_local_factory, df_seeder_factory, t_replica, t_masters, seeder_config):
    replica = df_local_factory.create(
        port=BASE_PORT, proactor_threads=t_replica)
    masters = [df_local_factory.create(
        port=BASE_PORT+i+1, proactor_threads=t) for i, t in enumerate(t_masters)]
    seeders = [df_seeder_factory.create(
        port=m.port, **seeder_config) for m in masters]

    df_local_factory.start_all([replica] + masters)

    c_replica = aioredis.Redis(port=replica.port)

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

@pytest.mark.skip("fails locally")
@pytest.mark.asyncio
async def test_cancel_replication_immediately(df_local_factory, df_seeder_factory):
    """
    Issue 40 replication commands randomally distributed over 10 seconds. This
    checks that the replication state machine can handle cancellation well.
    We assert that at least one command was cancelled during start and at least
    one command one successfull.
    After we finish the 'fuzzing' part, replicate the first master and check that
    all the data is correct.
    """
    COMMANDS_TO_ISSUE = 40

    replica = df_local_factory.create(port=BASE_PORT)
    masters = [df_local_factory.create(port=BASE_PORT+i+1) for i in range(4)]
    seeders = [df_seeder_factory.create(port=m.port) for m in masters]

    df_local_factory.start_all([replica] + masters)
    c_replica = aioredis.Redis(port=replica.port)
    await asyncio.gather(*(seeder.run(target_deviation=0.1) for seeder in seeders))

    replication_commands = []

    async def replicate(index):
        await asyncio.sleep(10.0 * random.random())
        try:
            start = time.time()
            await c_replica.execute_command(f"REPLICAOF localhost {masters[index].port}")
            # Giving replication commands shouldn't hang.
            assert time.time() - start < 2.0
            return True
        except redis.exceptions.ResponseError as e:
            assert e.args[0] == "replication cancelled"
            return False

    for i in range(COMMANDS_TO_ISSUE):
        index = random.choice(range(len(masters)))
        replication_commands.append(replicate(index))
    results = await asyncio.gather(*replication_commands)
    num_successes = sum(results)
    assert COMMANDS_TO_ISSUE > num_successes, "At least one REPLICAOF must be cancelled"
    assert num_successes > 0, "At least one REPLICAOF must be succeed"

    await c_replica.execute_command(f"REPLICAOF localhost {masters[0].port}")

    await wait_available_async(c_replica)
    capture = await seeders[0].capture()
    assert await seeders[0].compare(capture, replica.port)


"""
Test flushall command. Set data to master send flashall and set more data.
Check replica keys at the end.
"""

@pytest.mark.skip("fails locally")
@pytest.mark.asyncio
async def test_flushall(df_local_factory):
    master = df_local_factory.create(port=BASE_PORT, proactor_threads=4)
    replica = df_local_factory.create(port=BASE_PORT+1, proactor_threads=2)

    master.start()
    replica.start()

    # Connect replica to master
    c_replica = aioredis.Redis(port=replica.port)
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    n_keys = 1000

    def gen_test_data(start, end):
        for i in range(start, end):
            yield f"key-{i}", f"value-{i}"

    c_master = aioredis.Redis(port=master.port)
    pipe = c_master.pipeline(transaction=False)
    # Set simple keys 0..n_keys on master
    batch_fill_data(client=pipe, gen=gen_test_data(0, n_keys), batch_size=3)
    # flushall
    pipe.flushall()
    # Set simple keys n_keys..n_keys*2 on master
    batch_fill_data(client=pipe, gen=gen_test_data(
        n_keys, n_keys*2), batch_size=3)

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
    for i in range(n_keys, n_keys*2):
        pipe.get(f"key-{i}")
    vals = await pipe.execute()
    assert all(v is not None for v in vals)


"""
Test journal rewrites.
"""


@dfly_args({"proactor_threads": 4})
@pytest.mark.asyncio
async def test_rewrites(df_local_factory):
    CLOSE_TIMESTAMP = (int(time.time()) + 100)
    CLOSE_TIMESTAMP_MS = CLOSE_TIMESTAMP * 1000

    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(port=BASE_PORT+1)

    master.start()
    replica.start()

    # Connect clients, connect replica to master
    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    # Create monitor and bind utility functions
    m_replica = c_replica.monitor()

    async def get_next_command():
        mcmd = (await m_replica.next_command())['command']
        # skip select command
        if (mcmd == "SELECT 0"):
            print("Got:", mcmd)
            mcmd = (await m_replica.next_command())['command']
        print("Got:", mcmd)
        return mcmd

    async def is_match_rsp(rx):
        mcmd = (await get_next_command())
        print("Regex:", rx)
        return re.match(rx, mcmd)

    async def skip_cmd():
        await is_match_rsp(r".*")

    async def check(cmd, rx):
        await c_master.execute_command(cmd)
        match = (await is_match_rsp(rx))
        assert match

    async def check_list(cmd, rx_list):
        print("master cmd:", cmd)
        await c_master.execute_command(cmd)
        for rx in rx_list:
            match = (await is_match_rsp(rx))
            assert match

    async def check_list_ooo(cmd, rx_list):
        print("master cmd:", cmd)
        await c_master.execute_command(cmd)
        expected_cmds = len(rx_list)
        for i in range(expected_cmds):
            mcmd = (await get_next_command())
            # check command matches one regex from list
            match_rx = list(filter(lambda rx: re.match(rx, mcmd), rx_list))
            assert len(match_rx) == 1
            rx_list.remove(match_rx[0])

    async def check_expire(key):
        ttl1 = await c_master.ttl(key)
        ttl2 = await c_replica.ttl(key)
        await skip_cmd()
        assert abs(ttl1-ttl2) <= 1

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
        # Check RENAME turns into DEL SET and PEXPIREAT
        await check_list_ooo("RENAME renamekey renamed", [r"DEL renamekey", r"SET renamed 1000", r"PEXPIREAT renamed (.*?)"])
        await check_expire("renamed")
        # Check RENAMENX turns into DEL SET and PEXPIREAT
        await check_list_ooo("RENAMENX renamed renamekey", [r"DEL renamed", r"SET renamekey 1000", r"PEXPIREAT renamekey (.*?)"])
        await check_expire("renamekey")


"""
Test automatic replication of expiry.
"""

@pytest.mark.skip("fails locally")
@dfly_args({"proactor_threads": 4})
@pytest.mark.asyncio
async def test_expiry(df_local_factory, n_keys=1000):
    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(port=BASE_PORT+1, logtostdout=True)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

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
        c_master_db = aioredis.Redis(port=master.port, db=i)
        pipe = c_master_db.pipeline(transaction=is_multi)
        # Set simple keys n_keys..n_keys*2 on master
        start_key = n_keys*(i+1)
        end_key = start_key + n_keys
        batch_fill_data(client=pipe, gen=gen_test_data(
            end_key, start_key), batch_size=20)

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


@pytest.mark.skip(reason='Failing')
@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, num_ops, num_keys, num_par, flags", script_cases)
async def test_scripts(df_local_factory, t_master, t_replicas, num_ops, num_keys, num_par, flags):
    master = df_local_factory.create(
        port=BASE_PORT, proactor_threads=t_master)
    replicas = [df_local_factory.create(
        port=BASE_PORT+i+1, proactor_threads=t) for i, t in enumerate(t_replicas)]

    df_local_factory.start_all([master] + replicas)

    c_master = aioredis.Redis(port=master.port)
    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]
    for c_replica in c_replicas:
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        await wait_available_async(c_replica)

    script = script_test_s1.format(
        flags=f'#!lua flags={flags}' if flags else '')
    sha = await c_master.script_load(script)

    key_sets = [
        [f'{i}-{j}' for j in range(num_keys)] for i in range(num_par)
    ]

    rsps = await asyncio.gather(*(c_master.evalsha(sha, len(keys), *keys, num_ops) for keys in key_sets))
    assert rsps == [b'OK'] * num_par

    await check_all_replicas_finished(c_replicas, c_master)

    for c_replica in c_replicas:
        for key_set in key_sets:
            for j, k in enumerate(key_set):
                l = await c_replica.lrange(k, 0, -1)
                assert l == [f'{j}'.encode()] * num_ops

@pytest.mark.skip("fails locally")
@dfly_args({"proactor_threads": 4})
@pytest.mark.asyncio
async def test_auth_master(df_local_factory, n_keys=20):
    masterpass = 'requirepass'
    replicapass = 'replicapass'
    master = df_local_factory.create(port=BASE_PORT, requirepass=masterpass)
    replica = df_local_factory.create(
        port=BASE_PORT+1, logtostdout=True, masterauth=masterpass, requirepass=replicapass)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port, password=masterpass)
    c_replica = aioredis.Redis(port=replica.port, password=replicapass)

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

@pytest.mark.skip("fails locally")
@dfly_args({"proactor_threads": 2})
async def test_script_transfer(df_local_factory):
    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(port=BASE_PORT+1)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

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

@pytest.mark.skip("fails locally")
@dfly_args({"proactor_threads": 4})
@pytest.mark.asyncio
async def test_role_command(df_local_factory, n_keys=20):
    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(port=BASE_PORT+1, logtostdout=True)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

    assert await c_master.execute_command("role") == [b'master', []]
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    assert await c_master.execute_command("role") == [
        b'master', [[b'127.0.0.1', bytes(str(replica.port), 'ascii'), b'stable_sync']]]
    assert await c_replica.execute_command("role") == [
        b'replica', b'localhost', bytes(str(master.port), 'ascii'), b'stable_sync']

    # This tests that we react fast to socket shutdowns and don't hang on
    # things like the ACK or execution fibers.
    master.stop()
    await asyncio.sleep(0.1)
    assert await c_replica.execute_command("role") == [
        b'replica', b'localhost', bytes(str(master.port), 'ascii'), b'connecting']

    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


def parse_lag(replication_info: bytes):
    lags = re.findall(b"lag=([0-9]+)\r\n", replication_info)
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
        lag = (await inst.metrics())["dragonfly_connected_replica_lag_records"].samples[0].value
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


@pytest.mark.skip("fails locally"")
@dfly_args({"proactor_threads": 2})
@pytest.mark.asyncio
async def test_replication_info(df_local_factory, df_seeder_factory, n_keys=2000):
    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(
        port=BASE_PORT+1, logtostdout=True, replication_acks_interval=100)
    df_local_factory.start_all([master, replica])
    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

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

@pytest.mark.skip("seems to hang forever")
@pytest.mark.asyncio
async def test_flushall_in_full_sync(df_local_factory, df_seeder_factory):
    master = df_local_factory.create(
        port=BASE_PORT, proactor_threads=4, logtostdout=True)
    replica = df_local_factory.create(
        port=BASE_PORT+1, proactor_threads=2, logtostdout=True)

    # Start master
    master.start()
    c_master = aioredis.Redis(port=master.port)

    # Fill master with test data
    seeder = df_seeder_factory.create(
        port=master.port, keys=100_000, dbcount=1)
    await seeder.run(target_deviation=0.1)

    # Start replica
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    async def get_sync_mode(c_replica):
        result = await c_replica.execute_command("role")
        return result[3]

    async def is_full_sync_mode(c_replica):
        return await get_sync_mode(c_replica) == b'full_sync'

    # Wait for full sync to start
    while not await is_full_sync_mode(c_replica):
        await asyncio.sleep(0.0)

    syncid, _ = await c_replica.execute_command("DEBUG REPLICA OFFSET")

    # Issue FLUSHALL and push some more entries
    await c_master.execute_command("FLUSHALL")

    if not await is_full_sync_mode(c_replica):
        logging.error(
            "!!! Full sync finished too fast. Adjust test parameters !!!")
        return

    post_seeder = df_seeder_factory.create(
        port=master.port, keys=10, dbcount=1)
    await post_seeder.run(target_deviation=0.1)

    await check_all_replicas_finished([c_replica], c_master)

    await check_data(post_seeder, [replica], [c_replica])

    # Make sure that a new sync ID is present, meaning replication restarted following FLUSHALL.
    new_syncid, _ = await c_replica.execute_command("DEBUG REPLICA OFFSET")
    assert new_syncid != syncid

    await c_master.close()
    await c_replica.close()


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

@pytest.mark.skip("fails locally")
@pytest.mark.asyncio
async def test_readonly_script(df_local_factory):
    master = df_local_factory.create(
        port=BASE_PORT, proactor_threads=2, logtostdout=True)
    replica = df_local_factory.create(
        port=BASE_PORT+1, proactor_threads=2, logtostdout=True)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

    await c_master.set('WORKS', 'YES')

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    await c_replica.eval(READONLY_SCRIPT, 3, 'A', 'B', 'WORKS') == 'YES'

    try:
        await c_replica.eval(WRITE_SCRIPT, 1, 'A')
        assert False
    except aioredis.ResponseError as roe:
        assert 'READONLY ' in str(roe)


take_over_cases = [
    [2, 2],
    [2, 4],
    [4, 2],
    [8, 8],
]

@pytest.mark.skip("fails locally")
@pytest.mark.parametrize("master_threads, replica_threads", take_over_cases)
@pytest.mark.asyncio
async def test_take_over_counters(df_local_factory, master_threads, replica_threads):
    master = df_local_factory.create(proactor_threads=master_threads,
                                     port=BASE_PORT,
                                     #  vmodule="journal_slice=2,dflycmd=2,main_service=1",
                                     logtostderr=True)
    replica1 = df_local_factory.create(
        port=BASE_PORT+1, proactor_threads=replica_threads)
    replica2 = df_local_factory.create(
        port=BASE_PORT+2, proactor_threads=replica_threads)
    replica3 = df_local_factory.create(
        port=BASE_PORT+3, proactor_threads=replica_threads)
    df_local_factory.start_all([master, replica1, replica2, replica3])
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
        await asyncio.sleep(1)
        await c1.execute_command(f"REPLTAKEOVER 5")

    _, _, *results = await asyncio.gather(delayed_takeover(), block_during_takeover(), *[counter(f"key{i}") for i in range(16)])
    assert await c1.execute_command("role") == [b'master', []]

    for key, client_value in results:
        replicated_value = await c1.get(key)
        assert client_value == int(replicated_value)

    await disconnect_clients(c_master, c1, c_blocking, c2, c3)


@pytest.mark.skip("fails locally")
@pytest.mark.parametrize("master_threads, replica_threads", take_over_cases)
@pytest.mark.asyncio
async def test_take_over_seeder(df_local_factory, df_seeder_factory, master_threads, replica_threads):
    master = df_local_factory.create(proactor_threads=master_threads,
                                     port=BASE_PORT,
                                     dbfilename=f"dump_{master_threads}_{replica_threads}",
                                     logtostderr=True)
    replica = df_local_factory.create(
        port=BASE_PORT+1, proactor_threads=replica_threads)
    df_local_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(
        port=master.port, keys=1000, dbcount=5, stop_on_failure=False)

    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    async def seed():
        await seeder.run(target_ops=3000)

    fill_task = asyncio.create_task(seed())

    # Give the seeder a bit of time.
    await asyncio.sleep(1)
    await c_replica.execute_command(f"REPLTAKEOVER 5")
    seeder.stop()

    assert await c_replica.execute_command("role") == [b'master', []]

    # Need to wait a bit to give time to write the shutdown snapshot
    await asyncio.sleep(1)
    assert master.proc.poll() == 0, "Master process did not exit correctly."

    master.start()
    await wait_available_async(c_master)

    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)

    await disconnect_clients(c_master, c_replica)

@pytest.mark.skip(reason='Failing on github regression action')
@pytest.mark.asyncio
async def test_take_over_timeout(df_local_factory, df_seeder_factory):
    master = df_local_factory.create(proactor_threads=2,
                                     port=BASE_PORT,
                                     logtostderr=True)
    replica = df_local_factory.create(
        port=BASE_PORT+1, proactor_threads=2)
    df_local_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(
        port=master.port, keys=1000, dbcount=5, stop_on_failure=False)

    c_master = master.client()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)

    async def seed():
        await seeder.run(target_ops=3000)

    fill_task = asyncio.create_task(seed())

    # Give the seeder a bit of time.
    await asyncio.sleep(1)
    try:
        await c_replica.execute_command(f"REPLTAKEOVER 0.0001")
    except redis.exceptions.ResponseError as e:
        assert str(e) == "Couldn't execute takeover"
    else:
        assert False, "Takeover should not succeed."
    seeder.stop()
    await fill_task

    assert await c_master.execute_command("role") == [b'master', [[b'127.0.0.1', bytes(str(replica.port), 'ascii'), b'stable_sync']]]
    assert await c_replica.execute_command("role") == [b'replica', b'localhost', bytes(str(master.port), 'ascii'), b'stable_sync']

    await disconnect_clients(c_master, c_replica)


# 1. Number of master threads
# 2. Number of threads for each replica
replication_cases = [(8, 8)]

@pytest.mark.skip("fails locally")
@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replica", replication_cases)
async def test_no_tls_on_admin_port(df_local_factory, df_seeder_factory, t_master, t_replica, with_tls_args):
    # 1. Spin up dragonfly without tls, debug populate
    master = df_local_factory.create(admin_port=ADMIN_PORT, **with_tls_args, port=BASE_PORT, proactor_threads=t_master)
    master.start()
    c_master = aioredis.Redis(port=master.admin_port)
    await c_master.execute_command("DEBUG POPULATE 100")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_local_factory.create(admin_port=ADMIN_PORT + 1, **with_tls_args, port=BASE_PORT + 1, proactor_threads=t_replica)
    replica.start()
    c_replica = aioredis.Redis(port=replica.admin_port)
    res = await c_replica.execute_command("REPLICAOF localhost " + str(master.admin_port))
    assert b"OK" == res
    await check_all_replicas_finished([c_replica], c_master)

    # 3. Verify that replica dbsize == debug populate key size -- replication works
    db_size = await c_replica.execute_command("DBSIZE")
    assert 100 == db_size
