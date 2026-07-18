import asyncio
import logging
import os
import random
import signal
import time
from itertools import chain, repeat

import async_timeout
import pytest
import redis
from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstanceFactory, DflyInstance
from .seeder import DebugPopulateSeeder
from .seeder import Seeder as SeederV2
from .replication_utils import (
    ADMIN_PORT,
    get_metric_value,
    master_role_reply,
    replica_role_reply,
    start_replication,
    wait_for_replica_status,
)
from .utility import (
    assert_eventually,
    check_all_replicas_finished,
    DflySeederFactory,
    ExpirySeeder,
    info_tick_timer,
    tmp_file_name,
    wait_available_async,
    wait_for_replicas_state,
)

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

M_OPT = [pytest.mark.opt_only]
M_SLOW = [pytest.mark.large]


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


@pytest.mark.large
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


@pytest.mark.large
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


@pytest.mark.large
async def test_cancel_replication_immediately(df_factory, df_seeder_factory: DflySeederFactory):
    """
    Issue 100 replication commands. This checks that the replication state
    machine can handle cancellation well.
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
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        return True

    ping_job = asyncio.create_task(ping_status())
    replication_commands = [asyncio.create_task(replicate()) for _ in range(COMMANDS_TO_ISSUE)]

    num_successes = 0
    for result in asyncio.as_completed(replication_commands, timeout=80):
        num_successes += await result

    logging.info(f"succeses: {num_successes}")
    assert COMMANDS_TO_ISSUE == num_successes

    await wait_available_async(c_replica)
    capture = await seeder.capture()
    logging.info(f"number of items captured {len(capture)}")
    assert await seeder.compare(capture, replica.port)

    ping_job.cancel()

    replica.stop()
    lines = replica.find_in_logs("Stopping replication")
    # Cancelled 99 times by REPLICAOF command and once by Shutdown() because
    # we stopped the instance
    assert len(lines) == COMMANDS_TO_ISSUE


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


take_over_cases = [
    [2, 2],
    [2, 4],
    [4, 2],
    [8, 8],
]


@pytest.mark.exclude_epoll
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
            except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError):
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
        await c1.execute_command("REPLTAKEOVER 5")

    _, _, *results = await asyncio.gather(
        delayed_takeover(), block_during_takeover(), *[counter(f"key{i}") for i in range(16)]
    )
    assert await c1.execute_command("role") == master_role_reply([])

    for key, client_value in results:
        replicated_value = await c1.get(key)
        assert client_value == int(replicated_value)


@pytest.mark.exclude_epoll
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

    await start_replication(c_replica, master.admin_port)

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
    await c_replica.execute_command("REPLTAKEOVER 30 SAVE")
    logging.debug("after running repltakover")
    seeder.stop()
    await fill_task

    assert await c_replica.execute_command("role") == master_role_reply([])
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

    c_master = master.client(socket_timeout=1, socket_connect_timeout=1)
    await c_master.execute_command("SET foo bar")

    c_replica = replica.client()
    await start_replication(c_replica, master.port)

    async def prompt():
        client = replica.client()
        master_alive = True
        for i in range(10):
            # TODO remove try block when we no longer shut down master after take over
            if master_alive:
                try:
                    res = await c_master.execute_command("GET foo")
                    assert res == "bar"
                    res = await c_master.execute_command("CONFIG SET aclfile myfile")
                    assert res == "OK"
                except:
                    master_alive = False
            res = await client.execute_command("GET foo")
            assert res == "bar"

    promt_task = asyncio.create_task(prompt())
    await c_replica.execute_command("REPLTAKEOVER 5")

    assert await c_replica.execute_command("role") == master_role_reply([])
    await promt_task


async def test_take_over_timeout(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)
    df_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(port=master.port, keys=1000, dbcount=5, stop_on_failure=False)

    c_master = master.client()
    c_replica = replica.client()

    logging.debug(f"PORTS ARE:  {master.port} {replica.port}")

    await start_replication(c_replica, master.port)

    fill_task = asyncio.create_task(seeder.run(target_ops=3000))

    # Give the seeder a bit of time.
    await asyncio.sleep(1)
    try:
        await c_replica.execute_command("REPLTAKEOVER 0")
    except redis.exceptions.ResponseError as e:
        # Should fail with detailed error message
        assert str(e).startswith("Couldn't execute takeover")
        # Verify it includes diagnostic information
        assert ":" in str(e), "Error message should include diagnostic details"
    else:
        assert False, "Takeover should not succeed."
    seeder.stop()
    await fill_task

    assert await c_master.execute_command("role") == master_role_reply(replica)
    assert await c_replica.execute_command("role") == replica_role_reply(master)


@pytest.mark.parametrize(
    "master_threads, backlog_len, stream_during, stream_target_ops, num_drops, sleep_range, check_stale_log",
    [
        (6, None, False, None, 10, (0, 10), False),
        (4, 4000, True, 4000, 3, (10, 20), False),
        (4, 1, True, None, 3, (5, 10), True),
    ],
)
async def test_network_disconnect(
    df_factory,
    df_seeder_factory,
    master_threads,
    backlog_len,
    stream_during,
    stream_target_ops,
    num_drops,
    sleep_range,
    check_stale_log,
    proxy_factory,
):
    master_kwargs = dict(proactor_threads=master_threads)
    if backlog_len is not None:
        master_kwargs["shard_repl_backlog_len"] = backlog_len
    master = df_factory.create(**master_kwargs)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([replica, master])
    seeder = df_seeder_factory.create(port=master.port)

    async with replica.client() as c_replica, master.client() as c_master:
        await seeder.run(target_deviation=0.1)

        proxy = await proxy_factory(master.port)
        await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

        if check_stale_log:
            # Wait for the two nodes to be in sync (stable state replication) before seeding
            await wait_available_async(c_replica)

        fill_task = None
        if stream_during:
            if stream_target_ops is not None:
                fill_task = asyncio.create_task(seeder.run(target_ops=stream_target_ops))
            else:
                fill_task = asyncio.create_task(seeder.run())

        for _ in range(num_drops):
            await asyncio.sleep(random.randint(*sleep_range) / 10)
            proxy.drop_connection()

        if fill_task is not None:
            seeder.stop()
            await fill_task

        # Give time to detect dropped connection and reconnect
        await asyncio.sleep(1.0)
        await wait_available_async(c_replica)

        logging.debug(await c_replica.execute_command("INFO REPLICATION"))
        logging.debug(await c_master.execute_command("INFO REPLICATION"))

        capture = await seeder.capture()
        assert await seeder.compare(capture, replica.port)

    if check_stale_log:
        master.stop()
        lines = master.find_in_logs("Partial sync requested from stale LSN")
        assert len(lines) > 0


async def test_replica_reconnections_after_network_disconnect(
    df_factory, df_seeder_factory, proxy_factory
):
    master = df_factory.create(proactor_threads=6)
    replica = df_factory.create(proactor_threads=4)

    df_factory.start_all([replica, master])
    seeder = df_seeder_factory.create(port=master.port)

    async with replica.client() as c_replica:
        await seeder.run(target_deviation=0.1)

        proxy = await proxy_factory(master.port)
        await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")

        # Wait replica to be up and synchronized with master
        await wait_available_async(c_replica)

        initial_reconnects_count = await get_replica_reconnects_count(replica)

        # Fully drop the server
        await proxy.close()

        # After dropping the connection replica should try to reconnect
        await wait_for_replica_status(c_replica, status="down")
        await asyncio.sleep(2)

        # Restart the proxy
        await proxy.start_serving()

        # Wait replica to be reconnected and synchronized with master
        await wait_available_async(c_replica)

        capture = await seeder.capture()
        assert await seeder.compare(capture, replica.port)

        # Assert replica reconnects metrics increased
        await assert_replica_reconnections(replica, initial_reconnects_count)


@pytest.mark.debug_only
@dfly_args({"proactor_threads": 2})
async def test_replicaof_reject_on_load(df_factory, df_seeder_factory):
    master = df_factory.create()
    replica = df_factory.create(dbfilename=f"dump_{tmp_file_name()}")
    df_factory.start_all([master, replica])

    c_replica = replica.client()

    await c_replica.execute_command("DEBUG POPULATE 1000 key 500 RAND type set elements 500")

    replica.stop()
    replica.start()
    # Disable retries so that BusyLoadingError is raised immediately.
    # redis-py >= 7 retries on ConnectionError by default, and BusyLoadingError
    # inherits from ConnectionError, causing the REPLICAOF to be silently
    # retried until loading finishes.
    from redis.retry import Retry
    from redis.backoff import NoBackoff

    c_replica = replica.client(retry=Retry(NoBackoff(), 0))

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
    await start_replication(c_replica, master.port)
    assert (await c_replica.info("REPLICATION"))["master_link_status"] == "up"

    # kill existing master, create master with different repl_id but same port
    master_port = master.port
    master.stop()

    await asyncio.sleep(1)

    repl_info = await c_replica.info("REPLICATION")
    assert repl_info["master_link_status"] == "down", str(repl_info)

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
    await start_replication(c_replica, master.port)
    assert await c_replica.execute_command("get k") == "6789"


@pytest.mark.parametrize(
    "with_expiry_seeder",
    [
        False,
        pytest.param(True, marks=pytest.mark.large),
    ],
)
async def test_replication_timeout_on_full_sync(
    df_factory: DflyInstanceFactory, df_seeder_factory, with_expiry_seeder
):
    if with_expiry_seeder:
        # Timeout set to 3 seconds because we must first saturate the socket such that subsequent
        # writes block. Otherwise, we will break the flows before Heartbeat actually deadlocks.
        master = df_factory.create(
            proactor_threads=2, replication_timeout=3000, vmodule="replica=2,dflycmd=2"
        )
    else:
        # setting replication_timeout to a very small value to force the replica to timeout
        master = df_factory.create(
            replication_timeout=100, vmodule="replica=2,dflycmd=2,snapshot=1,rdb_save=1,rdb_load=1"
        )
    replica = df_factory.create()

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    if with_expiry_seeder:
        await c_master.execute_command("debug", "populate", "100000", "foo", "5000", "RAND")

        c_master = master.client()
        c_replica = replica.client()

        expiry_seeder = ExpirySeeder()
        expiry_seeder_task = asyncio.create_task(expiry_seeder.run(c_master))
        await expiry_seeder.wait_until_n_inserts(50000)
        expiry_seeder.stop()
        await expiry_seeder_task
    else:
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

    # Dragonfly would get stuck here (with expiry seeder) without the bug fix. When replica does
    # not read from the socket, Heartbeat() will block on the journal write for the expired items
    # and shard_handler would never be called and break replication. More details on #3936.
    pause_duration = 6 if with_expiry_seeder else 1
    await asyncio.sleep(pause_duration)

    await c_replica.execute_command("debug replica resume")  # resume replication

    await asyncio.sleep(1)  # replica will start resync

    if not with_expiry_seeder:
        seeder.stop()
        await seeder_task

    await check_all_replicas_finished([c_replica], c_master, timeout=60)
    await assert_replica_reconnections(replica, 0)


@pytest.mark.exclude_epoll
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


async def test_double_take_over(df_factory, df_seeder_factory):
    master = df_factory.create(proactor_threads=4, dbfilename="", admin_port=ADMIN_PORT)
    replica = df_factory.create(proactor_threads=4, dbfilename="", admin_port=ADMIN_PORT + 1)
    df_factory.start_all([master, replica])

    seeder = df_seeder_factory.create(port=master.port, keys=1000, dbcount=5, stop_on_failure=False)
    await seeder.run(target_deviation=0.1)

    capture = await seeder.capture(port=master.port)

    c_replica = replica.client()

    logging.debug("start replication")
    await start_replication(c_replica, master.admin_port)

    logging.debug("running repltakover")
    await c_replica.execute_command("REPLTAKEOVER 10")
    assert await c_replica.execute_command("role") == master_role_reply([])

    @assert_eventually
    async def check_master_status():
        assert master.proc.poll() == 0, "Master process did not exit correctly."

    await check_master_status()

    logging.debug("restart previous master")
    master.start()
    c_master = master.client()

    logging.debug("start second replication")
    await start_replication(c_master, replica.admin_port)

    logging.debug("running second repltakover")
    await c_master.execute_command("REPLTAKEOVER 10")
    assert await c_master.execute_command("role") == master_role_reply([])

    assert await seeder.compare(capture, port=master.port)


async def test_replica_of_replica(df_factory):
    # Can't connect a replica to a replica, but OK to connect 2 replicas to the same master.
    # Cascaded replication is only allowed behind --experimental_cascaded_partial_sync (off here).
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


@pytest.mark.parametrize(
    "use_takeover, backlog_len",
    [(False, 2), (False, 1), (True, 1), (True, 10)],
)
async def test_partial_replication_on_same_source_master(df_factory, use_takeover, backlog_len):
    master = df_factory.create()
    replica1 = df_factory.create(shard_repl_backlog_len=backlog_len)
    replica2 = df_factory.create()

    df_factory.start_all([master, replica1, replica2])
    c_master = master.client()
    c_replica1 = replica1.client()
    c_replica2 = replica2.client()

    logging.debug("Fill master with test data")
    seeder = DebugPopulateSeeder(key_target=50)
    await seeder.run(c_master)

    logging.debug("Start replication and wait for full sync")
    await c_replica1.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica1)
    await c_replica2.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica2)

    # Send some traffic
    seeder = SeederV2(key_target=8_000)
    await seeder.run(c_master, target_deviation=0.01)

    # Wait for all journal changes propagate to replicas
    await check_all_replicas_finished([c_replica1, c_replica2], c_master)

    if use_takeover:
        # Promote first replica to master
        await c_replica1.execute_command("REPLTAKEOVER 5")
        if backlog_len > 1:
            await c_replica1.execute_command("SET bar foo")
            await c_replica1.execute_command("SET foo bar")

    else:
        # Promote first replica to master
        await c_replica1.execute_command("REPLICAOF NO ONE")
        await c_master.set("x", "y")
        await c_master.set("x", "y")
        await check_all_replicas_finished([c_replica2], c_master)

    # Start replication with new master
    await c_replica2.execute_command(f"REPLICAOF localhost {replica1.port}")

    await check_all_replicas_finished([c_replica2], c_replica1)
    # Validate data
    if use_takeover:
        hash1, hash2 = await asyncio.gather(
            *(SeederV2.capture(c) for c in (c_replica1, c_replica2))
        )
        assert hash1 == hash2
        s1 = await c_replica1.execute_command("dbsize")
        s2 = await c_replica1.execute_command("dbsize")
        assert s1 == s2

    # Check we can takeover to the second replica
    await c_replica2.execute_command("REPLTAKEOVER 5")

    replica1.stop()
    replica2.stop()
    if use_takeover:
        # Check logs for partial replication
        lines = replica2.find_in_logs(f"Started partial sync with localhost:{replica1.port}")
        assert len(lines) == 1
        # Check no full sync logs
        lines = replica2.find_in_logs(f"Started full sync with localhost:{replica1.port}")
        assert len(lines) == 0
    else:
        lines = replica2.find_in_logs(f"Started full sync with localhost:{replica1.port}")
        assert len(lines) == 1
        # No partial sync after NO ONE
        lines = replica2.find_in_logs(f"Started partial sync with localhost:{replica1.port}")
        assert len(lines) == 0


async def test_partial_replication_on_same_source_master_with_replica_lsn_inc(df_factory):
    server1 = df_factory.create()
    server2 = df_factory.create()
    server3 = df_factory.create()
    server4 = df_factory.create()

    df_factory.start_all([server1, server2, server3, server4])
    c_s2 = server2.client()
    c_s3 = server3.client()
    c_s4 = server4.client()

    logging.debug("Start replication and wait for full sync")
    await c_s2.execute_command(f"REPLICAOF localhost {server1.port}")
    await wait_for_replicas_state(c_s2)
    await c_s3.execute_command(f"REPLICAOF localhost {server1.port}")
    await wait_for_replicas_state(c_s3)

    # Promote server 2 to master
    await c_s2.execute_command("REPLTAKEOVER 20")
    # Make server 4 replica of server 2
    await c_s4.execute_command(f"REPLICAOF localhost {server2.port}")
    # Send some write command for lsn inc
    # NOTE: using append temporarily because SET is omit-optimized with disables partial sync
    for i in range(100):
        await c_s2.append(i, "val")
    # Make server 3 replica of server 2
    await c_s3.execute_command(f"REPLICAOF localhost {server2.port}")

    await check_all_replicas_finished([c_s3], c_s2)
    await check_all_replicas_finished([c_s4], c_s2)

    s2_sz = await c_s2.dbsize()
    s3_sz = await c_s3.dbsize()
    assert s2_sz == 100
    assert s2_sz == s3_sz

    s4_sz = await c_s4.dbsize()
    assert s3_sz == s4_sz

    server3.stop()
    # Check logs for partial replication
    lines = server3.find_in_logs(f"Started partial sync with localhost:{server2.port}")
    assert len(lines) == 1


@pytest.mark.parametrize("reconnect_to", [0, 1])
async def test_cascaded_partial_sync(df_factory, reconnect_to):
    """
    Cascaded replication (master -> r1 -> ... -> rn): the last replica reconnects to node
    `reconnect_to` in the chain and should partial sync instead of full sync. reconnect_to=0 is
    the lineage root master; any other index is an intermediate node, which works too because
    every node in the chain shares the lineage root's LSN space.
    """
    flag = {"proactor_threads": 4, "experimental_cascaded_partial_sync": None}
    instances = [df_factory.create(**flag) for i in range(4)]
    df_factory.start_all(instances)

    clients = [i.client() for i in instances]

    # Fill master with test data
    seeder = DebugPopulateSeeder(key_target=50)
    c_master = clients[0]
    await seeder.run(c_master)

    # Chain replication
    for i in range(1, len(instances)):
        await clients[i].execute_command(f"REPLICAOF localhost {instances[i-1].port}")
        # Wait for this link to reach stable sync before chaining the next replica, otherwise the
        # next replica may connect while this one is still LOADING and abort its handshake.
        await wait_for_replicas_state(clients[i])

    # Generate journal traffic that lands in the partial-sync buffer.
    # NOTE: append (not set) because SET is omit-optimized which disables partial sync.
    for i in range(100):
        await c_master.append(f"k{i}", "val")

    for i in range(1, len(instances)):
        await check_all_replicas_finished([clients[i]], clients[i - 1])

    # The last instance reconnects to the target node
    target = instances[reconnect_to]
    await clients[-1].execute_command(f"REPLICAOF localhost {target.port}")
    await check_all_replicas_finished([clients[-1]], clients[reconnect_to])

    # Further writes originating at the master should keep flowing down to the last replica.
    for i in range(100, 150):
        await c_master.append(f"k{i}", "val")
    if reconnect_to != 0:
        await check_all_replicas_finished([clients[reconnect_to]], c_master)
    await check_all_replicas_finished([clients[-1]], clients[reconnect_to])

    # Validate data consistency.
    master_hash, last_hash = await asyncio.gather(
        *(SeederV2.capture(c) for c in (c_master, clients[-1]))
    )
    assert master_hash == last_hash

    info = await clients[-1].info("replication")
    assert info["psync_successes"] == 1

    instances[-1].stop()
    lines = instances[-1].find_in_logs(f"Started partial sync with localhost:{target.port}")
    assert len(lines) == 1
    lines = instances[-1].find_in_logs(f"Started full sync with localhost:{target.port}")
    assert len(lines) == 0


async def test_cascaded_full_sync_on_master_switch(df_factory):
    """Transitive re-sync when an intermediate node switches to a new, independent master.

    Chain master -> r1 -> r2 -> r3. r1 then does REPLICAOF m2 where m2 is an independent master
    holding a different dataset. r1 full syncs m2 (replacing its data outside its journal), which
    must force r2 to full sync r1, which in turn forces r3 to full sync r2. All downstream replicas
    must converge to m2's dataset.
    """
    flag = {"proactor_threads": 4, "experimental_cascaded_partial_sync": None}
    master, r1, r2, r3, m2 = (df_factory.create(**flag) for _ in range(5))
    df_factory.start_all([master, r1, r2, r3, m2])

    c_master, c_r1, c_r2, c_r3, c_m2 = (
        master.client(),
        r1.client(),
        r2.client(),
        r3.client(),
        m2.client(),
    )

    # master and m2 hold clearly different datasets.
    for i in range(100):
        await c_master.append(f"k{i}", "master")
    for i in range(100):
        await c_m2.append(f"k{i}", "M2_DIFFERENT")

    # Build chain master -> r1 -> r2 -> r3.
    for c, upstream in ((c_r1, master), (c_r2, r1), (c_r3, r2)):
        await c.execute_command(f"REPLICAOF localhost {upstream.port}")
        await wait_for_replicas_state(c)
    await check_all_replicas_finished([c_r1], c_master)
    await check_all_replicas_finished([c_r2], c_r1)
    await check_all_replicas_finished([c_r3], c_r2)

    master_hash, r3_hash = await asyncio.gather(SeederV2.capture(c_master), SeederV2.capture(c_r3))
    assert master_hash == r3_hash

    # r1 switches to the independent master m2.
    await c_r1.execute_command(f"REPLICAOF localhost {m2.port}")
    await wait_for_replicas_state(c_r1)
    await check_all_replicas_finished([c_r1], c_m2)

    # Downstream replicas must transitively full sync and converge to m2's dataset.
    await check_all_replicas_finished([c_r2], c_r1)
    await check_all_replicas_finished([c_r3], c_r2)

    m2_hash, r1_hash, r2_hash, r3_hash = await asyncio.gather(
        SeederV2.capture(c_m2),
        SeederV2.capture(c_r1),
        SeederV2.capture(c_r2),
        SeederV2.capture(c_r3),
    )
    assert r1_hash == m2_hash, "r1 did not converge to m2"
    assert r2_hash == m2_hash, "r2 did not transitively converge to m2"
    assert r3_hash == m2_hash, "r3 did not transitively converge to m2"


@pytest.mark.xfail(
    strict=True,
    reason="Master rotation not implemented yet",
)
async def test_cascaded_partial_sync_split_brain(df_factory):
    """Split-brain safety: master -> r1 -> r2, then r1 is promoted to standalone and
    BOTH master and r1 take independent writes to the same key. r2 (following r1) then reconnects
    directly to master.
    """
    flag = {"proactor_threads": 4, "experimental_cascaded_partial_sync": None}
    master, r1, r2 = (df_factory.create(**flag) for _ in range(3))
    df_factory.start_all([master, r1, r2])

    c_master, c_r1, c_r2 = master.client(), r1.client(), r2.client()

    # Base data + a shared key that both branches will later diverge on.
    # NOTE: append (not set) because SET is omit-optimized which disables partial sync.
    for i in range(50):
        await c_master.append(f"k{i}", "val")
    await c_master.append("diverge", "base")

    # Build chain master -> r1 -> r2.
    await c_r1.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_r1)
    await c_r2.execute_command(f"REPLICAOF localhost {r1.port}")
    await wait_for_replicas_state(c_r2)
    await check_all_replicas_finished([c_r1], c_master)
    await check_all_replicas_finished([c_r2], c_r1)

    # r1 splits from master and both sides write divergent values to the same key in the shared
    # LSN space.
    await c_r1.execute_command("REPLICAOF NO ONE")
    await c_master.append("diverge", "_MASTER")
    await c_r1.append("diverge", "_R1")
    await check_all_replicas_finished([c_r2], c_r1)

    assert (await c_master.get("diverge")) == "base_MASTER"
    assert (await c_r2.get("diverge")) == "base_R1"

    # r2 reconnects directly to master. It must converge to master's authoritative value.
    await c_r2.execute_command(f"REPLICAOF localhost {master.port}")
    await check_all_replicas_finished([c_r2], c_master)

    master_val, r2_val = await asyncio.gather(c_master.get("diverge"), c_r2.get("diverge"))
    assert r2_val == master_val, f"split-brain divergence: master={master_val!r} r2={r2_val!r}"


@pytest.mark.parametrize("proactors", [1, 4, 6])
@pytest.mark.parametrize("backlog_len", [1, 256, 1024, 1300])
async def test_partial_sync(df_factory, proactors, backlog_len, proxy_factory):
    keys = 5_000
    if proactors > 1:
        keys = 10_000

    # We use lock_on_hashtag because we want to seed enough elements to one flow/journal such that
    # the partial sync stales.
    master = df_factory.create(
        proactor_threads=proactors, shard_repl_backlog_len=backlog_len, lock_on_hashtags=True
    )
    replica = df_factory.create(proactor_threads=proactors)

    df_factory.start_all([replica, master])

    async def stream(client, total):
        for i in range(0, total):
            prefix = "{prefix}"
            # Seed to one shard only. This will eventually cause one of the flows to become stale.
            await client.execute_command(f"SET {prefix}foo{i} bar{i}")

    async with replica.client() as c_replica, master.client() as c_master:
        seeder = SeederV2(key_target=keys)
        await seeder.run(c_master, target_deviation=0.01)

        proxy = await proxy_factory(master.port)
        await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")
        # Reach stable sync
        await wait_for_replicas_state(c_replica)
        # Stream some elements
        await stream(c_master, backlog_len)

        proxy.drop_connection()
        # Give time to detect dropped connection and reconnect
        await asyncio.sleep(1.0)
        # Partial synced here
        await check_all_replicas_finished([c_replica], c_master)
        hash1, hash2 = await asyncio.gather(*(SeederV2.capture(c) for c in (c_master, c_replica)))
        assert hash1 == hash2

        await proxy.close()
        # Whoops we moved too much, no partial sync here
        await stream(c_master, backlog_len + 10)
        await proxy.start_serving()
        await asyncio.sleep(1.0)

        await check_all_replicas_finished([c_replica], c_master)

        hash1, hash2 = await asyncio.gather(*(SeederV2.capture(c) for c in (c_master, c_replica)))
        assert hash1 == hash2

    master.stop()
    replica.stop()
    # Partial sync worked
    lines = master.find_in_logs("Partial sync requested from LSN")
    # Because we run with num_shards = proactors - 1
    total_attempts = 1
    if proactors > 1:
        total_attempts = proactors - 1 + proactors - 2
    assert len(lines) == total_attempts
    # Second partial sync failed because of stale LSN
    lines = master.find_in_logs("Partial sync requested from stale LSN")
    assert len(lines) == 1


@pytest.mark.large
async def test_takeover_bug_wrong_replica_checked_in_logs(df_factory):
    master = df_factory.create(proactor_threads=4, vmodule="dflycmd=1")
    replicas = [df_factory.create(proactor_threads=2) for _ in range(3)]
    df_factory.start_all([master] + replicas)

    c_master = master.client()
    clients = [r.client() for r in replicas]

    # Connect all replicas
    for c in clients:
        await c.execute_command(f"REPLICAOF localhost {master.port}")
    await asyncio.gather(*[wait_available_async(c) for c in clients])

    # Disconnect replica[1] to create lag
    await clients[1].execute_command("REPLICAOF NO ONE")

    # Write data that replica[1] will miss
    pipe = c_master.pipeline()
    for i in range(10000):
        pipe.set(f"k{i}", "x" * 100)
    await pipe.execute()

    # Reconnect replica[1] and immediately takeover from replica[0]
    await clients[1].execute_command(f"REPLICAOF localhost {master.port}")

    await check_all_replicas_finished(clients, c_master)

    await clients[0].execute_command("REPLTAKEOVER 10")

    # Check master logs
    master.stop(kill=False)

    timeout_logs = master.find_in_logs(
        f"Couldn't synchronize with replica for takeover in time: 127.0.0.1:{replicas[0].port}"
    )
    assert not timeout_logs


@pytest.mark.large
async def test_takeover_timeout_on_unresponsive_master(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=2)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Setup replication
    await start_replication(c_replica, master.port)

    # Write some data
    for i in range(10):
        await c_master.set(f"key{i}", f"val{i}")
    await asyncio.sleep(0.2)

    # PAUSE master process (SIGSTOP) - socket stays open but doesn't respondExpand commentComment on line R3629Code has comments. Press enter to view.
    os.kill(master.proc.pid, signal.SIGSTOP)
    logging.info(f"Paused master process {master.proc.pid}")

    # Try takeover with 5 second timeout
    # BUG: This will hang forever because SendNextPhaseRequest has no timeout
    # FIXED: Should return error within ~15 seconds (5 + buffer)
    start_time = time.time()
    try:
        await asyncio.wait_for(
            c_replica.execute_command("REPLTAKEOVER 5"),
            timeout=20,  # Should complete within 20 seconds
        )
        elapsed = time.time() - start_time
        logging.info(f"Takeover completed in {elapsed:.1f}s")
    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        pytest.fail(
            f"BUG: REPLTAKEOVER hung for {elapsed:.1f}s without timeout. "
            "SendNextPhaseRequest in replica.cc has no socket timeout."
        )
    except Exception as e:
        # Expected: connection error or timeout error
        elapsed = time.time() - start_time
        logging.info(f"Takeover failed after {elapsed:.1f}s: {e}")
        # Should fail quickly, not hang
        assert elapsed < 20, f"Took too long: {elapsed:.1f}s"
    finally:
        # Resume master so it can be stopped properly
        try:
            os.kill(master.proc.pid, signal.SIGCONT)
        except Exception:
            pass


async def test_partial_sync_with_different_shard_sizes(df_factory):
    master = df_factory.create(proactor_threads=3)
    replica1 = df_factory.create(proactor_threads=4)
    replica2 = df_factory.create(proactor_threads=5)
    replica3 = df_factory.create(proactor_threads=6)

    df_factory.start_all([replica1, replica2, replica3, master])

    c_replica1 = replica1.client()
    c_replica2 = replica2.client()
    c_replica3 = replica3.client()

    c_master = master.client()

    await c_master.execute_command("debug populate 5000")

    await c_replica1.execute_command(f"replicaof localhost {master.port}")
    await c_replica2.execute_command(f"replicaof localhost {master.port}")
    await c_replica3.execute_command(f"replicaof localhost {master.port}")

    seeder = SeederV2(key_target=100)
    await seeder.run(c_master, target_deviation=0.01)

    await check_all_replicas_finished([c_replica1, c_replica2, c_replica3], c_master)

    await c_replica1.execute_command("repltakeover 5")
    await c_replica2.execute_command(f"replicaof localhost {replica1.port}")
    await c_replica3.execute_command(f"replicaof localhost {replica1.port}")

    await check_all_replicas_finished([c_replica2, c_replica3], c_replica1)

    for replica in (replica1, replica2, replica3):
        replica.stop()

    lines = replica2.find_in_logs(f"Started partial sync with localhost:{replica1.port}")
    assert len(lines) == 0
    lines = replica3.find_in_logs(f"Started partial sync with localhost:{replica1.port}")
    assert len(lines) == 0


@pytest.mark.large
async def test_replica_reconnection_leaks_connections(df_factory: DflyInstanceFactory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    info = await c_master.info("clients")
    baseline = info["connected_clients"]

    num_cycles = 20
    for _ in range(num_cycles):
        await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        await wait_for_replicas_state(c_replica)
        await c_replica.execute_command("REPLICAOF NO ONE")

    # Wait for connected_clients to stabilize (stop changing)
    prev = None
    async for info, breaker in info_tick_timer(c_master, "clients", timeout=10):
        with breaker:
            curr = info["connected_clients"]
            assert curr == prev
        prev = curr

    leaked = prev - baseline
    assert leaked == 0, f"connected_clients leaked {leaked} after {num_cycles} reconnect cycles"

    await c_master.aclose()
    await c_replica.aclose()


@dfly_args({"proactor_threads": 4})
@pytest.mark.timeout(60)
async def test_replica_no_deadlock_on_disconnect(df_factory: DflyInstanceFactory):
    """Replica must not deadlock when the master dies while a global command
    (FLUSHALL) is being replicated across shards. The bug: one shard reads
    the FLUSHALL from its socket buffer and enters Barrier::Wait inside
    ExecuteTx, but CancelAllBlockingEntities already ran (triggered by
    another shard whose socket broke first), so the new entry is never
    cancelled and the shard fiber hangs forever."""
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("DEBUG", "POPULATE", "1000")
    await c_replica.execute_command("REPLICAOF", "localhost", str(master.port))
    await wait_available_async(c_replica)
    await check_all_replicas_finished([c_replica], c_master)

    # Stream Lua-based multi-shard traffic that generates global commands,
    # then kill the master. The seeder's Lua scripts create complex multi-shard
    # operations that widen the race window for the barrier deadlock.
    stream_seeder = SeederV2(key_target=1000)
    seed_task = asyncio.create_task(stream_seeder.run(c_master, target_deviation=0.1))
    await asyncio.sleep(0.5)
    await c_master.execute_command("FLUSHALL")
    master.stop(kill=True)
    seed_task.cancel()
    try:
        await seed_task
    except (asyncio.CancelledError, Exception):
        pass

    # Restart master and re-point the replica.
    master.start()
    c_master = master.client()
    await wait_available_async(c_master)
    await c_master.execute_command("DEBUG", "POPULATE", "500")

    # Re-point replica to the new master port. If the replica is deadlocked
    # in an ExecuteTx barrier, even this command won't help it recover.
    await c_replica.execute_command("REPLICAOF", "localhost", str(master.port))

    await asyncio.wait_for(wait_for_replicas_state(c_replica), timeout=20)
    await check_all_replicas_finished([c_replica], c_master, timeout=20)

    assert await c_replica.dbsize() == await c_master.dbsize()
    await c_replica.execute_command("REPLICAOF", "NO", "ONE")
