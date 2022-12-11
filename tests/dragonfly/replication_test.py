
import pytest
import asyncio
import aioredis
import random
from itertools import count, chain, repeat

from .utility import *
from . import dfly_args


BASE_PORT = 1111

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

"""
Test full replication pipeline. Test full sync with streaming changes and stable state streaming.
"""

# 1. Number of master threads
# 2. Number of threads for each replica
# 3. Number of keys stored and sent in full sync
# 4. Number of keys overwritten during full sync
replication_cases = [
    (8, [8], 20000, 5000),
    (8, [8], 10000, 10000),
    (8, [2, 2, 2, 2], 20000, 5000),
    (6, [6, 6, 6], 30000, 15000),
    (4, [1] * 12, 10000, 4000),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, n_keys, n_stream_keys", replication_cases)
async def test_replication_all(df_local_factory, t_master, t_replicas, n_keys, n_stream_keys):
    master = df_local_factory.create(port=1111, proactor_threads=t_master)
    replicas = [
        df_local_factory.create(port=BASE_PORT+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Start master and fill with test data
    master.start()
    c_master = aioredis.Redis(port=master.port)
    await batch_fill_data_async(c_master, gen_test_data(n_keys, seed=1))

    # Start replicas
    for replica in replicas:
        replica.start()

    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]

    async def stream_data():
        """ Stream data during stable state replication phase and afterwards """
        gen = gen_test_data(n_stream_keys, seed=2)
        for chunk in grouper(3, gen):
            await c_master.mset({k: v for k, v in chunk})

    async def run_replication(c_replica):
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))

    async def check_replication(c_replica):
        """ Check that static and streamed data arrived """
        await wait_available_async(c_replica)
        # Check range [n_stream_keys, n_keys] is of seed 1
        await batch_check_data_async(c_replica, gen_test_data(n_keys, start=n_stream_keys, seed=1))
        # Check range [0, n_stream_keys] is of seed 2
        await asyncio.sleep(0.2)
        await batch_check_data_async(c_replica, gen_test_data(n_stream_keys, seed=2))

    # Start streaming data and run REPLICAOF in parallel
    stream_fut = asyncio.create_task(stream_data())
    await asyncio.gather(*(asyncio.create_task(run_replication(c))
                           for c in c_replicas))

    assert not stream_fut.done(
    ), "Weak testcase. Increase number of streamed keys to surpass full sync"
    await stream_fut

    # Check full sync results
    await asyncio.gather(*(check_replication(c) for c in c_replicas))

    # Check stable state streaming
    await batch_fill_data_async(c_master, gen_test_data(n_keys, seed=3))

    await asyncio.sleep(0.5)
    await asyncio.gather(*(batch_check_data_async(c, gen_test_data(n_keys, seed=3))
                           for c in c_replicas))


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
    (8, [4, 4], [4, 4], [4], 10000),
    (8, [2] * 6, [2] * 6, [2, 2], 10000),
    # full sync heavy
    (8, [4] * 6, [], [], 10000),
    (8, [2] * 12, [], [], 10000),
    # stable state heavy
    (8, [], [4] * 6, [], 10000),
    (8, [], [2] * 12, [], 10000),
    # disconnect only
    (8, [], [], [2] * 6, 10000)
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_crash_fs, t_crash_ss, t_disonnect, n_keys", disconnect_cases)
async def test_disconnect_replica(df_local_factory, t_master, t_crash_fs, t_crash_ss, t_disonnect, n_keys):
    master = df_local_factory.create(port=BASE_PORT, proactor_threads=t_master)
    replicas = [
        (df_local_factory.create(
            port=BASE_PORT+i+1, proactor_threads=t), crash_fs)
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
    for replica, _ in replicas:
        replica.start()

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
    async def fill_loop():
        local_c = aioredis.Redis(
            port=master.port, single_connection_client=True)
        for seed in count(1):
            await batch_fill_data_async(local_c, gen_test_data(n_keys, seed=seed))

    fill_task = asyncio.create_task(fill_loop())

    # Run full sync
    async def full_sync(replica, c_replica, crash_type):
        c_replica = aioredis.Redis(port=replica.port)
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
        if crash_type == 0:
            await asyncio.sleep(random.random()/100+0.01)
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
        replica.stop(kill=True)

    await asyncio.gather(*(stable_sync(*args) for args
                           in replicas_of_type(lambda t: t == 1)))

    # Check master survived all crashes
    assert await c_master.ping()

    # Check phase 3 replica survived
    for _, c_replica, _ in replicas_of_type(lambda t: t > 1):
        assert await c_replica.ping()

    # Stop streaming
    fill_task.cancel()

    # Check master survived all crashes
    assert await c_master.ping()

    # Check phase 3 replicas are up-to-date and there is no gap or lag
    def check_gen(): return gen_test_data(n_keys//5, seed=0)

    await batch_fill_data_async(c_master, check_gen())
    await asyncio.sleep(1.0)
    for _, c_replica, _ in replicas_of_type(lambda t: t > 1):
        await batch_check_data_async(c_replica, check_gen())

    # Check disconnects
    async def disconnect(replica, c_replica, crash_type):
        await asyncio.sleep(random.random() / 100)
        await c_replica.execute_command("REPLICAOF NO ONE")

    await asyncio.gather(*(disconnect(*args) for args
                           in replicas_of_type(lambda t: t == 2)))

    await asyncio.sleep(0.5)

    # Check phase 3 replica survived
    for _, c_replica, _ in replicas_of_type(lambda t: t == 2):
        assert await c_replica.ping()
        await batch_check_data_async(c_replica, check_gen())

    # Check master survived all disconnects
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
    (4, [4], 3, 1000),
    (8, [8], 3, 5000),
    (6, [6, 6, 6], 3, 5000),
    (4, [2] * 8, 3, 5000),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, n_random_crashes, n_keys", master_crash_cases)
async def test_disconnect_master(df_local_factory, t_master, t_replicas, n_random_crashes, n_keys):
    master = df_local_factory.create(port=1111, proactor_threads=t_master)
    replicas = [
        df_local_factory.create(
            port=BASE_PORT+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    for replica in replicas:
        replica.start()

    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]

    async def full_sync(c_replica):
        try:
            await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
            await wait_available_async(c_replica)
        except aioredis.ResponseError as e:
            # This should mean master crashed during greet phase
            pass

    async def crash_master_fs():
        await asyncio.sleep(random.random() / 10 + 0.01)
        master.stop(kill=True)

    async def start_master():
        master.start()
        c_master = aioredis.Redis(port=master.port)
        assert await c_master.ping()
        await batch_fill_data_async(c_master, gen_test_data(n_keys, seed=0))

    await start_master()

    # Crash master during full sync
    await asyncio.gather(*(full_sync(c) for c in c_replicas), crash_master_fs())

    for _ in range(n_random_crashes):
        await start_master()
        await asyncio.sleep(random.random() + len(replicas) * random.random() / 10)
        # Crash master in some random state for each replica
        master.stop(kill=True)

    await start_master()
    await asyncio.sleep(1 + len(replicas) * 0.5)  # Replicas check every 500ms.
    for c_replica in c_replicas:
        await wait_available_async(c_replica)
        await batch_check_data_async(c_replica, gen_test_data(n_keys, seed=0))

    # Crash master during stable state
    master.stop(kill=True)

    await start_master()
    await asyncio.sleep(1 + len(replicas) * 0.5)
    for c_replica in c_replicas:
        await wait_available_async(c_replica)
        await batch_check_data_async(c_replica, gen_test_data(n_keys, seed=0))
