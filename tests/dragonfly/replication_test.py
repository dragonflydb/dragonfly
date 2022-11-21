
import pytest
import asyncio
import aioredis
import random
from itertools import count, chain, repeat

from .utility import *
from . import dfly_args


BASE_PORT = 1111

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
Test replica crash during full sync on multiple replicas without altering data during replication.
"""

# 1. Number of master threads
# 2. Number of threads for each replica that crashes during full sync
# 2. NUmber of threads for each replica that crashes during stable sync
# 3. Number of distinct keys that are constantly streamed
crash_cases = [
    # balanced
    (8, [4, 4], [4, 4], 10000),
    (8, [2] * 6, [2] * 6, 10000),
    # full sync heavy
    (8, [4] * 6, [], 10000),
    (8, [2] * 12, [], 10000),
    # stable state heavy
    (8, [], [4] * 6, 10000),
    (8, [], [2] * 12, 10000)
]


@dfly_args({"logtostdout": ""})
@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_crash_fs, t_crash_ss, n_keys", crash_cases)
async def test_crash(df_local_factory, t_master, t_crash_fs, t_crash_ss, n_keys):
    master = df_local_factory.create(port=BASE_PORT, proactor_threads=t_master)
    replicas = [
        (df_local_factory.create(
            port=BASE_PORT+i+1, proactor_threads=t), crash_fs)
        for i, (t, crash_fs) in enumerate(
            chain(
                zip(t_crash_fs, repeat(True)),
                zip(t_crash_ss, repeat(False))
            )
        )
    ]

    # Start master
    master.start()
    c_master = aioredis.Redis(port=master.port, single_connection_client=True)

    # Start replicas
    for replica, _ in replicas:
        replica.start()

    # Start data fill loop
    async def fill_loop():
        for seed in count(1):
            await batch_fill_data_async(c_master, gen_test_data(n_keys, seed=seed))

    fill_task = asyncio.create_task(fill_loop())

    # Run full sync
    async def full_sync(replica, crash_fs):
        c_replica = aioredis.Redis(port=replica.port)
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
        if crash_fs:
            await asyncio.sleep(random.random()/100+0.01)
            replica.stop(kill=True)
        else:
            await wait_available_async(c_replica)

    await asyncio.gather(*(full_sync(*args) for args in replicas))

    # Wait for master to stream a bit more
    await asyncio.sleep(0.1)

    # Check master survived full sync crashes
    assert await c_master.ping()

    # Check phase-2 replicas survived
    c_replicas_ss = [
        (replica, aioredis.Redis(port=replica.port))
        for replica, crash_fs in replicas
        if not crash_fs
    ]
    for _, c_replica in c_replicas_ss:
        assert await c_replica.ping()

    # Run stable state crashes
    async def stable_sync(replica, c_replica):
        await asyncio.sleep(random.random() / 100)
        replica.stop(kill=True)

    await asyncio.gather(*(stable_sync(*args) for args in c_replicas_ss))

    # Check master survived full sync crashes
    assert await c_master.ping()
    fill_task.cancel()
