
import pytest
import asyncio
import aioredis

from .utility import *


BASE_PORT = 1111

"""
Test full replication pipeline. Test full sync with streaming changes and stable state streaming.
"""

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


# (threads_master, threads_replicas, n entries)
simple_full_sync_multi_crash_cases = [
    (5, [1] * 15, 5000),
    (5, [1] * 20, 5000),
    (5, [1] * 25, 5000)
]


@pytest.mark.asyncio
@pytest.mark.skip(reason="test is currently crashing")
@pytest.mark.parametrize("t_master, t_replicas, n_keys", simple_full_sync_multi_crash_cases)
async def test_simple_full_sync_mutli_crash(df_local_factory, t_master, t_replicas, n_keys):
    def data_gen(): return gen_test_data(n_keys)

    master = df_local_factory.create(port=BASE_PORT, proactor_threads=t_master)
    replicas = [
        df_local_factory.create(port=BASE_PORT+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Start master and fill with test data
    master.start()
    c_master = aioredis.Redis(port=master.port, single_connection_client=True)
    await batch_fill_data_async(c_master, data_gen())

    # Start replica tasks in parallel
    tasks = [
        asyncio.create_task(run_sfs_crash_replica(
            replica, master, data_gen), name="replica-"+str(replica.port))
        for replica in replicas
    ]

    for task in tasks:
        assert await task

    # Check master is ok
    await batch_check_data_async(c_master, data_gen())

    await c_master.connection_pool.disconnect()


async def run_sfs_crash_replica(replica, master, data_gen):
    replica.start()
    c_replica = aioredis.Redis(
        port=replica.port, single_connection_client=None)

    await c_replica.execute_command("REPLICAOF localhost " + str(master.port))

    # Kill the replica after a short delay
    await asyncio.sleep(0.0)
    replica.stop(kill=True)

    await c_replica.connection_pool.disconnect()
    return True
