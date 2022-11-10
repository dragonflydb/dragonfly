
import pytest
import asyncio
import aioredis
import redis
import time

from .utility import *
from . import dfly_args


BASE_PORT = 1111

"""
Test full replication pipeline. Test full sync with streaming changes and stable state streaming.
"""

replication_cases = [
    (2, 2, 1500, 200)
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replica, n_keys, n_stream_keys", replication_cases)
@dfly_args({"logtostdout": ""})
async def test_replication_all(df_local_factory, t_master, t_replica, n_keys, n_stream_keys):
    master = df_local_factory.create(port=1111, proactor_threads=t_master)
    replica = df_local_factory.create(port=1112, proactor_threads=t_replica)

    # Start master and fill with test data
    master.start()
    c_master = aioredis.Redis(port=master.port)
    await batch_fill_data_async(c_master, gen_test_data(n_keys, seed=1))

    # Start replica
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)

    async def stream_data():
        for k, v in gen_test_data(n_stream_keys, seed=2):
            await c_master.set(k, v)

    # Start streaming data and run REPLICAOF in parallel
    stream_fut = asyncio.create_task(stream_data())
    await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
    await stream_fut

    await wait_available_async(c_replica)
    # Check range [n_stream_keys, n_keys] is of seed 1
    await batch_check_data_async(c_replica, gen_test_data(n_keys, start=n_stream_keys, seed=1))
    # Check range [0, n_stream_keys] is of seed 2
    await batch_check_data_async(c_replica, gen_test_data(n_stream_keys, seed=2))

    # Check stable state streaming
    await batch_fill_data_async(c_master, gen_test_data(n_keys, seed=3))
    await asyncio.sleep(0.1)
    await batch_check_data_async(c_replica, gen_test_data(n_keys, seed=3))


"""
Test simple full sync on one replica without altering data during replication.
"""

# (threads_master, threads_replica, n entries)
simple_full_sync_cases = [
    (2, 2, 100),
    (8, 2, 500),
    (2, 8, 500),
    (6, 4, 500)
]


@pytest.mark.parametrize("t_master, t_replica, n_keys", simple_full_sync_cases)
def test_simple_full_sync(df_local_factory, t_master, t_replica, n_keys):
    master = df_local_factory.create(port=1111, proactor_threads=t_master)
    replica = df_local_factory.create(port=1112, proactor_threads=t_replica)

    # Start master and fill with test data
    master.start()
    c_master = redis.Redis(port=master.port)
    batch_fill_data(c_master, gen_test_data(n_keys))

    # Start replica and run REPLICAOF
    replica.start()
    c_replica = redis.Redis(port=replica.port)
    c_replica.replicaof("localhost", str(master.port))

    # Check replica received test data
    wait_available(c_replica)
    batch_check_data(c_replica, gen_test_data(n_keys))

    # Stop replication manually
    c_replica.replicaof("NO", "ONE")
    assert c_replica.set("writeable", "true")

    # Check test data persisted
    batch_check_data(c_replica, gen_test_data(n_keys))


"""
Test simple full sync on multiple replicas without altering data during replication.
The replicas start running in parallel.
"""

# (threads_master, threads_replicas, n entries)
simple_full_sync_multi_cases = [
    (4, [3, 2], 500),
    (8, [6, 5, 4], 500),
    (8, [2] * 5, 100),
    (4, [1] * 20, 500)
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, n_keys", simple_full_sync_multi_cases)
async def test_simple_full_sync_multi(df_local_factory, t_master, t_replicas, n_keys):
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
        asyncio.create_task(run_sfs_replica(
            replica, master, data_gen), name="replica-"+str(replica.port))
        for replica in replicas
    ]

    for task in tasks:
        assert await task

    await c_master.connection_pool.disconnect()


async def run_sfs_replica(replica, master, data_gen):
    replica.start()
    c_replica = aioredis.Redis(
        port=replica.port, single_connection_client=None)

    await c_replica.execute_command("REPLICAOF localhost " + str(master.port))

    await wait_available_async(c_replica)
    await batch_check_data_async(c_replica, data_gen())

    await c_replica.connection_pool.disconnect()
    return True


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
