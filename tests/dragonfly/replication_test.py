
import pytest
import asyncio
import aioredis
import redis
import time

from .utility import *


BASE_PORT = 1111

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
    time.sleep(0.1)
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
simple_full_sync_mutli_cases = [
    (4, [3, 2], 500),
    (8, [6, 5, 4], 500),
    (8, [2] * 5, 100),
    (4, [1] * 20, 500)
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replicas, n_keys", simple_full_sync_mutli_cases)
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
    wait_time = len(replicas) * 0.1
    tasks = [
        asyncio.create_task(run_sfs_replica(
            replica, master, data_gen, wait_time), name="replica-"+str(replica.port))
        for replica in replicas
    ]

    for task in tasks:
        assert await task

    await c_master.connection_pool.disconnect()


async def run_sfs_replica(replica, master, data_gen, wait_time):
    replica.start()
    c_replica = aioredis.Redis(
        port=replica.port, single_connection_client=None)

    await c_replica.execute_command("REPLICAOF localhost " + str(master.port))

    await asyncio.sleep(wait_time)
    await batch_check_data_async(c_replica, data_gen())

    await c_replica.connection_pool.disconnect()
    return True
