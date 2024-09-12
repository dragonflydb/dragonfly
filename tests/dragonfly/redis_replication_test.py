import time
import pytest
import asyncio
from redis import asyncio as aioredis
import subprocess
from .utility import *
from .instance import DflyInstanceFactory
from .proxy import Proxy


# Checks that master redis and dragonfly replica are synced by writing a random key to master
# and waiting for it to exist in replica. Foreach db in 0..dbcount-1.
async def await_synced(c_master: aioredis.Redis, c_replica: aioredis.Redis, dbcount=1):
    rnd_str = "".join(random.choices(string.ascii_letters, k=10))
    key = "sync_key/" + rnd_str
    for db in range(dbcount):
        await c_master.set(key, "dummy")
        logging.debug(f"set {key} MASTER db = {db}")
        timeout = 30
        while timeout > 0:
            v = await c_replica.get(key)
            logging.debug(f"get {key} from REPLICA db = {db} got {v}")
            if v is not None:
                break
            repl_state = await c_master.info("replication")
            logging.debug(f"replication info: {repl_state}")
            await asyncio.sleep(1)

            timeout -= 1
        await c_master.close()
        await c_replica.close()
        assert timeout > 0, "Timeout while waiting for replica to sync"


async def await_synced_all(c_master, c_replicas):
    for c_replica in c_replicas:
        await await_synced(c_master, c_replica)


async def check_data(seeder, replicas, c_replicas):
    capture = await seeder.capture()
    for replica, c_replica in zip(replicas, c_replicas):
        await wait_available_async(c_replica)
        assert await seeder.compare(capture, port=replica.port)


# Start replication
async def run_replication(client: aioredis.Redis, port):
    res = await client.execute_command("REPLICAOF localhost " + str(port))
    assert res == "OK"
    await wait_available_async(client)


async def replicate_all(replicas, port):
    await asyncio.gather(*(asyncio.create_task(run_replication(c, port)) for c in replicas))


full_sync_replication_specs = [
    ([1], dict(keys=100, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1], dict(keys=5000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([2], dict(keys=5000, dbcount=4, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.parametrize("t_replicas, seeder_config", full_sync_replication_specs)
async def test_replication_full_sync(
    df_factory, df_seeder_factory, redis_server, t_replicas, seeder_config, port_picker
):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    replica = df_factory.create(
        port=port_picker.get_available_port(), proactor_threads=t_replicas[0]
    )
    replica.start()
    c_replica = replica.client()
    assert await c_replica.ping()

    await run_replication(c_replica, master.port)
    await await_synced(c_master, c_replica, seeder_config["dbcount"])

    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)


stable_sync_replication_specs = [
    ([1], dict(keys=100, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1], dict(keys=10_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([2], dict(keys=10_000, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([2], dict(keys=10_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([8], dict(keys=10_000, dbcount=4, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.parametrize("t_replicas, seeder_config", stable_sync_replication_specs)
async def test_replication_stable_sync(
    df_factory, df_seeder_factory, redis_server, t_replicas, seeder_config, port_picker
):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    replica = df_factory.create(
        port=port_picker.get_available_port(), proactor_threads=t_replicas[0]
    )
    replica.start()
    c_replica = replica.client()
    assert await c_replica.ping()

    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await wait_available_async(c_replica)

    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_ops=1000)

    await await_synced(c_master, c_replica, seeder_config["dbcount"])

    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)


# Threads for each dragonfly replica, Seeder Config.
replication_specs = [
    ([1], dict(keys=1000, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([6, 6, 6], dict(keys=4_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([2, 2], dict(keys=4_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([8, 8], dict(keys=4_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([1] * 8, dict(keys=500, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1], dict(keys=100, dbcount=4, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.parametrize("t_replicas, seeder_config", replication_specs)
async def test_redis_replication_all(
    df_factory: DflyInstanceFactory,
    df_seeder_factory,
    redis_server,
    t_replicas,
    seeder_config,
    port_picker,
):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    replicas = [
        df_factory.create(port=port_picker.get_available_port(), proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Fill master with test data
    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    # Start replicas
    df_factory.start_all(replicas)

    c_replicas = [replica.client() for replica in replicas]

    # Start data stream
    stream_task = asyncio.create_task(seeder.run())
    await asyncio.sleep(0.0)

    await replicate_all(c_replicas, master.port)

    # Wait for streaming to finish
    assert (
        not stream_task.done()
    ), "Weak testcase. Increase number of streamed iterations to surpass full sync"
    seeder.stop()
    await stream_task

    # Check data after full sync
    await await_synced_all(c_master, c_replicas)
    await check_data(seeder, replicas, c_replicas)

    # Stream more data in stable state
    await seeder.run(target_ops=2000)

    # Check data after stable state stream
    await await_synced_all(c_master, c_replicas)
    await check_data(seeder, replicas, c_replicas)


master_disconnect_cases = [
    ([6], 1, dict(keys=4_000, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1, 4, 6], 3, dict(keys=1_000, dbcount=2, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.parametrize("t_replicas, t_disconnect, seeder_config", master_disconnect_cases)
async def test_redis_master_restart(
    df_factory,
    df_seeder_factory,
    redis_server,
    t_replicas,
    t_disconnect,
    seeder_config,
    port_picker,
):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    replicas = [
        df_factory.create(port=port_picker.get_available_port(), proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Fill master with test data
    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    # Start replicas
    df_factory.start_all(replicas)

    c_replicas = [replica.client() for replica in replicas]

    # Start data stream
    stream_task = asyncio.create_task(seeder.run())
    await asyncio.sleep(0.0)

    await replicate_all(c_replicas, master.port)

    # Wait for streaming to finish
    assert (
        not stream_task.done()
    ), "Weak testcase. Increase number of streamed iterations to surpass full sync"
    seeder.stop()
    await stream_task

    for _ in range(t_disconnect):
        master.stop()
        await asyncio.sleep(1)
        master.start()
        await asyncio.sleep(1)
        # fill master with data
        await seeder.run(target_deviation=0.1)

    # Check data after stable state stream
    await wait_available_async(c_replicas)
    await await_synced_all(c_master, c_replicas)
    await check_data(seeder, replicas, c_replicas)


master_disconnect_cases = [
    ([6], dict(keys=4_000, dbcount=1, unsupported_types=[ValueType.JSON])),
    pytest.param(
        [1, 4, 6],
        dict(keys=1_000, dbcount=2, unsupported_types=[ValueType.JSON]),
        marks=pytest.mark.slow,
    ),
]


@pytest.mark.parametrize("t_replicas, seeder_config", master_disconnect_cases)
async def test_disconnect_master(
    df_factory,
    df_seeder_factory,
    redis_server,
    t_replicas,
    seeder_config,
    port_picker,
):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    proxy = Proxy("127.0.0.1", 1114, "127.0.0.1", master.port)
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

    replicas = [
        df_factory.create(port=port_picker.get_available_port(), proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Fill master with test data
    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    # Start replicas
    df_factory.start_all(replicas)

    c_replicas = [replica.client() for replica in replicas]

    # Start data stream
    stream_task = asyncio.create_task(seeder.run())
    await asyncio.sleep(0.5)

    await replicate_all(c_replicas, proxy.port)

    # Break the connection between master and replica
    await proxy.close(proxy_task)
    await asyncio.sleep(2)
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

    # finish streaming data
    await asyncio.sleep(1)
    seeder.stop()
    await stream_task

    # Check data after stable state stream
    await wait_available_async(c_replicas)
    await await_synced_all(c_master, c_replicas)
    await check_data(seeder, replicas, c_replicas)

    await proxy.close(proxy_task)
