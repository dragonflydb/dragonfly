import asyncio
import logging
import random
import string

import pytest
import redis.asyncio as aioredis

from .instance import DflyInstanceFactory
from .utility import ValueType, wait_available_async, assert_eventually


async def _bounded(awaitable, timeout=5):
    # Bounded so a stalled connection produces an exception instead of hanging the poll.
    return await asyncio.wait_for(awaitable, timeout=timeout)


async def _log_progress(c_master, c_replicas, interval=10):
    # Periodic progress so a long-running test phase (seeder.run, REPLICAOF, check_data)
    # is not silent. Caller cancels the task at teardown.
    while True:
        await asyncio.sleep(interval)
        infos = await asyncio.gather(
            _bounded(c_master.info("replication")),
            *(_bounded(c.info("replication")) for c in c_replicas),
            return_exceptions=True,
        )
        logging.info(f"progress master info={infos[0]}")
        for i, info in enumerate(infos[1:]):
            logging.info(f"progress replica[{i}] info={info}")


# Checks that master redis and dragonfly replica are synced by writing a random key to master
# and waiting for it to exist in replica. Foreach db in 0..dbcount-1.
async def await_synced(c_master: aioredis.Redis, c_replica: aioredis.Redis, dbcount=1, timeout=30):
    rnd_str = "".join(random.choices(string.ascii_letters, k=10))
    key = "sync_key/" + rnd_str
    for db in range(dbcount):
        await c_master.set(key, "dummy")
        logging.info(f"await_synced: set {key} on MASTER db={db}, polling REPLICA up to {timeout}s")
        remaining = timeout
        while remaining > 0:
            # Run concurrently so a tick is bounded by the slowest single call (5s), not their sum.
            # return_exceptions=True so a stalled call surfaces in its slot without aborting the rest.
            v, master_info, replica_info, master_dbsize, replica_dbsize = await asyncio.gather(
                c_replica.get(key),
                _bounded(c_master.info("replication")),
                _bounded(c_replica.info("replication")),
                _bounded(c_master.dbsize()),
                _bounded(c_replica.dbsize()),
                return_exceptions=True,
            )
            if not isinstance(v, BaseException) and v is not None:
                logging.info(f"await_synced: REPLICA db={db} synced after {timeout - remaining}s")
                break
            logging.info(
                f"await_synced db={db} t={timeout - remaining}s "
                f"dbsize master={master_dbsize} replica={replica_dbsize} "
                f"master_info={master_info} replica_info={replica_info}"
            )
            await asyncio.sleep(1)
            remaining -= 1
        else:
            logging.error(
                f"await_synced TIMEOUT db={db} key={key} after {timeout}s "
                f"dbsize master={master_dbsize} replica={replica_dbsize} "
                f"master_info={master_info} replica_info={replica_info}"
            )
            assert False, "Timeout while waiting for replica to sync"


async def await_synced_all(c_master, c_replicas, timeout=30, dbcount=1):
    for i, c_replica in enumerate(c_replicas):
        logging.info(f"await_synced_all: checking replica {i + 1}/{len(c_replicas)}")
        await await_synced(c_master, c_replica, dbcount=dbcount, timeout=timeout)


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

    progress = asyncio.create_task(_log_progress(c_master, c_replicas))
    stream_task = None
    try:
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
        stream_task = None

        # Check data after full sync
        await await_synced_all(c_master, c_replicas, timeout=60)
        await check_data(seeder, replicas, c_replicas)

        # Stream more data in stable state
        await seeder.run(target_ops=2000)

        # Check data after stable state stream
        await await_synced_all(c_master, c_replicas, timeout=60)
        await check_data(seeder, replicas, c_replicas)
    finally:
        try:
            if stream_task is not None:
                seeder.stop()
                await asyncio.gather(stream_task, return_exceptions=True)
        finally:
            progress.cancel()
            try:
                await progress
            except asyncio.CancelledError:
                pass


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
    try:
        await asyncio.sleep(0.0)

        await replicate_all(c_replicas, master.port)

        # Wait for streaming to finish
        assert (
            not stream_task.done()
        ), "Weak testcase. Increase number of streamed iterations to surpass full sync"
        seeder.stop()
        await stream_task
        stream_task = None

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
    finally:
        if stream_task is not None:
            seeder.stop()
            await asyncio.gather(stream_task, return_exceptions=True)


master_disconnect_cases = [
    ([6], dict(keys=4_000, dbcount=1, unsupported_types=[ValueType.JSON])),
    pytest.param(
        [1, 4, 6],
        dict(keys=1_000, dbcount=2, unsupported_types=[ValueType.JSON]),
        marks=pytest.mark.large,
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
    proxy_factory,
):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    stream_task = None
    seeder = None

    proxy = await proxy_factory(master.port)
    try:
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
        await proxy.close()
        await asyncio.sleep(2)
        await proxy.start_serving()

        # finish streaming data
        await asyncio.sleep(1)
        seeder.stop()
        await stream_task
        stream_task = None

        # Check data after stable state stream
        await wait_available_async(c_replicas)
        await await_synced_all(c_master, c_replicas)
        await check_data(seeder, replicas, c_replicas)
        seeder = None

    finally:
        if seeder is not None:
            seeder.stop()
        if stream_task is not None:
            await asyncio.gather(stream_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_redis_replication_info_offset(df_factory, redis_server, port_picker):
    """
    Dragonfly replica of a Redis master must report a non-zero slave_repl_offset
    in INFO REPLICATION that matches the offset the master tracks for that slave.
    """
    master = redis_server
    c_master = aioredis.Redis(port=master.port)

    for i in range(100):
        await c_master.set(f"key:{i}", f"value:{i}")

    replica = df_factory.create(port=port_picker.get_available_port(), proactor_threads=2)
    replica.start()
    c_replica = replica.client()

    await run_replication(c_replica, master.port)
    await await_synced(c_master, c_replica)

    # Give the ACK fiber time to fire (interval is 1000ms).
    await asyncio.sleep(2)

    replica_info = await c_replica.info("replication")
    replica_offset = replica_info.get("slave_repl_offset", 0)
    assert replica_offset > 0, f"slave_repl_offset={replica_offset}, expected non-zero"

    @assert_eventually(timeout=30)
    async def offsets_converge():
        m_info = await c_master.info("replication")
        r_info = await c_replica.info("replication")

        slave_entry = next(
            (v for k, v in m_info.items() if k.startswith("slave") and isinstance(v, dict)),
            None,
        )
        assert slave_entry is not None, "Master sees no slaves"

        master_offset = slave_entry["offset"]
        replica_offset = r_info.get("slave_repl_offset", 0)
        assert (
            replica_offset == master_offset
        ), f"slave_repl_offset mismatch: replica={replica_offset} master={master_offset}"

    await offsets_converge()
