import time
import pytest
import asyncio
import aioredis
import subprocess
from .utility import *


class RedisServer:
    def __init__(self):
        self.port = 5555
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(["redis-server-6.2.11",
                                      f"--port {self.port}",
                                      "--save ''",
                                      "--appendonly no",
                                      "--protected-mode no",
                                      "--repl-diskless-sync yes",
                                      "--repl-diskless-sync-delay 0"])
        print(self.proc.args)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except Exception as e:
            pass

# Checks that master and redis are synced by writing a random key to master
# and waiting for it to exist in replica. Foreach db in 0..dbcount-1.


async def await_synced(master_port, replica_port, dbcount=1):
    rnd_str = "".join(random.choices(string.ascii_letters, k=10))
    key = "sync_key/" + rnd_str
    for db in range(dbcount):
        c_master = aioredis.Redis(port=master_port, db=db)
        await c_master.set(key, "dummy")
        print(f"set {key} MASTER db = {db}")
        c_replica = aioredis.Redis(port=replica_port, db=db)
        timeout = 30
        while timeout > 0:
            timeout -= 1
            v = await c_replica.get(key)
            print(f"get {key} from REPLICA db = {db} got {v}")
            if v is not None:
                break
            await asyncio.sleep(1)
        await c_master.close()
        await c_replica.close()
        assert timeout > 0, "Timeout while waiting for replica to sync"


async def await_synced_all(c_master, c_replicas):
    for c_replica in c_replicas:
        await await_synced(c_master, c_replica)


async def check_data(seeder, replicas, c_replicas):
    capture = await seeder.capture()
    for (replica, c_replica) in zip(replicas, c_replicas):
        await wait_available_async(c_replica)
        assert await seeder.compare(capture, port=replica.port)


@pytest.fixture(scope="function")
def redis_server() -> RedisServer:
    s = RedisServer()
    s.start()
    time.sleep(1)
    yield s
    s.stop()


full_sync_replication_specs = [
    ([1], dict(keys=100, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1], dict(keys=5000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([2], dict(keys=5000, dbcount=4, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_replicas, seeder_config", full_sync_replication_specs)
async def test_replication_full_sync(df_local_factory, df_seeder_factory, redis_server, t_replicas, seeder_config):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    replica = df_local_factory.create(
        port=master.port + 1, proactor_threads=t_replicas[0])
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)
    assert await c_replica.ping()

    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await wait_available_async(c_replica)
    await await_synced(master.port, replica.port, seeder_config["dbcount"])

    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)

stable_sync_replication_specs = [
    ([1], dict(keys=100, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1], dict(keys=10_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([2], dict(keys=10_000, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([2], dict(keys=10_000, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([8], dict(keys=10_000, dbcount=4, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_replicas, seeder_config", stable_sync_replication_specs)
async def test_replication_stable_sync(df_local_factory, df_seeder_factory, redis_server, t_replicas, seeder_config):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    replica = df_local_factory.create(
        port=master.port + 1, proactor_threads=t_replicas[0])
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)
    assert await c_replica.ping()

    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await wait_available_async(c_replica)

    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_ops=1000)

    await await_synced(master.port, replica.port, seeder_config["dbcount"])

    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)


# Threads for each dragonfly replica, Seeder Config.
replication_specs = [
    ([1], dict(keys=1000, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([6, 6, 6], dict(keys=4_000, dbcount=4, unsupported_types=[ValueType.JSON])),
    ([2, 2, 2, 2], dict(keys=4_000, dbcount=4, unsupported_types=[ValueType.JSON])),
    ([8, 8], dict(keys=4_000, dbcount=4, unsupported_types=[ValueType.JSON])),
    ([1] * 8, dict(keys=500, dbcount=2, unsupported_types=[ValueType.JSON])),
    ([1], dict(keys=100, dbcount=2, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_replicas, seeder_config", replication_specs)
async def test_redis_replication_all(df_local_factory, df_seeder_factory, redis_server, t_replicas, seeder_config):
    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    replicas = [
        df_local_factory.create(port=master.port+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Fill master with test data
    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    # Start replicas
    df_local_factory.start_all(replicas)

    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]

    # Start data stream
    stream_task = asyncio.create_task(seeder.run())
    await asyncio.sleep(0.0)

    # Start replication
    async def run_replication(c_replica):
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
        await wait_available_async(c_replica)

    await asyncio.gather(*(asyncio.create_task(run_replication(c))
                           for c in c_replicas))

    # Wait for streaming to finish
    assert not stream_task.done(
    ), "Weak testcase. Increase number of streamed iterations to surpass full sync"
    seeder.stop()
    await stream_task

    # Check data after full sync
    await await_synced_all(master.port, [replica.port for replica in replicas])
    await check_data(seeder, replicas, c_replicas)

    # Stream more data in stable state
    await seeder.run(target_ops=2000)

    # Check data after stable state stream
    await await_synced_all(master.port, [replica.port for replica in replicas])
    await check_data(seeder, replicas, c_replicas)


master_disconnect_cases = [
    ([6], 1, dict(keys=4_000, dbcount=1, unsupported_types=[ValueType.JSON])),
    ([1, 4, 6], 3, dict(keys=1_000, dbcount=2, unsupported_types=[ValueType.JSON])),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("t_replicas, t_disconnect, seeder_config", master_disconnect_cases)
async def test_disconnect_master(df_local_factory, df_seeder_factory, redis_server, t_replicas, t_disconnect, seeder_config):

    master = redis_server
    c_master = aioredis.Redis(port=master.port)
    assert await c_master.ping()

    replicas = [
        df_local_factory.create(port=master.port+i+1, proactor_threads=t)
        for i, t in enumerate(t_replicas)
    ]

    # Fill master with test data
    seeder = df_seeder_factory.create(port=master.port, **seeder_config)
    await seeder.run(target_deviation=0.1)

    # Start replicas
    df_local_factory.start_all(replicas)

    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]

    # Start data stream
    stream_task = asyncio.create_task(seeder.run())
    await asyncio.sleep(0.0)

    # Start replication
    async def run_replication(c_replica):
        await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
        await wait_available_async(c_replica)

    await asyncio.gather(*(asyncio.create_task(run_replication(c))
                           for c in c_replicas))

    # Wait for streaming to finish
    assert not stream_task.done(
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
    await asyncio.gather(*(asyncio.create_task(wait_available_async(c)) for c in c_replicas))
    await await_synced_all(master.port, [replica.port for replica in replicas])
    await check_data(seeder, replicas, c_replicas)
