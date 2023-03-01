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


@pytest.fixture(scope="function")
def redis_server() -> RedisServer:
    s = RedisServer()
    s.start()
    time.sleep(1)
    yield s
    s.stop()


@pytest.mark.asyncio
async def test_replication_full_sync(df_local_factory, df_seeder_factory, redis_server):
    c_master = aioredis.Redis(port=redis_server.port)
    assert await c_master.ping()

    seeder = df_seeder_factory.create(port=redis_server.port, keys=100, dbcount=1, unsupported_types=[ValueType.JSON])
    await seeder.run(target_deviation=0.1)

    replica = df_local_factory.create(port=redis_server.port + 1)
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)
    assert await c_replica.ping()

    await c_replica.execute_command("REPLICAOF", "localhost", redis_server.port)
    await wait_available_async(c_replica)
    await asyncio.sleep(5)
    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)



@pytest.mark.asyncio
async def test_replication_stable_sync(df_local_factory, df_seeder_factory, redis_server):
    c_master = aioredis.Redis(port=redis_server.port)
    assert await c_master.ping()

    replica = df_local_factory.create(port=redis_server.port + 1)
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)
    assert await c_replica.ping()

    await c_replica.execute_command("REPLICAOF", "localhost", redis_server.port)
    await wait_available_async(c_replica)

    seeder = df_seeder_factory.create(port=redis_server.port, keys=100, dbcount=1, unsupported_types=[ValueType.JSON])
    await seeder.run(target_ops=100)

    await asyncio.sleep(5)
    capture = await seeder.capture()
    assert await seeder.compare(capture, port=replica.port)
