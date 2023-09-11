import os
import pytest
import redis
import asyncio
from redis import asyncio as aioredis

from . import dfly_multi_test_args, dfly_args, DflyStartException
from .utility import batch_fill_data, gen_test_data


@dfly_multi_test_args({"keys_output_limit": 512}, {"keys_output_limit": 1024})
class TestKeys:
    async def test_max_keys(self, async_client: aioredis.Redis, df_server):
        max_keys = df_server["keys_output_limit"]
        pipe = async_client.pipeline()
        batch_fill_data(pipe, gen_test_data(max_keys * 3))
        await pipe.execute()
        keys = await async_client.keys()
        assert len(keys) in range(max_keys, max_keys + 512)


@pytest.fixture(scope="function")
def export_dfly_password() -> str:
    pwd = "flypwd"
    os.environ["DFLY_PASSWORD"] = pwd
    yield pwd
    del os.environ["DFLY_PASSWORD"]


async def test_password(df_local_factory, export_dfly_password):
    dfly = df_local_factory.create()
    dfly.start()

    # Expect password form environment variable
    with pytest.raises(redis.exceptions.AuthenticationError):
        client = aioredis.Redis()
        await client.ping()
    client = aioredis.Redis(password=export_dfly_password)
    await client.ping()
    dfly.stop()

    # --requirepass should take precedence over environment variable
    requirepass = "requirepass"
    dfly = df_local_factory.create(requirepass=requirepass)
    dfly.start()

    # Expect password form flag
    with pytest.raises(redis.exceptions.AuthenticationError):
        client = aioredis.Redis(password=export_dfly_password)
        await client.ping()
    client = aioredis.Redis(password=requirepass)
    await client.ping()
    dfly.stop()


"""
Make sure that multi-hop transactions can't run OOO.
"""

MULTI_HOPS = """
for i = 0, ARGV[1] do
  redis.call('INCR', KEYS[1])
end
"""


@dfly_args({"proactor_threads": 1})
async def test_txq_ooo(async_client: aioredis.Redis, df_server):
    async def task1(k, h):
        c = aioredis.Redis(port=df_server.port)
        for _ in range(100):
            await c.eval(MULTI_HOPS, 1, k, h)

    async def task2(k, n):
        c = aioredis.Redis(port=df_server.port)
        for _ in range(100):
            pipe = c.pipeline(transaction=False)
            pipe.lpush(k, 1)
            for _ in range(n):
                pipe.blpop(k, 0.001)
            await pipe.execute()

    await asyncio.gather(
        task1("i1", 2), task1("i2", 3), task2("l1", 2), task2("l1", 2), task2("l1", 5)
    )


@dfly_args({"port": 6377})
async def test_arg_from_environ_overwritten_by_cli(df_local_factory):
    os.environ["DFLY_port"] = "6378"
    dfly = df_local_factory.create()
    dfly.start()
    client = aioredis.Redis(port="6377")
    await client.ping()


async def test_arg_from_environ(df_local_factory, export_dfly_password):
    os.environ["DFLY_requirepass"] = "true"
    dfly = df_local_factory.create()
    dfly.start()

    # Expect password form environment variable
    with pytest.raises(redis.exceptions.AuthenticationError):
        client = aioredis.Redis()
        await client.ping()

    client = aioredis.Redis(password=export_dfly_password)
    await client.ping()
    dfly.stop()


async def test_unknown_dfly_env(df_local_factory, export_dfly_password):
    os.environ["DFLY_abcdef"] = "xyz"
    with pytest.raises(DflyStartException):
        dfly = df_local_factory.create()
        dfly.start()
