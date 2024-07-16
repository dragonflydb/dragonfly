import pytest
import asyncio
import redis
from redis import asyncio as aioredis
from pathlib import Path

from . import dfly_args

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}


@pytest.mark.skip(
    reason="Currently we can not guarantee that on shutdown if command is executed and value is written we response before breaking the connection"
)
@dfly_args({"proactor_threads": "4"})
class TestDflyAutoLoadSnapshot:
    """
    Test automatic loading of dump files on startup with timestamp.
    When command is executed if a value is written we should send the response before shutdown
    """

    @pytest.mark.asyncio
    async def test_gracefull_shutdown(self, df_factory):
        df_args = {"dbfilename": "dump", **BASIC_ARGS, "port": 1111}

        df_server = df_factory.create(**df_args)
        df_server.start()
        client = aioredis.Redis(port=df_server.port)

        async def counter(key):
            value = 0
            await client.execute_command(f"SET {key} 0")
            while True:
                try:
                    value = await client.execute_command(f"INCR {key}")
                except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError) as e:
                    break
            return key, value

        async def delayed_takeover():
            await asyncio.sleep(1)
            await client.execute_command("SHUTDOWN")
            await client.connection_pool.disconnect()

        _, *results = await asyncio.gather(
            delayed_takeover(), *[counter(f"key{i}") for i in range(16)]
        )

        df_server.start()
        client = aioredis.Redis(port=df_server.port)

        for key, acknowleged_value in results:
            value_from_snapshot = await client.get(key)
            assert acknowleged_value == int(value_from_snapshot)

        await client.connection_pool.disconnect()
