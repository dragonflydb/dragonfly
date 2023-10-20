import pytest
import asyncio
from redis import asyncio as aioredis
from redis.exceptions import ResponseError


@pytest.mark.asyncio
async def test_config_cmd(df_factory):
    with df_factory.create() as server:
        async with server.client() as client:
            with pytest.raises(ResponseError):
                await client.config_set("foo", "bar")
            await client.config_set("requirepass", "foobar") == "OK"
            res = await client.config_get("*")
            assert len(res) > 0
            assert res["requirepass"] == "foobar"


@pytest.mark.asyncio
async def test_config_cmd_set_multiple_params(df_factory):
    with df_factory.create() as server:
        async with server.client() as client:
            await client.config_set("requirepass", "foobar", "maxclients", 100) == "OK"
            res = await client.config_get("*")
            assert len(res) > 0
            assert res["requirepass"] == "foobar"
            assert res["maxclients"] == "100"
