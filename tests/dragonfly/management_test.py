import pytest
from redis.exceptions import ResponseError

from redis import asyncio as aioredis


@pytest.mark.asyncio
async def test_config_cmd(async_client: aioredis.Redis):
    with pytest.raises(ResponseError):
        await async_client.config_set("foo", "bar")
    await async_client.config_set("requirepass", "foobar") == "OK"
    res = await async_client.config_get("*")
    assert len(res) > 0
    assert res["requirepass"] == "foobar"
