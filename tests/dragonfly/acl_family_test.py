import pytest
import redis
from redis import asyncio as aioredis
from . import DflyInstanceFactory
from .utility import disconnect_clients


@pytest.mark.asyncio
async def test_acl_list_default_user(async_client):
    """
    make sure that the default created user is printed correctly
    """

    # Bad input
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ACL LIST TEMP")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ACL")

    result = await async_client.execute_command("ACL LIST")
    assert 1 == len(result)
    assert "user default on nopass +@all" == result[0]
