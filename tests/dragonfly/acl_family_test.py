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
    result = await async_client.execute_command("ACL LIST")
    assert 1 == len(result)
    assert "user default on nopass +@ALL" == result[0]


@pytest.mark.asyncio
async def test_acl_setuser(async_client):
    # Bad input
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ACL SETUSER")

    await async_client.execute_command("ACL SETUSER kostas")
    result = await async_client.execute_command("ACL LIST")
    assert 2 == len(result)
    assert "user kostas off nopass +@NONE" in result

    await async_client.execute_command("ACL SETUSER kostas ON")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@NONE" in result

    await async_client.execute_command("ACL SETUSER kostas +@list +@string +@admin")
    result = await async_client.execute_command("ACL LIST")
    # TODO consider printing to lowercase
    assert "user kostas on nopass +@LIST +@STRING +@ADMIN" in result

    await async_client.execute_command("ACL SETUSER kostas -@list -@admin")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@STRING" in result

    # mix and match
    await async_client.execute_command("ACL SETUSER kostas +@list -@string")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@LIST" in result

    await async_client.execute_command("ACL SETUSER kostas +@all")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@ALL" in result
