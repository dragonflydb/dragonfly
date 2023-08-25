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


@pytest.mark.asyncio
async def test_acl_auth(async_client):
    await async_client.execute_command("ACL SETUSER shahar >mypass")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("AUTH shahar wrong_pass")

    # This should fail because user is inactive
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("AUTH shahar mypass")

    # Activate user
    await async_client.execute_command("ACL SETUSER shahar ON +@fast")

    result = await async_client.execute_command("AUTH shahar mypass")
    result == "ok"

    # Let's also try default
    result = await async_client.execute_command("AUTH default nopass")
    result == "ok"


@pytest.mark.asyncio
async def test_acl_categories(async_client):
    await async_client.execute_command("ACL SETUSER vlad ON >mypass +@string +@list +@connection")

    result = await async_client.execute_command("AUTH vlad mypass")
    result == "ok"

    result = await async_client.execute_command("SET foo bar")
    result == "ok"

    result = await async_client.execute_command("LPUSH mykey space_monkey")
    result == "ok"

    # This should fail, vlad does not have @admin
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ACL SETUSER vlad ON >mypass")

    # This should fail, vlad does not have @sortedset
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ZADD myset 1 two")

    result = await async_client.execute_command("AUTH default nopass")
    result == "ok"

    # Make vlad an admin
    await async_client.execute_command("ACL SETUSER vlad -@string")
    result == "ok"

    result = await async_client.execute_command("AUTH vlad mypass")
    result == "ok"

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("GET foo")

    result = await async_client.execute_command("AUTH default nopass")
    result == "ok"

    # Vlad goes rogue starts giving admin stats to random users
    await async_client.execute_command("ACL SETUSER adi >adi +@admin")
    result == "ok"

    # Vlad can now execute everything
    await async_client.execute_command("ACL SETUSER vlad +@all")
    result == "ok"

    await async_client.execute_command("ZADD myset 1 two")
    result == "ok"
