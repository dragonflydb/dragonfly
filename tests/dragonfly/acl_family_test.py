import pytest
import redis
from redis import asyncio as aioredis
from .instance import DflyInstanceFactory
from .utility import disconnect_clients
import tempfile
import asyncio
import os
from . import dfly_args


@pytest.mark.asyncio
async def test_acl_setuser(async_client):
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

    # mix and match interleaved
    await async_client.execute_command("ACL SETUSER kostas +@set -@set +@set")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@SET +@LIST" in result

    await async_client.execute_command("ACL SETUSER kostas +@all")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@ALL" in result

    # commands
    await async_client.execute_command("ACL SETUSER kostas +set +get +hset")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@ALL +SET +GET +HSET" in result

    await async_client.execute_command("ACL SETUSER kostas -set -get +hset")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@ALL +HSET" in result

    # interleaved
    await async_client.execute_command("ACL SETUSER kostas -hset +get -get -@all")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@NONE" in result

    # interleaved with categories
    await async_client.execute_command("ACL SETUSER kostas +@string +get -get +set")
    result = await async_client.execute_command("ACL LIST")
    assert "user kostas on nopass +@STRING +SET" in result


@pytest.mark.asyncio
async def test_acl_categories(async_client):
    await async_client.execute_command("ACL SETUSER vlad ON >mypass +@string +@list +@connection")

    result = await async_client.execute_command("AUTH vlad mypass")
    assert result == "OK"

    result = await async_client.execute_command("SET foo bar")
    assert result == "OK"

    result = await async_client.execute_command("LPUSH mykey space_monkey")
    assert result == 1

    # This should fail, vlad does not have @admin
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ACL SETUSER vlad ON >mypass")

    # This should fail, vlad does not have @sortedset
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ZADD myset 1 two")

    result = await async_client.execute_command("AUTH default nopass")
    assert result == "OK"

    # Make vlad an admin
    await async_client.execute_command("ACL SETUSER vlad -@string")
    assert result == "OK"

    result = await async_client.execute_command("AUTH vlad mypass")
    assert result == "OK"

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("GET foo")

    result = await async_client.execute_command("AUTH default nopass")
    assert result == "OK"

    # Vlad goes rogue starts giving admin stats to random users
    await async_client.execute_command("ACL SETUSER adi >adi +@admin")
    assert result == "OK"

    # Vlad can now execute everything
    await async_client.execute_command("ACL SETUSER vlad +@all")
    assert result == "OK"

    await async_client.execute_command("ZADD myset 1 two")
    assert result == "OK"


@pytest.mark.asyncio
async def test_acl_commands(async_client):
    await async_client.execute_command("ACL SETUSER random ON >mypass +@NONE +set +get")

    result = await async_client.execute_command("AUTH random mypass")
    assert result == "OK"

    result = await async_client.execute_command("SET foo bar")
    assert result == "OK"

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ZADD myset 1 two")


@pytest.mark.asyncio
async def test_acl_cat_commands_multi_exec_squash(df_local_factory):
    df = df_local_factory.create(multi_exec_squash=True, port=1111)

    df.start()

    # Testing acl categories
    client = aioredis.Redis(port=df.port)
    res = await client.execute_command("ACL SETUSER kk ON >kk +@transaction +@string")
    assert res == b"OK"

    res = await client.execute_command("AUTH kk kk")
    assert res == b"OK"

    await client.execute_command("MULTI")
    assert res == b"OK"
    for x in range(33):
        await client.execute_command(f"SET x{x} {x}")
    await client.execute_command("EXEC")

    client = aioredis.Redis(port=df.port)
    await client.close()

    # NOPERM while executing multi
    await client.execute_command("ACL SETUSER kk -@string")
    assert res == b"OK"
    await client.execute_command("AUTH kk kk")
    assert res == b"OK"
    await client.execute_command("MULTI")
    assert res == b"OK"

    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command(f"SET x{x} {x}")
    await client.close()

    # NOPERM between multi and exec
    admin_client = aioredis.Redis(port=df.port)
    res = await client.execute_command("ACL SETUSER kk +@string")
    assert res == b"OK"

    client = aioredis.Redis(port=df.port)
    res = await client.execute_command("AUTH kk kk")
    assert res == b"OK"
    # CLIENT has permissions, starts MULTI and issues a bunch of SET commands
    await client.execute_command("MULTI")
    assert res == b"OK"
    for x in range(33):
        await client.execute_command(f"SET x{x} {x}")

    # ADMIN revokes permissions
    res = await admin_client.execute_command("ACL SETUSER kk -@string")
    assert res == b"OK"

    res = await client.execute_command("EXEC")
    # TODO(we need to fix this, basiscally SQUASHED/MULTI transaction commands
    # return multiple errors for each command failed. Since the nature of the error
    # is the same, that a rule has changed we should squash those error messages into
    # one.
    assert res[0].args[0] == "NOPERM: kk ACL rules changed between the MULTI and EXEC"

    await admin_client.close()
    await client.close()

    # Testing acl commands
    client = aioredis.Redis(port=df.port)
    res = await client.execute_command("ACL SETUSER myuser ON >kk +@transaction +set")
    assert res == b"OK"

    res = await client.execute_command("AUTH myuser kk")
    assert res == b"OK"

    await client.execute_command("MULTI")
    assert res == b"OK"
    for x in range(33):
        await client.execute_command(f"SET x{x} {x}")
    await client.execute_command("EXEC")

    # NOPERM between multi and exec
    admin_client = aioredis.Redis(port=df.port)
    res = await admin_client.execute_command("ACL SETUSER myuser -set")
    assert res == b"OK"

    # NOPERM while executing multi
    await client.execute_command("MULTI")

    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command(f"SET x{x} {x}")

    await admin_client.close()
    await client.close()


@pytest.mark.skip("Skip because it fails on arm release")
@pytest.mark.asyncio
async def test_acl_deluser(df_server):
    client = aioredis.Redis(port=df_server.port)

    res = await client.execute_command("ACL SETUSER george ON >pass +@transaction +@string")
    assert res == b"OK"

    res = await client.execute_command("AUTH george pass")
    assert res == b"OK"

    await client.execute_command("MULTI")
    await client.execute_command("SET key 44")

    admin_client = aioredis.Redis(port=df_server.port)
    await admin_client.execute_command("ACL DELUSER george")

    with pytest.raises(redis.exceptions.ConnectionError):
        await client.execute_command("EXEC")

    await admin_client.close()


script = """
for i = 1, 100000 do
  redis.call('SET', 'key', i)
  redis.call('SET', 'key1', i)
  redis.call('SET', 'key2', i)
  redis.call('SET', 'key3', i)
end
"""


@pytest.mark.asyncio
@pytest.mark.skip("Non deterministic")
async def test_acl_del_user_while_running_lua_script(df_server):
    client = aioredis.Redis(port=df_server.port)
    await client.execute_command("ACL SETUSER kostas ON >kk +@string +@scripting")
    await client.execute_command("AUTH kostas kk")
    admin_client = aioredis.Redis(port=df_server.port)

    with pytest.raises(redis.exceptions.ConnectionError):
        await asyncio.gather(
            client.eval(script, 4, "key", "key1", "key2", "key3"),
            admin_client.execute_command("ACL DELUSER kostas"),
        )

    for i in range(1, 4):
        res = await admin_client.get(f"key{i}")
        assert res == b"100000"

    await admin_client.close()


@pytest.mark.asyncio
@pytest.mark.skip("Non deterministic")
async def test_acl_with_long_running_script(df_server):
    client = aioredis.Redis(port=df_server.port)
    await client.execute_command("ACL SETUSER roman ON >yoman +@string +@scripting")
    await client.execute_command("AUTH roman yoman")
    admin_client = aioredis.Redis(port=df_server.port)

    await asyncio.gather(
        client.eval(script, 4, "key", "key1", "key2", "key3"),
        admin_client.execute_command("ACL SETUSER roman -@string -@scripting"),
    )

    for i in range(1, 4):
        res = await admin_client.get(f"key{i}")
        assert res == b"100000"

    await client.close()
    await admin_client.close()


def create_temp_file(content, tmp_dir):
    file = tempfile.NamedTemporaryFile(mode="w", dir=tmp_dir, delete=False)
    acl = os.path.join(tmp_dir, file.name)
    file.write(content)
    file.flush()
    return acl


@pytest.mark.asyncio
@dfly_args({"port": 1111})
async def test_bad_acl_file(df_local_factory, tmp_dir):
    acl = create_temp_file("ACL SETUSER kostas ON >mypass +@WRONG", tmp_dir)

    df = df_local_factory.create(aclfile=acl)

    df.start()

    client = aioredis.Redis(port=df.port)

    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command("ACL LOAD")

    await client.close()


@pytest.mark.asyncio
@dfly_args({"port": 1111})
async def test_good_acl_file(df_local_factory, tmp_dir):
    acl = create_temp_file("", tmp_dir)
    df = df_local_factory.create(aclfile=acl)

    df.start()
    client = aioredis.Redis(port=df.port)

    await client.execute_command("ACL SETUSER roy ON >mypass +@STRING +HSET")
    await client.execute_command("ACL SETUSER shahar >mypass +@SET")
    await client.execute_command("ACL SETUSER vlad +@STRING")

    result = await client.execute_command("ACL LIST")
    assert 4 == len(result)
    assert "user roy on ea71c25a7a60224 +@STRING +HSET" in result
    assert "user shahar off ea71c25a7a60224 +@SET" in result
    assert "user vlad off nopass +@STRING" in result
    assert "user default on nopass +@ALL +ALL" in result

    result = await client.execute_command("ACL DELUSER shahar")
    assert result == b"OK"

    result = await client.execute_command("ACL SAVE")

    result = await client.execute_command("ACL LOAD")

    result = await client.execute_command("ACL LIST")
    assert 3 == len(result)
    assert "user roy on ea71c25a7a60224 +@STRING +HSET" in result
    assert "user vlad off nopass +@STRING" in result
    assert "user default on nopass +@ALL +ALL" in result

    await client.close()


@pytest.mark.asyncio
async def test_acl_log(async_client):
    res = await async_client.execute_command("ACL LOG")
    assert [] == res

    await async_client.execute_command("ACL SETUSER elon >mars ON +@string +@dangerous")

    with pytest.raises(redis.exceptions.AuthenticationError):
        await async_client.execute_command("AUTH elon wrong")

    res = await async_client.execute_command("ACL LOG")
    assert 1 == len(res)
    assert res[0]["reason"] == "AUTH"
    assert res[0]["object"] == "AUTH"
    assert res[0]["username"] == "elon"

    await async_client.execute_command("ACL LOG RESET")
    res = await async_client.execute_command("ACL LOG")
    assert 0 == len(res)

    res = await async_client.execute_command("AUTH elon mars")
    res = await async_client.execute_command("SET mykey 22")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("HSET mk kk 22")

    res = await async_client.execute_command("ACL LOG")
    assert 1 == len(res)
    assert res[0]["reason"] == "COMMAND"
    assert res[0]["object"] == "HSET"
    assert res[0]["username"] == "elon"

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("LPUSH mylist 2")

    res = await async_client.execute_command("ACL LOG")
    assert 2 == len(res)


@pytest.mark.asyncio
@dfly_args({"port": 1111, "admin_port": 1112, "requirepass": "mypass"})
async def test_require_pass(df_local_factory):
    df = df_local_factory.create()
    df.start()

    client = aioredis.Redis(port=df.port)

    with pytest.raises(redis.exceptions.AuthenticationError):
        await client.execute_command("AUTH default wrongpass")

    client = aioredis.Redis(password="mypass", port=df.port)

    res = await client.execute_command("AUTH default mypass")
    assert res == b"OK"

    res = await client.execute_command("CONFIG SET requirepass newpass")
    assert res == b"OK"

    res = await client.execute_command("AUTH default newpass")
    assert res == b"OK"

    client = aioredis.Redis(password="newpass", port=df.admin_port)

    await client.execute_command("SET foo 44")
    res = await client.execute_command("GET foo")
    assert res == b"44"

    await client.close()


@pytest.mark.asyncio
async def test_set_acl_file(async_client: aioredis.Redis, tmp_dir):
    acl_file_content = "ACL SETUSER roy ON >ea71c25a7a602246b4c39824b855678894a96f43bb9b71319c39700a1e045222 +@STRING +HSET"

    acl = create_temp_file(acl_file_content, tmp_dir)

    await async_client.execute_command(f"CONFIG SET aclfile {acl}")

    await async_client.execute_command("ACL LOAD")

    result = await async_client.execute_command("ACL LIST")
    assert 2 == len(result)

    result = await async_client.execute_command("AUTH roy mypass")
    assert result == "OK"
