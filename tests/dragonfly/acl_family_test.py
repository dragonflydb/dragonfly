import pytest
import redis
from redis import asyncio as aioredis
from .instance import DflyInstanceFactory
from .utility import *
import tempfile
import asyncio
import os
from . import dfly_args


@pytest.mark.asyncio
async def test_acl_setuser(async_client):
    await async_client.execute_command("ACL SETUSER kostas")
    result = await async_client.execute_command("ACL list")
    assert 2 == len(result)
    assert "user kostas off -@all" in result

    await async_client.execute_command("ACL SETUSER kostas ON")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@all" in result

    await async_client.execute_command("ACL SETUSER kostas +@list +@string +@admin")
    result = await async_client.execute_command("ACL list")
    # TODO consider printing to lowercase
    assert "user kostas on -@all +@list +@string +@admin" in result

    await async_client.execute_command("ACL SETUSER kostas -@list -@admin")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@all +@string -@list -@admin" in result

    # mix and match
    await async_client.execute_command("ACL SETUSER kostas +@list -@string")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@all -@admin +@list -@string" in result

    # mix and match interleaved
    await async_client.execute_command("ACL SETUSER kostas +@set -@set +@set")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@all -@admin +@list -@string +@set" in result

    await async_client.execute_command("ACL SETUSER kostas +@all")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@admin +@list -@string +@set +@all" in result

    # commands
    await async_client.execute_command("ACL SETUSER kostas +set +get +hset")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@admin +@list -@string +@set +@all +set +get +hset" in result

    await async_client.execute_command("ACL SETUSER kostas -set -get +hset")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@admin +@list -@string +@set +@all -set -get +hset" in result

    # interleaved
    await async_client.execute_command("ACL SETUSER kostas -hset +get -get -@all")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@admin +@list -@string +@set -set -hset -get -@all" in result

    # interleaved with categories
    await async_client.execute_command("ACL SETUSER kostas +@string +get -get +set")
    result = await async_client.execute_command("ACL list")
    assert "user kostas on -@admin +@list +@set -hset -@all +@string -get +set" in result


@pytest.mark.asyncio
async def test_acl_categories(async_client):
    await async_client.execute_command(
        "ACL SETUSER vlad ON >mypass -@all +@string +@list +@connection ~*"
    )

    result = await async_client.execute_command("AUTH vlad mypass")
    assert result == "OK"

    result = await async_client.execute_command("SET foo bar")
    assert result == "OK"

    result = await async_client.execute_command("LPUSH mykey space_monkey")
    assert result == 1

    # This should fail, vlad does not have @admin
    with pytest.raises(redis.exceptions.ResponseError):
        result = await async_client.execute_command("ACL SETUSER vlad ON >mypass")

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
    await async_client.execute_command("ACL SETUSER random ON >mypass -@all +set +get ~*")

    result = await async_client.execute_command("AUTH random mypass")
    assert result == "OK"

    result = await async_client.execute_command("SET foo bar")
    assert result == "OK"

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ZADD myset 1 two")


@pytest.mark.asyncio
async def test_acl_cat_commands_multi_exec_squash(df_factory):
    df = df_factory.create(multi_exec_squash=True, port=1111)

    df.start()

    # Testing acl categories
    client = aioredis.Redis(port=df.port)
    res = await client.execute_command("ACL SETUSER kk ON >kk +@transaction +@string ~*")
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

    with pytest.raises(redis.exceptions.NoPermissionError):
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

    # admin revokes permissions
    res = await admin_client.execute_command("ACL SETUSER kk -@string")
    assert res == b"OK"

    res = await client.execute_command("EXEC")
    # TODO(we need to fix this, basiscally SQUASHED/MULTI transaction commands
    # return multiple errors for each command failed. Since the nature of the error
    # is the same, that a rule has changed we should squash those error messages into
    # one.
    assert res[0].args[0] == "kk ACL rules changed between the MULTI and EXEC"

    await admin_client.close()
    await client.close()

    # Testing acl commands
    client = aioredis.Redis(port=df.port)
    res = await client.execute_command("ACL SETUSER myuser ON >kk +@transaction +set ~*")
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

    with pytest.raises(redis.exceptions.NoPermissionError):
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
async def test_bad_acl_file(df_factory, tmp_dir):
    acl = create_temp_file("ACL SETUSER kostas ON >mypass +@WRONG", tmp_dir)

    df = df_factory.create(aclfile=acl)

    df.start()

    client = aioredis.Redis(port=df.port)

    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command("ACL LOAD")

    await client.close()


@pytest.mark.asyncio
@dfly_args({"port": 1111})
async def test_good_acl_file(df_factory, tmp_dir):
    acl = create_temp_file("USER MrFoo ON >mypass", tmp_dir)
    df = df_factory.create(aclfile=acl)

    df.start()
    client = df.client()

    await client.execute_command("ACL LOAD")
    result = await client.execute_command("ACL list")
    assert 2 == len(result)
    assert "user MrFoo on #ea71c25a7a60224 -@all" in result
    assert "user default on nopass ~* +@all" in result
    await client.execute_command("ACL DELUSER MrFoo")

    await client.execute_command("ACL SETUSER roy ON >mypass +@string +hset")
    await client.execute_command("ACL SETUSER shahar >mypass +@set")
    await client.execute_command("ACL SETUSER vlad ~foo ~bar* +@string")

    result = await client.execute_command("ACL list")
    assert 4 == len(result)
    assert "user roy on #ea71c25a7a60224 -@all +@string +hset" in result
    assert "user shahar off #ea71c25a7a60224 -@all +@set" in result
    assert "user vlad off ~foo ~bar* -@all +@string" in result
    assert "user default on nopass ~* +@all" in result

    result = await client.execute_command("ACL DELUSER shahar")
    assert result == 1

    result = await client.execute_command("ACL SAVE")

    result = await client.execute_command("ACL LOAD")

    result = await client.execute_command("ACL list")
    assert 3 == len(result)
    assert "user roy on #ea71c25a7a60224 -@all +@string +hset" in result
    assert "user vlad off ~foo ~bar* -@all +@string" in result
    assert "user default on nopass ~* +@all" in result

    await client.close()


@pytest.mark.asyncio
async def test_acl_log(async_client):
    res = await async_client.execute_command("ACL LOG")
    assert [] == res

    await async_client.execute_command("ACL SETUSER elon >mars ON +@string +@dangerous ~*")

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
        await async_client.execute_command("hset mk kk 22")

    res = await async_client.execute_command("ACL LOG")
    assert 1 == len(res)
    assert res[0]["reason"] == "COMMAND"
    assert res[0]["object"] == "HSET"
    assert res[0]["username"] == "elon"

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("LPUSH mylist 2")

    res = await async_client.execute_command("ACL LOG")
    assert 2 == len(res)

    res = await async_client.execute_command("ACL LOG RESET")
    await async_client.execute_command("ACL SETUSER elon resetkeys ~foo")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("SET bar val")

    res = await async_client.execute_command("ACL LOG")
    assert 1 == len(res)
    assert res[0]["reason"] == "KEY"
    assert res[0]["object"] == "SET"
    assert res[0]["username"] == "elon"


@pytest.mark.asyncio
@dfly_args({"port": 1111, "admin_port": 1112, "requirepass": "mypass"})
async def test_require_pass(df_factory):
    df = df_factory.create()
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
    acl_file_content = "USER roy ON #ea71c25a7a602246b4c39824b855678894a96f43bb9b71319c39700a1e045222 +@string +@fast +hset\nUSER john on nopass +@string"

    acl = create_temp_file(acl_file_content, tmp_dir)

    await async_client.execute_command(f"CONFIG SET aclfile {acl}")

    await async_client.execute_command("ACL LOAD")

    result = await async_client.execute_command("ACL list")
    assert 3 == len(result)

    result = await async_client.execute_command("AUTH roy mypass")
    assert result == "OK"

    result = await async_client.execute_command("AUTH john nopass")
    assert result == "OK"


@pytest.mark.asyncio
@dfly_args({"proactor_threads": 1})
async def test_set_len_acl_log(async_client):
    res = await async_client.execute_command("ACL LOG")
    assert [] == res

    await async_client.execute_command("ACL SETUSER elon >mars ON +@string +@dangerous")

    for x in range(7):
        with pytest.raises(redis.exceptions.AuthenticationError):
            await async_client.execute_command("AUTH elon wrong")

    res = await async_client.execute_command("ACL LOG")
    assert 7 == len(res)

    await async_client.execute_command(f"CONFIG SET acllog_max_len 3")

    res = await async_client.execute_command("ACL LOG")
    assert 3 == len(res)

    await async_client.execute_command(f"CONFIG SET acllog_max_len 10")

    for x in range(7):
        with pytest.raises(redis.exceptions.AuthenticationError):
            await async_client.execute_command("AUTH elon wrong")

    res = await async_client.execute_command("ACL LOG")
    assert 10 == len(res)


@pytest.mark.asyncio
async def test_acl_keys(async_client):
    await async_client.execute_command("ACL SETUSER mrkeys ON >mrkeys allkeys +@admin")
    await async_client.execute_command("AUTH mrkeys mrkeys")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("SET foo bar")

    await async_client.execute_command(
        "ACL SETUSER mrkeys ON >mrkeys resetkeys +@string ~foo ~bar* ~dr*gon"
    )

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("SET random rand")

    assert "OK" == await async_client.execute_command("SET foo val")
    assert "OK" == await async_client.execute_command("SET bar val")
    assert "OK" == await async_client.execute_command("SET barsomething val")
    assert "OK" == await async_client.execute_command("SET dragon val")

    await async_client.execute_command("ACL SETUSER mrkeys ON >mrkeys allkeys +@sortedset")
    assert "OK" == await async_client.execute_command("SET random rand")

    await async_client.execute_command(
        "ACL SETUSER mrkeys ON >mrkeys resetkeys resetkeys %R~foo %W~bar"
    )

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("SET foo val")
    assert "val" == await async_client.execute_command("GET foo")

    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("GET bar")
    assert "OK" == await async_client.execute_command("SET bar val")

    await async_client.execute_command("ACL SETUSER mrkeys resetkeys ~bar* +@sortedset")
    assert 1 == await async_client.execute_command("ZADD barz1 1 val1")
    assert 1 == await async_client.execute_command("ZADD barz2 1 val2")
    # reject because bonus key does not match
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("ZUNIONSTORE destkey 2 barz1 barz2")


@pytest.mark.asyncio
async def test_namespaces(df_factory):
    df = df_factory.create()
    df.start()

    admin = aioredis.Redis(port=df.port)
    assert await admin.execute_command("SET foo admin") == b"OK"
    assert await admin.execute_command("GET foo") == b"admin"

    # Create ns space named 'ns1'
    await admin.execute_command("ACL SETUSER adi NS:ns1 ON >adi_pass +@all ~*")

    adi = aioredis.Redis(port=df.port)
    assert await adi.execute_command("AUTH adi adi_pass") == b"OK"
    assert await adi.execute_command("SET foo bar") == b"OK"
    assert await adi.execute_command("GET foo") == b"bar"
    assert await admin.execute_command("GET foo") == b"admin"

    # Adi and Shahar are on the same team
    await admin.execute_command("ACL SETUSER shahar NS:ns1 ON >shahar_pass +@all ~*")

    shahar = aioredis.Redis(port=df.port)
    assert await shahar.execute_command("AUTH shahar shahar_pass") == b"OK"
    assert await shahar.execute_command("GET foo") == b"bar"
    assert await shahar.execute_command("SET foo bar2") == b"OK"
    assert await adi.execute_command("GET foo") == b"bar2"

    # Roman is a CTO, he has his own private space
    await admin.execute_command("ACL SETUSER roman NS:ns2 ON >roman_pass +@all ~*")

    roman = aioredis.Redis(port=df.port)
    assert await roman.execute_command("AUTH roman roman_pass") == b"OK"
    assert await roman.execute_command("GET foo") == None

    await close_clients(admin, adi, shahar, roman)


@pytest.mark.asyncio
async def default_user_bug(df_factory):
    df.start()

    client = aioredis.Redis(port=df.port)

    await async_client.execute_command("ACL SETUSER default -@all")
    await client.close()

    client = aioredis.Redis(port=df.port)

    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command("SET foo bar")

    await client.close()
