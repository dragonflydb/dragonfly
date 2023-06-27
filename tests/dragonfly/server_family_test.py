import pytest
import redis
from .utility import *


def test_quit(connection):
    connection.send_command("QUIT")
    assert connection.read_response() == b'OK'

    with pytest.raises(redis.exceptions.ConnectionError) as e:
        connection.read_response()


def test_quit_after_sub(connection):
    connection.send_command("SUBSCRIBE", "foo")
    connection.read_response()

    connection.send_command("QUIT")
    assert connection.read_response() == b'OK'

    with pytest.raises(redis.exceptions.ConnectionError) as e:
        connection.read_response()


async def test_multi_exec(async_client: aioredis.Redis):
    pipeline = async_client.pipeline()
    pipeline.set("foo", "bar")
    pipeline.get("foo")
    val = await pipeline.execute()
    assert val == [True, "bar"]


'''
see https://github.com/dragonflydb/dragonfly/issues/457
For now we would not allow for eval command inside multi
As this would create to level transactions (in effect recursive call
to Schedule function).
When this issue is fully fixed, this test would failed, and then it should
change to match the fact that we supporting this operation.
For now we are expecting to get an error
'''


async def test_multi_eval(async_client: aioredis.Redis):
    try:
        pipeline = async_client.pipeline()
        pipeline.set("foo", "bar")
        pipeline.get("foo")
        pipeline.eval("return 43", 0)
        val = await pipeline.execute()
        assert val == "foo"
    except Exception as e:
        msg = str(e)
        assert "Dragonfly does not allow execution of" in msg


async def test_connection_name(async_client: aioredis.Redis):
    name = await async_client.execute_command("CLIENT GETNAME")
    assert name == "default-async-fixture"
    await async_client.execute_command("CLIENT SETNAME test_conn_name")
    name = await async_client.execute_command("CLIENT GETNAME")
    assert name == "test_conn_name"


async def test_client_list(df_factory):
    instance = df_factory.create(port=1111, admin_port=1112)
    instance.start()
    async with (aioredis.Redis(port=instance.port) as client, aioredis.Redis(port=instance.admin_port) as admin_client):
        await client.ping()
        await admin_client.ping()
        assert len(await client.execute_command("CLIENT LIST")) == 2
        assert len(await admin_client.execute_command("CLIENT LIST")) == 2
    instance.stop()


async def test_scan(async_client: aioredis.Redis):
    '''
    make sure that the scan command is working with python
    '''
    def gen_test_data():
        for i in range(10):
            yield f"key-{i}", f"value-{i}"

    for key, val in gen_test_data():
        res = await async_client.set(key, val)
        assert res is not None
        cur, keys = await async_client.scan(cursor=0, match=key, count=2)
        assert cur == 0
        assert len(keys) == 1
        assert keys[0] == key
