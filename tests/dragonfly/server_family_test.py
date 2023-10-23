import pytest
import redis

from . import dfly_args
from .utility import *


def test_quit(connection):
    connection.send_command("QUIT")
    assert connection.read_response() == b"OK"

    with pytest.raises(redis.exceptions.ConnectionError) as e:
        connection.read_response()


def test_quit_after_sub(connection):
    connection.send_command("SUBSCRIBE", "foo")
    connection.read_response()

    connection.send_command("QUIT")
    assert connection.read_response() == b"OK"

    with pytest.raises(redis.exceptions.ConnectionError) as e:
        connection.read_response()


async def test_multi_exec(async_client: aioredis.Redis):
    pipeline = async_client.pipeline()
    pipeline.set("foo", "bar")
    pipeline.get("foo")
    val = await pipeline.execute()
    assert val == [True, "bar"]


"""
see https://github.com/dragonflydb/dragonfly/issues/457
For now we would not allow for eval command inside multi
As this would create to level transactions (in effect recursive call
to Schedule function).
When this issue is fully fixed, this test would failed, and then it should
change to match the fact that we supporting this operation.
For now we are expecting to get an error
"""


@pytest.mark.skip("Skip until we decided on correct behaviour of eval inside multi")
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


async def test_get_databases(async_client: aioredis.Redis):
    """
    make sure that the config get databases command is working
    to ensure compatibility with UI frameworks like AnotherRedisDesktopManager
    """
    dbnum = await async_client.config_get("databases")
    assert dbnum == {"databases": "16"}


async def test_client_list(df_factory):
    with df_factory.create(port=1111, admin_port=1112) as instance:
        client = aioredis.Redis(port=instance.port)
        admin_client = aioredis.Redis(port=instance.admin_port)

        await client.ping()
        await admin_client.ping()
        assert len(await client.execute_command("CLIENT LIST")) == 2
        assert len(await admin_client.execute_command("CLIENT LIST")) == 2

        await disconnect_clients(client, admin_client)


async def test_scan(async_client: aioredis.Redis):
    """
    make sure that the scan command is working with python
    """

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


@pytest.mark.skip("Skip because it fails on arm release")
@pytest.mark.asyncio
@dfly_args({"slowlog_log_slower_than": 0, "slowlog_max_len": 3})
async def test_slowlog_client_name_and_ip(df_local_factory, async_client: aioredis.Redis):
    df = df_local_factory.create()
    df.start()
    expected_clientname = "dragonfly"

    await async_client.client_setname(expected_clientname)

    client_list = await async_client.client_list()
    addr = client_list[0]["addr"]

    slowlog = await async_client.slowlog_get(1)
    assert slowlog[0]["client_name"].decode() == expected_clientname
    assert slowlog[0]["client_address"].decode() == addr


@pytest.mark.skip("Skip because it fails on arm release")
@pytest.mark.asyncio
@dfly_args({"slowlog_log_slower_than": 0, "slowlog_max_len": 3})
async def test_blocking_commands_should_not_show_up_in_slow_log(
    df_local_factory, async_client: aioredis.Redis
):
    await async_client.slowlog_reset()
    df = df_local_factory.create()
    df.start()

    await async_client.blpop("mykey", 0.5)

    # blpop does not show up, only the previous reset
    assert (await async_client.slowlog_get())[0]["command"].decode() == "SLOWLOG RESET"
