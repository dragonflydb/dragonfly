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


async def test_client_kill(df_factory):
    with df_factory.create(port=1111, admin_port=1112) as instance:
        client = aioredis.Redis(port=instance.port)
        admin_client = aioredis.Redis(port=instance.admin_port)
        await admin_client.ping()

        # This creates `client_conn` as a non-auto-reconnect client
        async with client.client() as client_conn:
            assert len(await client_conn.execute_command("CLIENT LIST")) == 2
            assert len(await admin_client.execute_command("CLIENT LIST")) == 2

            # Can't kill admin from regular connection
            with pytest.raises(Exception) as e_info:
                await client_conn.execute_command("CLIENT KILL LADDR 127.0.0.1:1112")

            assert len(await admin_client.execute_command("CLIENT LIST")) == 2
            await admin_client.execute_command("CLIENT KILL LADDR 127.0.0.1:1111")
            assert len(await admin_client.execute_command("CLIENT LIST")) == 1
            with pytest.raises(Exception) as e_info:
                await client_conn.ping()


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


def configure_slowlog_parsing(async_client: aioredis.Redis):
    def parse_slowlog_get(response, **options):
        logging.info(f"slowlog response: {response}")

        def stringify(item):
            if isinstance(item, bytes):
                return item.decode()
            if isinstance(item, list):
                return [stringify(i) for i in item]
            return item

        def parse_item(item):
            item = stringify(item)
            result = {"id": item[0], "start_time": int(item[1]), "duration": int(item[2])}
            result["command"] = " ".join(item[3])
            result["client_address"] = item[4]
            result["client_name"] = item[5]
            return result

        return [parse_item(item) for item in response]

    async_client.set_response_callback("SLOWLOG GET", parse_slowlog_get)
    return async_client


@pytest.mark.asyncio
@dfly_args({"slowlog_log_slower_than": 0, "slowlog_max_len": 3})
async def test_slowlog_client_name_and_ip(df_factory, async_client: aioredis.Redis):
    df = df_factory.create()
    df.start()
    expected_clientname = "dragonfly"

    await async_client.client_setname(expected_clientname)
    async_client = configure_slowlog_parsing(async_client)

    client_list = await async_client.client_list()
    addr = client_list[0]["addr"]

    slowlog = await async_client.slowlog_get(1)
    assert slowlog[0]["client_name"] == expected_clientname
    assert slowlog[0]["client_address"] == addr


@pytest.mark.asyncio
@dfly_args({"slowlog_log_slower_than": 0, "slowlog_max_len": 3})
async def test_blocking_commands_should_not_show_up_in_slow_log(
    df_factory, async_client: aioredis.Redis
):
    await async_client.slowlog_reset()
    df = df_factory.create()
    df.start()
    async_client = configure_slowlog_parsing(async_client)

    await async_client.blpop("mykey", 0.5)
    reply = await async_client.slowlog_get()

    # blpop does not show up, only the previous reset
    assert reply[0]["command"] == "SLOWLOG RESET"
