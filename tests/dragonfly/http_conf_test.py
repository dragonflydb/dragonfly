import json
import asyncio
import aiohttp

from . import dfly_args
from .instance import DflyInstance
from .utility import info_tick_timer


def get_http_session(*args):
    if args:
        return aiohttp.ClientSession(auth=aiohttp.BasicAuth(*args))
    return aiohttp.ClientSession()


@dfly_args({"proactor_threads": "1", "requirepass": "XXX"})
async def test_password(df_server: DflyInstance):
    async with get_http_session() as session:
        resp = await session.get(f"http://localhost:{df_server.port}/")
        assert resp.status == 401
    async with get_http_session("default", "wrongpassword") as session:
        resp = await session.get(f"http://localhost:{df_server.port}/")
        assert resp.status == 401
    async with get_http_session("default", "XXX") as session:
        resp = await session.get(f"http://localhost:{df_server.port}/")
        assert resp.status == 200


@dfly_args({"proactor_threads": "1", "requirepass": "XXX", "admin_port": 1113})
async def test_skip_metrics(df_server: DflyInstance):
    async with get_http_session("whoops", "whoops") as session:
        resp = await session.get(f"http://localhost:{df_server.port}/metrics")
        assert resp.status == 200
    async with get_http_session("whoops", "whoops") as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/metrics")
        assert resp.status == 200


@dfly_args({"proactor_threads": "1"})
async def test_metrics_does_not_count_http_connection(df_server: DflyInstance):
    """Verify that a metrics request does not count its HTTP connection as a client."""
    async with get_http_session() as session:
        async with session.get(f"http://localhost:{df_server.port}/metrics") as resp:
            assert resp.status == 200
            metrics = await resp.text()

    assert 'dragonfly_connected_clients{listener="main"} 0' in metrics


@dfly_args({"proactor_threads": "1"})
async def test_http_metrics_does_not_leak_read_buffer_capacity(df_server: DflyInstance):
    """Verify pre-registration HTTP reads do not leak read-buffer capacity."""
    observer = df_server.client()  # ordinary RESP client
    baseline_read_buffer_bytes = int((await observer.info("clients"))["client_read_buffer_bytes"])

    reader, writer = await asyncio.open_connection(
        "localhost", df_server.port
    )  # Raw asyncio TCP stream (Manually constructed HTTP)
    writer.write(b"GET /" + (b"a" * 200))
    await writer.drain()
    await asyncio.sleep(0.1)
    writer.write(b" HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    await writer.drain()
    response = await reader.readuntil(b"\r\n\r\n")
    assert response.startswith(b"HTTP/1.1 ")

    writer.close()
    await writer.wait_closed()

    # The response can reach the client before the server finishes cleaning up its read buffer.
    # Wait for the metric to reflect that cleanup before checking for a leak.
    async for info, breaker in info_tick_timer(observer, section="clients"):
        with breaker:
            assert int(info["client_read_buffer_bytes"]) == baseline_read_buffer_bytes

    await observer.aclose()


async def test_no_password_main_port(df_server: DflyInstance):
    async with get_http_session("default", "XXX") as session:
        resp = await session.get(f"http://localhost:{df_server.port}/")
        assert resp.status == 200
    async with get_http_session("random") as session:
        resp = await session.get(f"http://localhost:{df_server.port}/")
        assert resp.status == 200
    async with get_http_session() as session:
        resp = await session.get(f"http://localhost:{df_server.port}/")
        assert resp.status == 200


@dfly_args(
    {
        "proactor_threads": "1",
        "requirepass": "XXX",
        "admin_port": 1113,
        "primary_port_http_enabled": True,
        "admin_nopass": True,
    }
)
async def test_no_password_on_admin(df_server: DflyInstance):
    async with get_http_session("default", "XXX") as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/")
        assert resp.status == 200
    async with get_http_session("random") as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/")
        assert resp.status == 200
    async with get_http_session() as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/")
        assert resp.status == 200


@dfly_args({"proactor_threads": "1", "requirepass": "XXX", "admin_port": 1113})
async def test_password_on_admin(df_server: DflyInstance):
    async with get_http_session("default", "badpass") as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/")
        assert resp.status == 401
    async with get_http_session() as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/")
        assert resp.status == 401
    async with get_http_session("default", "XXX") as session:
        resp = await session.get(f"http://localhost:{df_server.admin_port}/")
        assert resp.status == 200


@dfly_args({"proactor_threads": "1", "expose_http_api": "true"})
async def test_no_password_on_http_api(df_server: DflyInstance):
    async with get_http_session("default", "XXX") as session:
        resp = await session.post(f"http://localhost:{df_server.port}/api", json=["ping"])
        assert resp.status == 200
    async with get_http_session("random") as session:
        resp = await session.post(f"http://localhost:{df_server.port}/api", json=["ping"])
        assert resp.status == 200
    async with get_http_session() as session:
        resp = await session.post(f"http://localhost:{df_server.port}/api", json=["ping"])
        assert resp.status == 200


@dfly_args({"proactor_threads": "1", "expose_http_api": "true"})
async def test_http_api(df_server: DflyInstance):
    client = df_server.client()
    async with get_http_session() as session:
        body = '["set", "foo", "МайяХилли", "ex", "100"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            assert text.strip() == '{"result":"OK"}'

        body = '["get", "foo"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            assert text.strip() == '{"result":"МайяХилли"}'

        body = '["foo", "bar"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            assert text.strip() == '{"error": "unknown command `FOO`"}'

    assert await client.ttl("foo") > 0


@dfly_args({"proactor_threads": "1", "expose_http_api": "true", "requirepass": "XXX"})
async def test_password_on_http_api(df_server: DflyInstance):
    async with get_http_session("default", "badpass") as session:
        resp = await session.post(f"http://localhost:{df_server.port}/api", json=["ping"])
        assert resp.status == 401
    async with get_http_session() as session:
        resp = await session.post(f"http://localhost:{df_server.port}/api", json=["ping"])
        assert resp.status == 401
    async with get_http_session("default", "XXX") as session:
        resp = await session.post(f"http://localhost:{df_server.port}/api", json=["ping"])
        assert resp.status == 200


def get_json_object(json_str):
    try:
        json_obj = json.loads(json_str)
        return json_obj
    except ValueError:
        return None


@dfly_args({"proactor_threads": "1", "expose_http_api": "true", "slowlog_log_slower_than": 0})
async def test_http_api_json_response(df_server: DflyInstance):
    async with get_http_session() as session:
        body = '["set", "foo","bar"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            json_object = get_json_object(text)
            assert json_object != None
            assert json_object == {"result": "OK"}

        body = '["get", "foo"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            json_object = get_json_object(text)
            assert json_object != None
            assert json_object == {"result": "bar"}

        body = '["slowlog", "get"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            json_object = get_json_object(text)
            assert json_object != None
            # Compare commands
            assert json_object["result"][0][3] == ["GET", "foo"]
            assert json_object["result"][1][3] == ["SET", "foo", "bar"]

        body = '["hset", "myhash", "k1", "1", "k2", "2"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            json_object = get_json_object(text)
            assert json_object != None
            assert json_object == {"result": 2}

        body = '["hkeys", "myhash"]'
        async with session.post(f"http://localhost:{df_server.port}/api", data=body) as resp:
            assert resp.status == 200
            text = await resp.text()
            json_object = get_json_object(text)
            assert json_object != None
            assert json_object["result"] == ["k1", "k2"]
