import aiohttp
import json
from . import dfly_args
from .instance import DflyInstance


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
    client = df_server.client()
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
