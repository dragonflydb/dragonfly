import aiohttp


async def test_password(df_factory):
    with df_factory.create(port=1112, requirepass="XXX") as server:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth("default", "wrongpassword")
        ) as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("default", "XXX")) as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 200


async def test_skip_metrics(df_factory):
    with df_factory.create(port=1112, admin_port=1113, requirepass="XXX") as server:
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("whoops", "whoops")) as session:
            resp = await session.get(f"http://localhost:{server.port}/metrics")
            assert resp.status == 200
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("whoops", "whoops")) as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/metrics")
            assert resp.status == 200


async def test_no_password_on_admin(df_factory):
    with df_factory.create(
        port=1112,
        admin_port=1113,
        requirepass="XXX",
        primary_port_http_enabled=True,
        admin_nopass=None,
    ) as server:
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("default", "XXX")) as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            assert resp.status == 200
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("default")) as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            assert resp.status == 200
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            # we still need to specify the user default in the header
            assert resp.status == 401


async def test_password_on_admin(df_factory):
    with df_factory.create(
        port=1112,
        admin_port=1113,
        requirepass="XXX",
    ) as server:
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("default", "badpass")) as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("default", "XXX")) as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            assert resp.status == 200
