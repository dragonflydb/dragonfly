import aiohttp


async def test_password(df_factory):
    # Needs a private key and certificate.
    with df_factory.create(port=1112, requirepass="XXX") as server:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth("user", "wrongpassword")
        ) as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("user", "XXX")) as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 200


async def test_no_password_on_admin(df_factory):
    # Needs a private key and certificate.
    with df_factory.create(
        port=1112,
        admin_port=1113,
        requirepass="XXX",
        noprimary_port_http_enabled=None,
        admin_nopass=None,
    ) as server:
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("user", "XXX")) as session:
            resp = await session.get(f"http://localhost:{server.admin_port}/")
            assert resp.status == 200


async def test_password_on_admin(df_factory):
    # Needs a private key and certificate.
    with df_factory.create(
        port=1112,
        admin_port=1113,
        requirepass="XXX",
        admin_nopass=None,
    ) as server:
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("user", "badpass")) as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 401
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("user", "XXX")) as session:
            resp = await session.get(f"http://localhost:{server.port}/")
            assert resp.status == 200
