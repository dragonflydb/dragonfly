import asyncio

import pytest
from redis.asyncio import Redis as RedisClient

import redis


async def test_maxclients(df_factory):
    # Needs some authentication
    with df_factory.create(port=1111, maxclients=1, admin_port=1112) as server:
        async with server.client() as client1:
            assert ["maxclients", "1"] == await client1.execute_command("CONFIG GET maxclients")

            with pytest.raises(redis.exceptions.ConnectionError):
                async with server.client() as client2:
                    await client2.get("test")

            # Check that admin connections are not limited.
            async with RedisClient(port=server.admin_port) as admin_client:
                await admin_client.get("test")

            await client1.execute_command("CONFIG SET maxclients 3")
            assert ["maxclients", "3"] == await client1.execute_command("CONFIG GET maxclients")
            async with server.client() as client2:
                await client2.get("test")


async def test_conn_use_incoming_cpu(df_factory):
    with df_factory.create(proactor_threads=4) as server:
        async with server.client() as client:
            assert ["conn_use_incoming_cpu", "false"] == await client.execute_command(
                "CONFIG GET conn_use_incoming_cpu"
            )

            # Connections established before any toggle. Placement is decided once,
            # at accept time, so these must be unaffected by later flag changes.
            persistent = [server.client() for _ in range(5)]
            for i, c in enumerate(persistent):
                await c.set(f"persistent:{i}", f"value:{i}")

            async def churn_connections(worker):
                # Every iteration opens a fresh connection, so its placement decision
                # races with the concurrent CONFIG SET flips below.
                for i in range(20):
                    async with server.client() as c:
                        key = f"churn:{worker}:{i}"
                        await c.set(key, "x")
                        assert "x" == await c.get(key)

            async def toggle_flag():
                for i in range(40):
                    value = "true" if i % 2 == 0 else "false"
                    assert "OK" == await client.execute_command(
                        "CONFIG SET conn_use_incoming_cpu", value
                    )

            # Race new-connection creation against runtime toggles of the placement
            # policy: every accept must take a valid placement path either way.
            await asyncio.gather(*(churn_connections(w) for w in range(4)), toggle_flag())

            # Connections that predate the toggles still serve traffic and kept state.
            for i, c in enumerate(persistent):
                assert f"value:{i}" == await c.get(f"persistent:{i}")

            await client.execute_command("CONFIG SET conn_use_incoming_cpu true")
            assert ["conn_use_incoming_cpu", "true"] == await client.execute_command(
                "CONFIG GET conn_use_incoming_cpu"
            )

            # A connection accepted while the policy is enabled is served normally.
            async with server.client() as c:
                await c.ping()

            await client.execute_command("CONFIG SET conn_use_incoming_cpu false")
            assert ["conn_use_incoming_cpu", "false"] == await client.execute_command(
                "CONFIG GET conn_use_incoming_cpu"
            )
