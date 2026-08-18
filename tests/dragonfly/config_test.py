import asyncio
import os
import socket
import sys

import pytest
from redis.asyncio import Redis as RedisClient

import redis

SO_INCOMING_CPU = 49


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


def incoming_cpu_supported(port) -> bool:
    s = socket.socket()
    try:
        s.connect(("localhost", port))
        s.getsockopt(socket.SOL_SOCKET, SO_INCOMING_CPU)
        return True
    except OSError:
        return False
    finally:
        s.close()


async def new_conn_tid(server) -> int:
    # CLIENT INFO exposes the proactor thread ("tid=N") the connection landed on.
    async with server.client() as c:
        info = await c.execute_command("CLIENT INFO")
        if not isinstance(info, dict):  # raw reply, redis-py version dependent
            info = dict(field.split("=", 1) for field in str(info).split())
        return int(info["tid"])


async def test_conn_use_incoming_cpu_placement(df_factory):
    """CONFIG SET conn_use_incoming_cpu must change where new connections are placed.

    conn_io_thread_start/conn_io_threads restrict the round-robin fallback to tids
    {2, 3}, so a connection on tid 0 can only result from the incoming-cpu policy.
    This catches a cached-flag regression: if PickConnectionProactor stopped
    re-reading the flag, every connection would stay in {2, 3} despite CONFIG SET.
    """
    if sys.platform != "linux":
        pytest.skip("SO_INCOMING_CPU placement requires Linux")

    with df_factory.create(
        proactor_threads=4,
        conn_io_thread_start=2,
        conn_io_threads=2,
        proactor_affinity_mode="on",
    ) as server:
        if not incoming_cpu_supported(server.port):
            pytest.skip("kernel does not support SO_INCOMING_CPU")

        async with server.client() as client:
            initial = (await client.execute_command("CONFIG GET conn_use_incoming_cpu"))[1]

            await client.execute_command("CONFIG SET conn_use_incoming_cpu false")
            assert await new_conn_tid(server) in (2, 3)

            await client.execute_command("CONFIG SET conn_use_incoming_cpu true")
            # Pin this process to the first allowed CPU: loopback packets are
            # processed on the sender's CPU, and proactor thread 0 is pinned to that
            # same CPU (first CPU of the allowed set), so the incoming-cpu policy
            # must place a fresh connection on tid 0 - outside the fallback window.
            # A few attempts absorb occasional kernel steering noise; a cached flag
            # yields zero hits no matter how many attempts.
            allowed = os.sched_getaffinity(0)
            try:
                os.sched_setaffinity(0, {min(allowed)})
                tids = [await new_conn_tid(server) for _ in range(10)]
            finally:
                os.sched_setaffinity(0, allowed)
            assert 0 in tids, f"no connection was steered by incoming cpu: {tids}"

            await client.execute_command("CONFIG SET conn_use_incoming_cpu false")
            assert await new_conn_tid(server) in (2, 3)

            await client.execute_command("CONFIG SET conn_use_incoming_cpu", initial)


async def test_conn_use_incoming_cpu(df_factory):
    with df_factory.create(proactor_threads=4) as server:
        async with server.client() as client:
            # Do not assume the startup value; it can be overridden with --df.
            initial = (await client.execute_command("CONFIG GET conn_use_incoming_cpu"))[1]
            assert initial in ("true", "false")
            flipped = "true" if initial == "false" else "false"

            # Connections established before the toggles. Placement is decided once,
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
                    value = flipped if i % 2 == 0 else initial
                    assert "OK" == await client.execute_command(
                        "CONFIG SET conn_use_incoming_cpu", value
                    )

            # Race new-connection creation against runtime toggles of the placement
            # policy: every accept must take a valid placement path either way.
            await asyncio.gather(*(churn_connections(w) for w in range(4)), toggle_flag())

            # Connections that predate the toggles still serve traffic and kept state.
            for i, c in enumerate(persistent):
                assert f"value:{i}" == await c.get(f"persistent:{i}")

            # Restore the startup value and verify the round-trip.
            await client.execute_command("CONFIG SET conn_use_incoming_cpu", initial)
            assert ["conn_use_incoming_cpu", initial] == await client.execute_command(
                "CONFIG GET conn_use_incoming_cpu"
            )
