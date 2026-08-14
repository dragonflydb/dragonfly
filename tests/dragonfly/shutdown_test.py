import asyncio

import pytest

import redis
from redis import asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from . import dfly_args
from .instance import DflyInstanceFactory
from .utility import wait_available_async

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}


@dfly_args({"proactor_threads": "4"})
class TestGracefulShutdown:
    """
    SHUTDOWN saves the snapshot only after all client connections are torn down, so
    every acknowledged write must survive the restart. The reply to a single in-flight
    command may be lost when the connection is severed, so the restored value may
    exceed the last acknowledged one by at most 1.
    """

    @pytest.mark.asyncio
    async def test_shutdown_snapshot_contains_acknowledged_writes(self, df_factory):
        df_server = df_factory.create(dbfilename="dump", **BASIC_ARGS)
        df_server.start()

        def make_client():
            # One pinned connection, no retries: a lost reply must surface as
            # ConnectionError instead of a silent re-send. Clients are registered
            # on the instance, so fixture teardown closes them on any failure.
            return df_server.client(
                single_connection_client=True,
                retry=Retry(NoBackoff(), 0),
                retry_on_error=[],
            )

        control = make_client()
        workers = [make_client() for _ in range(16)]

        # All SETs are acknowledged before the shutdown starts, so no key may be
        # missing after the restart.
        await asyncio.gather(*(w.set(f"key{i}", 0) for i, w in enumerate(workers)))

        async def counter(i, client):
            key, ack = f"key{i}", 0
            while True:
                try:
                    ack = await client.incr(key)
                except redis.exceptions.ConnectionError:
                    return key, ack, "connection closed"
                except redis.exceptions.ResponseError:
                    return key, ack, "command rejected"

        tasks = [asyncio.create_task(counter(i, w)) for i, w in enumerate(workers)]
        try:
            await asyncio.sleep(1)
            try:
                await control.execute_command("SHUTDOWN")
            except redis.exceptions.ConnectionError:
                pass  # the reply may be cut off together with the connection
            results = await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        df_server.wait()  # the snapshot is written before the process exits
        df_server.start()

        client = df_server.client()
        await wait_available_async(client)
        for key, acknowledged, reason in results:
            restored = await client.get(key)
            assert restored is not None, f"{key} missing after restart ({reason})"
            assert (
                acknowledged <= int(restored) <= acknowledged + 1
            ), f"{key}: acknowledged={acknowledged} restored={int(restored)} ({reason})"
        df_server.stop()


@dfly_args({"proactor_threads": "2"})
class TestShutdownOptions:
    @pytest.mark.asyncio
    async def test_shutdown_abort_and_invalid_option(self, df_factory):
        df_args = {"dbfilename": "dump", **BASIC_ARGS, "port": 1121}
        df_server = df_factory.create(**df_args)
        df_server.start()

        client = aioredis.Redis(port=df_server.port)

        # ABORT should be rejected and server should remain responsive
        with pytest.raises(redis.exceptions.ResponseError):
            await client.execute_command("SHUTDOWN ABORT")

        pong = await client.ping()
        assert pong is True

        # Invalid option -> syntax error
        with pytest.raises(redis.exceptions.ResponseError):
            await client.execute_command("SHUTDOWN FOO")

        await client.connection_pool.disconnect()
        df_server.stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("flavour", ["SAVE", "SAFE"])  # valkey uses SAFE instead of SAVE
    async def test_shutdown_save_persists_snapshot(
        self, df_factory: DflyInstanceFactory, tmp_path, flavour
    ):
        # Ensure snapshot dir exists and is used
        snap_dir = tmp_path
        df_args = {"dbfilename": "dump", "dir": str(snap_dir) + "/", "port": 1122}

        df_server = df_factory.create(**df_args)
        df_server.start()

        client = aioredis.Redis(port=df_server.port)
        await client.set("key", "value")

        # SHUTDOWN SAVE/SAFE should save synchronously and then stop
        try:
            await client.execute_command("SHUTDOWN", flavour)
        except Exception as e:
            print(e)
            # Connection may be dropped as part of shutdown; this is acceptable

        await client.connection_pool.disconnect()

        df_server.wait()
        lines = df_server.find_in_logs("Exit SnapshotSerializer")
        assert len(lines) == 1
        assert (
            "Exit SnapshotSerializer total_serialized: 1, buckets side saved 0, total bucket saved 1, journal_saved 0"
            in lines[0]
        )

        # Restart and verify data persisted
        df_server.start()
        client = aioredis.Redis(port=df_server.port)
        await wait_available_async(client)
        val = await client.get("key")
        assert val == b"value"
        await client.connection_pool.disconnect()
        df_server.stop()
