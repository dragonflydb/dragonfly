import pytest
import logging
import os
import glob
import asyncio
import async_timeout
import redis
from redis import asyncio as aioredis
from pathlib import Path
import boto3
from .instance import RedisServer

from . import dfly_args
from .utility import wait_available_async, chunked, is_saving

from .seeder import StaticSeeder

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/", "proactor_threads": 4}
FILE_FORMATS = ["RDB", "DF"]

# Should be used where text auxiliary mechanisms like filenames
LIGHTWEIGHT_SEEDER_ARGS = dict(key_target=100, data_size=100, variance=1, samples=1)


def find_main_file(path: Path, pattern):
    return next(iter(glob.glob(str(path) + "/" + pattern)), None)


@pytest.mark.opt_only
@pytest.mark.parametrize("format", FILE_FORMATS)
@pytest.mark.parametrize(
    "seeder_opts",
    [
        # Many small keys, high variance
        dict(key_target=50_000, data_size=100, variance=10, samples=50),
        # A few large keys, high variance
        dict(key_target=1000, data_size=5_000, variance=10, samples=10),
    ],
)
@dfly_args({**BASIC_ARGS, "proactor_threads": 4, "dbfilename": "test-consistency"})
async def test_consistency(async_client: aioredis.Redis, format: str, seeder_opts: dict):
    """
    Test consistency over a large variety of data with different sizes
    """
    await StaticSeeder(**seeder_opts).run(async_client)

    start_capture = await StaticSeeder.capture(async_client)

    # save + flush + load
    await async_client.execute_command("SAVE", format)
    assert await async_client.flushall()
    await async_client.execute_command(
        "DEBUG",
        "LOAD",
        "test-consistency.rdb" if format == "RDB" else "test-consistency-summary.dfs",
    )

    assert (await StaticSeeder.capture(async_client)) == start_capture


@pytest.mark.parametrize("format", FILE_FORMATS)
@dfly_args({**BASIC_ARGS, "proactor_threads": 4, "dbfilename": "test-multidb"})
async def test_multidb(async_client: aioredis.Redis, df_server, format: str):
    """
    Test serialization of multiple logical databases
    """
    start_captures = []
    for dbid in range(10):
        db_client = df_server.client(db=dbid)
        await StaticSeeder(key_target=1000).run(db_client)
        start_captures.append(await StaticSeeder.capture(db_client))

    # save + flush + load
    await async_client.execute_command("SAVE", format)
    assert await async_client.flushall()
    await async_client.execute_command(
        "DEBUG",
        "LOAD",
        "test-multidb.rdb" if format == "RDB" else "test-multidb-summary.dfs",
    )

    for dbid in range(10):
        db_client = df_server.client(db=dbid)
        assert (await StaticSeeder.capture(db_client)) == start_captures[dbid]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "save_type, dbfilename, pattern",
    [
        ("rdb", "test-autoload1-{{timestamp}}", "test-autoload1-*.rdb"),
        ("df", "test-autoload2-{{timestamp}}", "test-autoload2-*-summary.dfs"),
        ("rdb", "test-autoload3-{{timestamp}}.rdb", "test-autoload3-*.rdb"),
        ("rdb", "test-autoload4", "test-autoload4.rdb"),
        ("df", "test-autoload5", "test-autoload5-summary.dfs"),
        ("rdb", "test-autoload6.rdb", "test-autoload6.rdb"),
    ],
)
async def test_dbfilenames(
    df_local_factory, tmp_dir: Path, save_type: str, dbfilename: str, pattern: str
):
    df_args = {**BASIC_ARGS, "dbfilename": dbfilename, "port": 1111}

    if save_type == "rdb":
        df_args["nodf_snapshot_format"] = None

    start_capture = None

    with df_local_factory.create(**df_args) as df_server:
        async with df_server.client() as client:
            await wait_available_async(client)

            # We use the seeder just to check we don't loose any files (and thus keys)
            await StaticSeeder(**LIGHTWEIGHT_SEEDER_ARGS).run(client)
            start_capture = await StaticSeeder.capture(client)

            await client.execute_command("SAVE " + save_type)

    file = find_main_file(tmp_dir, pattern)
    assert file is not None
    assert os.path.basename(file).startswith(dbfilename.split("{{")[0])

    with df_local_factory.create(**df_args) as df_server:
        async with df_server.client() as client:
            await wait_available_async(client)
            assert await StaticSeeder.capture(client) == start_capture


@pytest.mark.asyncio
@dfly_args({**BASIC_ARGS, "proactor_threads": 4, "dbfilename": "test-redis-load-rdb"})
async def test_redis_load_snapshot(
    async_client: aioredis.Redis, df_server, redis_local_server: RedisServer, tmp_dir: Path
):
    """
    Test redis server loading dragonfly snapshot rdb format
    """
    await StaticSeeder(
        **LIGHTWEIGHT_SEEDER_ARGS, types=["STRING", "LIST", "SET", "HASH", "ZSET"]
    ).run(async_client)

    await async_client.execute_command("SAVE", "rdb")
    dbsize = await async_client.dbsize()

    await async_client.connection_pool.disconnect()
    df_server.stop()

    redis_local_server.start(dir=tmp_dir, dbfilename="test-redis-load-rdb.rdb")
    await asyncio.sleep(1)
    c_master = aioredis.Redis(port=redis_local_server.port)
    await c_master.ping()

    assert await c_master.dbsize() == dbsize


@pytest.mark.slow
@dfly_args({**BASIC_ARGS, "dbfilename": "test-cron", "snapshot_cron": "* * * * *"})
async def test_cron_snapshot(tmp_dir: Path, async_client: aioredis.Redis):
    await StaticSeeder(**LIGHTWEIGHT_SEEDER_ARGS).run(async_client)

    file = None
    with async_timeout.timeout(65):
        while file is None:
            await asyncio.sleep(1)
            file = find_main_file(tmp_dir, "test-cron-summary.dfs")

    assert file is not None, os.listdir(tmp_dir)


@pytest.mark.slow
@dfly_args({**BASIC_ARGS, "dbfilename": "test-cron-set"})
async def test_set_cron_snapshot(tmp_dir: Path, async_client: aioredis.Redis):
    await StaticSeeder(**LIGHTWEIGHT_SEEDER_ARGS).run(async_client)

    await async_client.config_set("snapshot_cron", "* * * * *")

    file = None
    with async_timeout.timeout(65):
        while file is None:
            await asyncio.sleep(1)
            file = find_main_file(tmp_dir, "test-cron-set-summary.dfs")

    assert file is not None


@dfly_args(
    {**BASIC_ARGS, "dbfilename": "test-save-rename-command", "rename_command": "save=save-foo"}
)
async def test_shutdown_save_with_rename(df_server):
    """Checks that on shutdown we save snapshot"""
    client = df_server.client()

    await StaticSeeder(**LIGHTWEIGHT_SEEDER_ARGS).run(client)
    start_capture = await StaticSeeder.capture(client)

    await client.connection_pool.disconnect()
    df_server.stop()
    df_server.start()
    client = df_server.client()

    await wait_available_async(client)
    assert await StaticSeeder.capture(client) == start_capture

    await client.connection_pool.disconnect()


@pytest.mark.slow
async def test_parallel_snapshot(async_client):
    """Dragonfly does not allow simultaneous save operations, send 2 save operations and make sure one is rejected"""

    await async_client.execute_command("debug", "populate", "1000000", "askldjh", "1000", "RAND")

    async def save():
        try:
            await async_client.execute_command("save", "rdb", "dump")
            return True
        except Exception as e:
            return False

    save_successes = sum(await asyncio.gather(*(save() for _ in range(2))), 0)
    assert save_successes == 1, "Only one SAVE must be successful"


async def test_path_escapes(df_local_factory):
    """Test that we don't allow path escapes. We just check that df_server.start()
    fails because we don't have a much better way to test that."""

    df_server = df_local_factory.create(dbfilename="../../../../etc/passwd")
    try:
        df_server.start()
        assert False, "Server should not start correctly"
    except Exception as e:
        pass


@dfly_args({**BASIC_ARGS, "dbfilename": "test-info-persistence"})
async def test_info_persistence_field(async_client):
    """Test is_loading field on INFO PERSISTENCE during snapshot loading"""

    await StaticSeeder(**LIGHTWEIGHT_SEEDER_ARGS).run(async_client)

    # Wait for snapshot to finish loading and try INFO PERSISTENCE
    await wait_available_async(async_client)
    assert "loading:0" in (await async_client.execute_command("INFO PERSISTENCE"))


# If DRAGONFLY_S3_BUCKET is configured, AWS credentials must also be
# configured.
@pytest.mark.skipif(
    "DRAGONFLY_S3_BUCKET" not in os.environ, reason="AWS S3 snapshots bucket is not configured"
)
@dfly_args({**BASIC_ARGS, "dir": "s3://{DRAGONFLY_S3_BUCKET}{DRAGONFLY_TMP}", "dbfilename": ""})
async def test_s3_snapshot(self, async_client):
    seeder = StaticSeeder(key_target=10_000)
    await seeder.run(async_client)

    start_capture = await StaticSeeder.capture()

    try:
        # save + flush + load
        await async_client.execute_command("SAVE DF snapshot")
        assert await async_client.flushall()
        await async_client.execute_command(
            "DEBUG LOAD "
            + os.environ["DRAGONFLY_S3_BUCKET"]
            + str(self.tmp_dir)
            + "/snapshot-summary.dfs"
        )

        assert await StaticSeeder.capture(async_client) == start_capture

    finally:

        def delete_objects(bucket, prefix):
            client = boto3.client("s3")
            resp = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
            )
            keys = []
            for obj in resp["Contents"]:
                keys.append({"Key": obj["Key"]})
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": keys},
            )

        delete_objects(
            os.environ["DRAGONFLY_S3_BUCKET"],
            str(self.tmp_dir)[1:],
        )


@dfly_args({**BASIC_ARGS, "dbfilename": "test-shutdown"})
class TestDflySnapshotOnShutdown:
    SEEDER_ARGS = dict(key_target=10_000)

    """Test multi file snapshot"""

    async def _get_info_memory_fields(self, client):
        res = await client.execute_command("INFO MEMORY")
        fields = {}
        for line in res.splitlines():
            if line.startswith("#"):
                continue
            k, v = line.split(":")
            if k == "object_used_memory" or k.startswith("type_used_memory_"):
                fields.update({k: int(v)})
        return fields

    async def _delete_all_keys(self, client: aioredis.Redis):
        while True:
            keys = await client.keys()
            if len(keys) == 0:
                break
            await client.delete(*keys)

    @pytest.mark.asyncio
    async def test_memory_counters(self, async_client: aioredis.Redis):
        memory_counters = await self._get_info_memory_fields(async_client)
        assert memory_counters == {"object_used_memory": 0}

        seeder = StaticSeeder(**self.SEEDER_ARGS)
        await seeder.run(async_client)

        memory_counters = await self._get_info_memory_fields(async_client)
        assert all(value > 0 for value in memory_counters.values())

        await self._delete_all_keys(async_client)
        memory_counters = await self._get_info_memory_fields(async_client)
        assert memory_counters == {"object_used_memory": 0}

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_server, async_client):
        """Checks that:
        1. After reloading the snapshot file the data is the same
        2. Memory counters after loading from snapshot is similar to before creating a snapshot
        3. Memory counters after deleting all keys loaded by snapshot - this validates the memory
           counting when loading from snapshot."""
        seeder = StaticSeeder(**self.SEEDER_ARGS)
        await seeder.run(async_client)
        start_capture = await StaticSeeder.capture(async_client)

        memory_before = await self._get_info_memory_fields(async_client)

        await async_client.connection_pool.disconnect()
        df_server.stop()
        df_server.start()

        async_client = df_server.client()
        await wait_available_async(async_client)

        assert await StaticSeeder.capture(async_client) == start_capture

        memory_after = await self._get_info_memory_fields(async_client)
        for counter, value in memory_before.items():
            # Unfortunately memory usage sometimes depends on order of insertion / deletion, so
            # it's usually not exactly the same. For the test to be stable we check that it's
            # at least 50% that of the original value.
            assert memory_after[counter] >= 0.5 * value

        await self._delete_all_keys(async_client)
        memory_empty = await self._get_info_memory_fields(async_client)
        assert memory_empty == {"object_used_memory": 0}


@pytest.mark.parametrize("format", FILE_FORMATS)
@dfly_args({**BASIC_ARGS, "dbfilename": "info-while-snapshot"})
async def test_infomemory_while_snapshoting(async_client: aioredis.Redis, format: str):
    await async_client.execute_command("DEBUG POPULATE 10000 key 4048 RAND")

    async def save():
        await async_client.execute_command("SAVE", format)

    save_finished = False

    async def info_in_loop():
        while not save_finished:
            await async_client.execute_command("INFO MEMORY")
            await asyncio.sleep(0.1)

    save_task = asyncio.create_task(save())
    info_task = asyncio.create_task(info_in_loop())

    await save_task
    save_finished = True
    await info_task


@dfly_args({**BASIC_ARGS, "dbfilename": "test-bgsave"})
async def test_bgsave_and_save(async_client: aioredis.Redis):
    await async_client.execute_command("DEBUG POPULATE 20000")

    await async_client.execute_command("BGSAVE")
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("BGSAVE")

    while await is_saving(async_client):
        await asyncio.sleep(0.1)
    await async_client.execute_command("BGSAVE")
    with pytest.raises(redis.exceptions.ResponseError):
        await async_client.execute_command("SAVE")

    while await is_saving(async_client):
        await asyncio.sleep(0.1)
    await async_client.execute_command("SAVE")
