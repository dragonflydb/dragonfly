import pytest
import os
import glob
import asyncio
import async_timeout
from redis import asyncio as aioredis
from pathlib import Path
import boto3

from . import dfly_args
from .utility import wait_available_async, chunked

from .seeder import FixedSeeder

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/", "proactor_threads": 4}
FILE_FORMATS = ["RDB", "DF"]

# Should be used where text auxiliary mechanisms like filenames
LIGHTWEIGHT_SEEDER_ARGS = dict(key_target=100, data_size=100, variance=1, samples=1)


def find_main_file(path: Path, pattern):
    return next(iter(glob.glob(str(path) + "/" + pattern)), None)


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
    seeder = FixedSeeder(**seeder_opts)
    await seeder.run(async_client)

    start_capture = await FixedSeeder.capture(async_client)

    # save + flush + load
    await async_client.execute_command("SAVE", format)
    assert await async_client.flushall()
    await async_client.execute_command(
        "DEBUG",
        "LOAD",
        "test-consistency.rdb" if format == "RDB" else "test-consistency-summary.dfs",
    )

    assert (await FixedSeeder.capture(async_client)) == start_capture


@pytest.mark.parametrize("format", FILE_FORMATS)
@dfly_args({**BASIC_ARGS, "proactor_threads": 4, "dbfilename": "test-consistency"})
async def test_mutlidb(async_client: aioredis.Redis, format: str):
    """
    Test serialization of multiple logical databases
    """
    seeder = FixedSeeder(key_target=1000)

    start_captures = []
    for dbid in range(10):
        await async_client.select(dbid)
        await seeder.run(async_client)
        start_captures.append(await FixedSeeder.capture(async_client))

    # save + flush + load
    await async_client.execute_command("SAVE", format)
    assert await async_client.flushall()
    await async_client.execute_command(
        "DEBUG",
        "LOAD",
        "test-consistency.rdb" if format == "RDB" else "test-consistency-summary.dfs",
    )

    for dbid in range(10):
        await async_client.select(dbid)
        assert (await FixedSeeder.capture(async_client)) == start_captures[dbid]


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

    # We use the seeder just to check we don't loose any files (and thus keys)
    seeder = FixedSeeder(**LIGHTWEIGHT_SEEDER_ARGS)
    start_capture = None

    with df_local_factory.create(**df_args) as df_server:
        async with df_server.client() as client:
            await wait_available_async(client)

            await seeder.run(client)
            start_capture = await FixedSeeder.capture(client)

            await client.execute_command("SAVE " + save_type)

    file = find_main_file(tmp_dir, pattern)
    assert file is not None
    assert os.path.basename(file).startswith(dbfilename.split("{{")[0])

    with df_local_factory.create(**df_args) as df_server:
        async with df_server.client() as client:
            await wait_available_async(client)
            assert await FixedSeeder.capture(client) == start_capture


@pytest.mark.slow
@dfly_args({**BASIC_ARGS, "dbfilename": "test-cron", "snapshot_cron": "* * * * *"})
async def test_cron_snapshot(tmp_path: Path, async_client: aioredis.Redis):
    seeder = FixedSeeder(**LIGHTWEIGHT_SEEDER_ARGS)
    await seeder.run(async_client)

    file = None
    with async_timeout.timeout(65):
        while file is None:
            await asyncio.sleep(1)
            file = find_main_file(tmp_path, "test-cron-summary.dfs")

    assert file is not None


@pytest.mark.slow
@dfly_args({**BASIC_ARGS, "dbfilename": "test-cron-set"})
async def test_set_cron_snapshot(tmp_path: Path, async_client: aioredis.Redis):
    seeder = FixedSeeder(**LIGHTWEIGHT_SEEDER_ARGS)
    await seeder.run(async_client)

    await async_client.config_set("snapshot_cron", "* * * * *")

    file = None
    with async_timeout.timeout(65):
        while file is None:
            await asyncio.sleep(1)
            file = find_main_file(tmp_path, "test-cron-summary.dfs")

    assert file is not None


@dfly_args(
    {**BASIC_ARGS, "dbfilename": "test-save-rename-command", "rename_command": "save=save-foo"}
)
async def test_shutdown_save_with_rename(df_server):
    """Checks that on shutdown we save snapshot"""
    client = df_server.client()

    seeder = FixedSeeder(**LIGHTWEIGHT_SEEDER_ARGS)
    await seeder.run(client)
    start_capture = await FixedSeeder.capture(client)

    await client.connection_pool.disconnect()
    df_server.stop()
    df_server.start()
    client = df_server.client()

    await wait_available_async(client)
    assert await FixedSeeder.capture(client) == start_capture

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

    def extract_is_loading_field(res):
        matcher = b"loading:"
        start = res.find(matcher)
        pos = start + len(matcher)
        return chr(res[pos])

    seeder = FixedSeeder(**LIGHTWEIGHT_SEEDER_ARGS)
    await seeder.run()

    # Wait for snapshot to finish loading and try INFO PERSISTENCE
    await wait_available_async(async_client)
    res = await async_client.execute_command("INFO PERSISTENCE")
    assert "0" == extract_is_loading_field(res)


# If DRAGONFLY_S3_BUCKET is configured, AWS credentials must also be
# configured.
@pytest.mark.skipif(
    "DRAGONFLY_S3_BUCKET" not in os.environ, reason="AWS S3 snapshots bucket is not configured"
)
@dfly_args({**BASIC_ARGS, "dir": "s3://{DRAGONFLY_S3_BUCKET}{DRAGONFLY_TMP}", "dbfilename": ""})
async def test_s3_snapshot(self, async_client):
    seeder = FixedSeeder(key_target=10_000)
    await seeder.run(async_client)

    start_capture = await seeder.capture()

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

        assert await FixedSeeder.capture(async_client) == start_capture

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


@pytest.mark.skip("TODO")
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
            keys = await client.keys("*")
            if len(keys) == 0:
                break
            await client.delete(*keys)

    @pytest.mark.asyncio
    async def test_memory_counters(self, async_client):
        memory_counters = await self._get_info_memory_fields(async_client)
        assert memory_counters == {"object_used_memory": 0}

        seeder = FixedSeeder(**self.SEEDER_ARGS)
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
        seeder = FixedSeeder(**self.SEEDER_ARGS)
        await seeder.run(async_client)
        start_capture = await FixedSeeder.capture(async_client)

        memory_before = await self._get_info_memory_fields(async_client)

        await async_client.connection_pool.disconnect()
        df_server.stop()
        df_server.start()
        async_client = df_server.client()

        await wait_available_async(async_client)
        assert await FixedSeeder.capture(async_client) == start_capture

        memory_after = await self._get_info_memory_fields(async_client)
        for counter, value in memory_before.items():
            # Unfortunately memory usage sometimes depends on order of insertion / deletion, so
            # it's usually not exactly the same. For the test to be stable we check that it's
            # at least 50% that of the original value.
            assert memory_after[counter] >= 0.5 * value

        await self._delete_all_keys(async_client)
        memory_empty = await self._get_info_memory_fields(async_client)
        assert memory_empty == {"object_used_memory": 0}
