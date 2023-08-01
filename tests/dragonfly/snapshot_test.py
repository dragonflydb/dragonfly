import time
import pytest
import os
import glob
import asyncio
from redis import asyncio as aioredis
from pathlib import Path

from . import dfly_args
from .utility import DflySeeder, wait_available_async

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}

SEEDER_ARGS = dict(keys=12_000, dbcount=5, multi_transaction_probability=0)


class SnapshotTestBase:
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    def get_main_file(self, pattern):
        def is_main(f):
            return "summary" in f if pattern.endswith("dfs") else True

        files = glob.glob(str(self.tmp_dir.absolute()) + "/" + pattern)
        possible_mains = list(filter(is_main, files))
        assert len(possible_mains) == 1, possible_mains
        return possible_mains[0]

    async def wait_for_save(self, pattern):
        while True:
            files = glob.glob(str(self.tmp_dir.absolute()) + "/" + pattern)
            if not len(files) == 0:
                break
            await asyncio.sleep(1)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-rdb-{{timestamp}}"})
class TestRdbSnapshot(SnapshotTestBase):
    """Test single file rdb snapshot"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE RDB")
        assert await async_client.flushall()
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("test-rdb-*.rdb"))

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-rdbexact.rdb", "nodf_snapshot_format": None})
class TestRdbSnapshotExactFilename(SnapshotTestBase):
    """Test single file rdb snapshot without a timestamp"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE RDB")
        assert await async_client.flushall()
        main_file = super().get_main_file("test-rdbexact.rdb")
        await async_client.execute_command("DEBUG LOAD " + main_file)

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-dfs"})
class TestDflySnapshot(SnapshotTestBase):
    """Test multi file snapshot"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE DF")
        assert await async_client.flushall()
        await async_client.execute_command(
            "DEBUG LOAD " + super().get_main_file("test-dfs-summary.dfs")
        )

        assert await seeder.compare(start_capture)


# We spawn instances manually, so reduce memory usage of default to minimum


@dfly_args({"proactor_threads": "1"})
class TestDflyAutoLoadSnapshot(SnapshotTestBase):
    """Test automatic loading of dump files on startup with timestamp"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    cases = [
        ("rdb", "test-autoload1-{{timestamp}}"),
        ("df", "test-autoload2-{{timestamp}}"),
        ("rdb", "test-autoload3-{{timestamp}}.rdb"),
        ("rdb", "test-autoload4"),
        ("df", "test-autoload5"),
        ("rdb", "test-autoload6.rdb"),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("save_type, dbfilename", cases)
    async def test_snapshot(self, df_local_factory, save_type, dbfilename):
        df_args = {"dbfilename": dbfilename, **BASIC_ARGS, "port": 1111}
        if save_type == "rdb":
            df_args["nodf_snapshot_format"] = ""
        df_server = df_local_factory.create(**df_args)
        df_server.start()

        client = aioredis.Redis(port=df_server.port)
        await client.set("TEST", hash(dbfilename))
        await client.execute_command("SAVE " + save_type)
        df_server.stop()

        df_server2 = df_local_factory.create(**df_args)
        df_server2.start()
        client = aioredis.Redis(port=df_server.port)
        response = await client.get("TEST")
        assert response.decode("utf-8") == str(hash(dbfilename))


@dfly_args({**BASIC_ARGS, "dbfilename": "test-periodic", "save_schedule": "*:*"})
class TestPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(
            port=df_server.port, keys=10, multi_transaction_probability=0
        )
        await seeder.run(target_deviation=0.5)

        await super().wait_for_save("test-periodic-summary.dfs")

        assert super().get_main_file("test-periodic-summary.dfs")


# save every 2 seconds
@dfly_args({**BASIC_ARGS, "dbfilename": "test-cron", "snapshot_cron": "*/2 * * * * *"})
class TestCronPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(
            port=df_server.port, keys=10, multi_transaction_probability=0
        )
        await seeder.run(target_deviation=0.5)

        await super().wait_for_save("test-cron-summary.dfs")

        assert super().get_main_file("test-cron-summary.dfs")


@dfly_args({**BASIC_ARGS})
class TestPathEscapes(SnapshotTestBase):
    """Test that we don't allow path escapes. We just check that df_server.start()
    fails because we don't have a much better way to test that."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_local_factory):
        df_server = df_local_factory.create(dbfilename="../../../../etc/passwd")
        try:
            df_server.start()
            assert False, "Server should not start correctly"
        except Exception as e:
            pass


@dfly_args({**BASIC_ARGS, "dbfilename": "test-shutdown"})
class TestDflySnapshotOnShutdown(SnapshotTestBase):
    """Test multi file snapshot"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        df_server.stop()
        df_server.start()

        a_client = aioredis.Redis(port=df_server.port)
        await wait_available_async(a_client)
        await a_client.connection_pool.disconnect()

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-info-persistence"})
class TestDflyInfoPersistenceLoadingField(SnapshotTestBase):
    """Test is_loading field on INFO PERSISTENCE during snapshot loading"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    def extract_is_loading_field(self, res):
        matcher = b"loading:"
        start = res.find(matcher)
        pos = start + len(matcher)
        return chr(res[pos])

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.05)
        a_client = aioredis.Redis(port=df_server.port)

        # Wait for snapshot to finish loading and try INFO PERSISTENCE
        await wait_available_async(a_client)
        res = await a_client.execute_command("INFO PERSISTENCE")
        assert "0" == self.extract_is_loading_field(res)

        await a_client.connection_pool.disconnect()
