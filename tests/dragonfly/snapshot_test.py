import time
import pytest
import os
import glob
from pathlib import Path

from . import dfly_args
from .utility import DflySeeder, wait_available_async

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}

SEEDER_ARGS = dict(keys=12_000, dbcount=5, multi_transaction_probability=0)


class SnapshotTestBase:
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    def get_main_file(self, pattern):
        def is_main(f): return "summary" in f if pattern.endswith("dfs") else True
        files = glob.glob(str(self.tmp_dir.absolute()) + '/' + pattern)
        possible_mains = list(filter(is_main, files))
        assert len(possible_mains) == 1, possible_mains
        return possible_mains[0]


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


@dfly_args({**BASIC_ARGS, "dbfilename": "test-rdbexact.rdb"})
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
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("test-dfs-summary.dfs"))

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-autoload-{{timestamp}}"})
class TestDflyAutoLoadSnapshot(SnapshotTestBase):
    """Test automatic loading of dump files on startup with timestamp"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, async_client, df_local_factory):
        df_server = df_local_factory.create()
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        await async_client.execute_command("SAVE")
        df_server.stop()

        df_server2 = df_local_factory.create()
        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-periodic.dfs", "save_schedule": "*:*"})
class TestPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, keys=10, multi_transaction_probability=0)
        await seeder.run(target_deviation=0.5)

        time.sleep(60)

        assert super().get_main_file("test-periodic-summary.dfs")
