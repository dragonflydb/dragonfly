import time
import pytest
import os
import glob
from pathlib import Path

from . import dfly_args
from .utility import DflySeeder, wait_available_async

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}

SEEDER_ARGS = dict(keys=12_000, dbcount=5)


class SnapshotTestBase:
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    def get_main_file(self, suffix):
        def is_main(f): return "summary" in f if suffix == "dfs" else True
        files = glob.glob(str(self.tmp_dir.absolute()) + '/test-*.'+suffix)
        return next(f for f in sorted(files) if is_main(f))


@dfly_args({**BASIC_ARGS, "dbfilename": "test-rdb"})
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
        await async_client.execute_command("SAVE")
        assert await async_client.flushall()
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("rdb"))

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
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("dfs"))

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-periodic.rdb", "save_schedule": "*:*"})
class TestPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, keys=10)
        await seeder.run(target_deviation=0.5)

        time.sleep(60)

        assert (self.tmp_dir / "test-periodic.rdb").exists()
