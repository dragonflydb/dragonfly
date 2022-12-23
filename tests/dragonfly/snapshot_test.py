import time
import pytest
import redis
import string
import os
import glob
from pathlib import Path

from . import dfly_args
from .generator import DflySeeder

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}
NUM_KEYS = 100


class SnapshotTestBase:
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir
        self.rdb_out = tmp_dir / "test.rdb"
        if self.rdb_out.exists():
            self.rdb_out.unlink()

    def get_main_file(self, suffix):
        def is_main(f): return "summary" in f if suffix == "dfs" else True
        files = glob.glob(str(self.tmp_dir.absolute()) + '/test-*.'+suffix)
        return next(f for f in sorted(files) if is_main(f))


@dfly_args({**BASIC_ARGS, "dbfilename": "test"})
class TestRdbSnapshot(SnapshotTestBase):
    """Test single file rdb snapshot"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, async_client, df_server):
        seeder = DflySeeder(port=df_server.port, keys=2_000, dbcount=5)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE")
        assert await async_client.flushall()
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("rdb"))

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test"})
class TestDflySnapshot(SnapshotTestBase):
    """Test multi file snapshot"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir
        files = glob.glob(str(tmp_dir.absolute()) + 'test-*.dfs')
        for file in files:
            os.remove(file)

    @pytest.mark.asyncio
    async def test_snapshot(self, async_client, df_server):
        seeder = DflySeeder(port=df_server.port, keys=2_000, dbcount=5)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE DF")
        assert await async_client.flushall()
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("dfs"))

        assert await seeder.compare(start_capture)


@dfly_args({**BASIC_ARGS, "dbfilename": "test.rdb", "save_schedule": "*:*"})
class TestPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_server):
        seeder = DflySeeder(port=df_server.port, keys=5_000, dbcount=5)
        await seeder.run(target_deviation=0.1)

        time.sleep(60)

        assert self.rdb_out.exists()
