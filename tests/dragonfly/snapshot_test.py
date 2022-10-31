import time
import pytest
import redis
import string
import os
import glob


from pathlib import Path
from dragonfly import dfly_args

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}


class SnapshotTestBase:
    KEYS = string.ascii_lowercase

    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir
        self.rdb_out = tmp_dir / "test.rdb"
        if self.rdb_out.exists():
            self.rdb_out.unlink()

    def populate(self, client: redis.Redis):
        """Populate instance with test data"""
        for k in self.KEYS:
            client.set(k, "val-"+k)

    def check(self, client: redis.Redis):
        """Check instance contains test data"""
        for k in self.KEYS:
            assert client.get(k) == "val-"+k

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

    def test_snapshot(self, client: redis.Redis):
        super().populate(client)

        # save + flush + load
        client.execute_command("SAVE")
        assert client.flushall()
        client.execute_command("DEBUG LOAD " + super().get_main_file("rdb"))

        super().check(client)


@dfly_args({**BASIC_ARGS, "dbfilename": "test"})
class TestDflySnapshot(SnapshotTestBase):
    """Test multi file snapshot"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir
        files = glob.glob(str(tmp_dir.absolute()) + 'test-*.dfs')
        for file in files:
            os.remove(file)

    def test_snapshot(self, client: redis.Redis):
        super().populate(client)

        # save + flush + load
        client.execute_command("SAVE DF")
        assert client.flushall()
        client.execute_command("DEBUG LOAD " + super().get_main_file("dfs"))

        super().check(client)


@dfly_args({**BASIC_ARGS, "dbfilename": "test.rdb", "save_schedule": "*:*"})
class TestPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    def test_snapshot(self, client: redis.Redis):
        super().populate(client)

        time.sleep(60)

        assert self.rdb_out.exists()
