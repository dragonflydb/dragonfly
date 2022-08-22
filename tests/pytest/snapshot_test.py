from pathlib import Path
from tempfile import TemporaryDirectory
from conftest import dfly_args

import pytest
import redis
import time
import os

OUT_DIR = TemporaryDirectory()


@dfly_args("--alsologtostderr", "--dbfilename", "test.rdb",
           "--save_schedule", "*:*",
           "--dir", "{DRAGONFLY_TMP}/")
class TestSnapshot:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.rdb_out = tmp_dir / "test.rdb"
        if self.rdb_out.exists():
            self.rdb_out.unlink()

    def test_snapshot(self, client: redis.Redis):
        client.set("test", "test")

        time.sleep(60)

        assert self.rdb_out.exists()
