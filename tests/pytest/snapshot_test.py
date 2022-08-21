from test_dragonfly import DRAGONFLY_PATH, dfly_args, df_server, client, connection

import redis
from pathlib import Path
import time
import pytest

OUT_DIR = Path(DRAGONFLY_PATH).parent

@pytest.mark.usefixtures("client")
@dfly_args("--dbfilename", "test.rdb", 
	"--save_schedule", "*:*", 
	"--dir", str(OUT_DIR)+"/")
class TestSnapshot:
	def test_snapshot(self, client: redis.Redis):
		out_path = OUT_DIR / "test.rdb"
		client.set("test", "test")

		if out_path.exists():
			out_path.unlink()

		time.sleep(60)

		assert out_path.exists()
	