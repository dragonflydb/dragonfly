from pathlib import Path
from tempfile import TemporaryDirectory

import os
import pytest
import redis
import subprocess
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DRAGONFLY_PATH = os.environ.get("DRAGONFLY_HOME", os.path.join(
    SCRIPT_DIR, '../../build-dbg/dragonfly'))


@pytest.fixture(scope="session")
def tmp_dir():
    tmp = TemporaryDirectory()
    yield Path(tmp.name)
    tmp.cleanup()


@pytest.fixture(scope="session")
def test_env(tmp_dir: Path):
    env = os.environ.copy()
    env["DRAGONFLY_TMP"] = str(tmp_dir)
    return env

# used to define a singular set of arguments for dragonfly test


def dfly_args(*args):
    return pytest.mark.parametrize("df_server", [args], indirect=True)


# used to define multiple sets of arguments to test multiple dragonfly configurations
def dfly_multi_test_args(*args):
    return pytest.mark.parametrize("df_server", args, indirect=True)


@pytest.fixture(scope="class", params=[[]])
def df_server(request, tmp_dir: Path, test_env):
    """ Starts a single DragonflyDB process, runs only once. """
    print("Starting DragonflyDB [{}]".format(DRAGONFLY_PATH))
    arguments = [arg.format(**test_env) for arg in request.param]
    p = subprocess.Popen([DRAGONFLY_PATH, *arguments],
                         env=test_env, cwd=str(tmp_dir))
    time.sleep(0.1)
    return_code = p.poll()
    if return_code is not None:
        pytest.exit("Failed to start DragonflyDB [{}]".format(DRAGONFLY_PATH))
        p.terminate()

    yield

    print("Terminating DragonflyDB process [{}]".format(p.pid))
    try:
        p.terminate()
        outs, errs = p.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        print("Unable to terminate DragonflyDB gracefully, it was killed")
        outs, errs = p.communicate()
        print(outs)
        print(errs)


@pytest.fixture(scope="class")
def connection(df_server):
    pool = redis.ConnectionPool(decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    return client


@pytest.fixture
def client(connection):
    """ Flushes all the records, runs before each test. """
    connection.flushall()
    return connection
