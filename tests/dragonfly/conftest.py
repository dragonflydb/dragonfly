"""
Pytest fixtures to be provided for all tests without import
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import os
import subprocess
import time

import pytest
import redis

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DRAGONFLY_PATH = os.environ.get("DRAGONFLY_HOME", os.path.join(
    SCRIPT_DIR, '../../build-dbg/dragonfly'))


@pytest.fixture(scope="session")
def tmp_dir():
    """
    Pytest fixture to provide the test temporary directory for the session
    where the Dragonfly executable will be run and where all test data
    should be stored. The directory will be cleaned up at the end of a session
    """
    tmp = TemporaryDirectory()
    yield Path(tmp.name)
    tmp.cleanup()


@pytest.fixture(scope="session")
def test_env(tmp_dir: Path):
    """
    Provide the environment the Dragonfly executable is running in as a
    python dictionary
    """
    env = os.environ.copy()
    env["DRAGONFLY_TMP"] = str(tmp_dir)
    return env


@pytest.fixture(scope="class", params=[[]])
def df_server(request, tmp_dir: Path, test_env):
    """ Starts a single DragonflyDB process, runs once per test class. """
    print(f"Starting DragonflyDB [{DRAGONFLY_PATH}]")
    arguments = [arg.format(**test_env) for arg in request.param]
    dfly_proc = subprocess.Popen([DRAGONFLY_PATH, *arguments],
                                 env=test_env, cwd=str(tmp_dir))
    time.sleep(0.3)
    return_code = dfly_proc.poll()
    if return_code is not None:
        dfly_proc.terminate()
        pytest.exit(f"Failed to start DragonflyDB [{DRAGONFLY_PATH}]")

    yield

    print(f"Terminating DragonflyDB process [{dfly_proc.pid}]")
    try:
        dfly_proc.terminate()
        outs, errs = dfly_proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        print("Unable to terminate DragonflyDB gracefully, it was killed")
        outs, errs = dfly_proc.communicate()
        print(outs)
        print(errs)

@pytest.fixture(scope="function")
def connection(df_server):
    return redis.Connection()

@pytest.fixture(scope="class")
def raw_client(df_server):
    """ Creates the Redis client to interact with the Dragonfly instance """
    pool = redis.ConnectionPool(decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    return client

@pytest.fixture
def client(raw_client):
    """ Flushes all the records, runs before each test. """
    raw_client.flushall()
    return raw_client
