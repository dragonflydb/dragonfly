"""
Pytest fixtures to be provided for all tests without import
"""

import os
import pytest
import pytest_asyncio
import redis
import aioredis

from pathlib import Path
from tempfile import TemporaryDirectory

from . import DflyInstance, DflyInstanceFactory

DATABASE_INDEX = 1


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


@pytest.fixture(scope="session", params=[{}])
def df_factory(request, tmp_dir, test_env) -> DflyInstanceFactory:
    """
    Create an instance factory with supplied params.
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.environ.get("DRAGONFLY_PATH", os.path.join(
        scripts_dir, '../../build-dbg/dragonfly'))

    args = request.param if request.param else {}
    return DflyInstanceFactory(test_env, tmp_dir, path=path, args=args)


@pytest.fixture(scope="session")
def df_server(df_factory: DflyInstanceFactory) -> DflyInstance:
    """
    Start the default Dragonfly server that will be used for the default pools
    and clients.
    """
    instance = df_factory.create()
    instance.start()
    yield instance
    instance.stop()


@pytest.fixture(scope="class")
def connection(df_server: DflyInstance):
    return redis.Connection(port=df_server.port)


@pytest.fixture(scope="class")
def sync_pool(df_server: DflyInstance):
    pool = redis.ConnectionPool(decode_responses=True, port=df_server.port)
    return pool


@pytest.fixture(scope="class")
def client(sync_pool):
    """
    Return a client to the default instance with all entries flushed.
    """
    client = redis.Redis(connection_pool=sync_pool)
    client.flushall()
    return client


@pytest.fixture(scope="class")
def async_pool(df_server: DflyInstance):
    pool = aioredis.ConnectionPool(host="localhost", port=df_server.port,
                                   db=DATABASE_INDEX, decode_responses=True, max_connections=16)
    return pool


@pytest_asyncio.fixture(scope="function")
async def async_client(async_pool):
    """
    Return an async client to the default instance with all entries flushed.
    """
    client = aioredis.Redis(connection_pool=async_pool)
    await client.flushall()
    return client
