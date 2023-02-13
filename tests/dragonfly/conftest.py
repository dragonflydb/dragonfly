"""
Pytest fixtures to be provided for all tests without import
"""

import os
import sys
import pytest
import pytest_asyncio
import redis
import aioredis

from pathlib import Path
from tempfile import TemporaryDirectory

from . import DflyInstance, DflyInstanceFactory, DflyParams
from .utility import DflySeederFactory

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
def df_seeder_factory(request) -> DflySeederFactory:
    return DflySeederFactory(request.config.getoption("--log-seeder"))


@pytest.fixture(scope="session", params=[{}])
def df_factory(request, tmp_dir, test_env) -> DflyInstanceFactory:
    """
    Create an instance factory with supplied params.
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.environ.get("DRAGONFLY_PATH", os.path.join(
        scripts_dir, '../../build-dbg/dragonfly'))

    args = request.param if request.param else {}

    params = DflyParams(
        path=path,
        cwd=tmp_dir,
        gdb=request.config.getoption("--gdb"),
        args=request.config.getoption("--df"),
        env=test_env
    )

    factory = DflyInstanceFactory(params, args)
    yield factory
    factory.stop_all()


@pytest.fixture(scope="function")
def df_local_factory(df_factory: DflyInstanceFactory):
    factory = DflyInstanceFactory(df_factory.params, df_factory.args)
    yield factory
    factory.stop_all()


@pytest.fixture(scope="session")
def df_server(df_factory: DflyInstanceFactory) -> DflyInstance:
    """
    Start the default Dragonfly server that will be used for the default pools
    and clients.
    """
    instance = df_factory.create()
    instance.start()

    yield instance

    clients_left = None
    try:
        client = redis.Redis(port=instance.port)
        clients_left = client.execute_command("INFO")['connected_clients']
    except Exception as e:
        print(e, file=sys.stderr)

    instance.stop()
    #assert clients_left == 1


@pytest.fixture(scope="class")
def connection(df_server: DflyInstance):
    return redis.Connection(port=df_server.port)


@pytest.fixture(scope="class")
def sync_pool(df_server: DflyInstance):
    pool = redis.ConnectionPool(decode_responses=True, port=df_server.port)
    yield pool
    pool.disconnect()


@pytest.fixture(scope="class")
def client(sync_pool):
    """
    Return a client to the default instance with all entries flushed.
    """
    client = redis.Redis(connection_pool=sync_pool)
    client.flushall()
    return client


@pytest_asyncio.fixture(scope="function")
async def async_pool(df_server: DflyInstance):
    pool = aioredis.ConnectionPool(host="127.0.0.1", port=df_server.port,
                                   db=DATABASE_INDEX, decode_responses=True, max_connections=16)
    yield pool
    await pool.disconnect()


@pytest_asyncio.fixture(scope="function")
async def async_client(async_pool):
    """
    Return an async client to the default instance with all entries flushed.
    """
    client = aioredis.Redis(connection_pool=async_pool)
    await client.flushall()
    return client


def pytest_addoption(parser):
    """
    Custom pytest options:
        --gdb - start all instances inside gdb
        --df arg - pass arg to all instances, can be used multiple times
        --log-seeder file - to log commands of last seeder run
    """
    parser.addoption(
        '--gdb', action='store_true', default=False, help='Run instances in gdb'
    )
    parser.addoption(
        '--df', action='append', default=[], help='Add arguments to dragonfly'
    )
    parser.addoption(
        '--log-seeder', action='store', default=None, help='Store last generator commands in file'
    )
