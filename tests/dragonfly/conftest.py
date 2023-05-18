"""
Pytest fixtures to be provided for all tests without import
"""

import logging
import os
import sys
from time import sleep
from redis import asyncio as aioredis
import pytest
import pytest_asyncio
import redis
import pymemcache
import random

from pathlib import Path
from tempfile import TemporaryDirectory

from . import DflyInstance, DflyInstanceFactory, DflyParams, PortPicker, dfly_args
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
    seed = request.config.getoption("--rand-seed")
    if seed is None:
        seed = random.randrange(sys.maxsize)


    random.seed(int(seed))
    print(f"--- Random seed: {seed}, check: {random.randrange(100)} ---")

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
    existing = request.config.getoption("--existing-port")
    existing_mc = request.config.getoption("--existing-mc-port")
    params = DflyParams(
        path=path,
        cwd=tmp_dir,
        gdb=request.config.getoption("--gdb"),
        args=request.config.getoption("--df"),
        existing_port=int(existing) if existing else None,
        existing_mc_port=int(existing_mc) if existing else None,
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
        client.client_setname("mgr")
        sleep(0.1)
        clients_left = [x for x in client.client_list() if x["name"] != "mgr"]
    except Exception as e:
        print(e, file=sys.stderr)

    instance.stop()

    # TODO: Investigate spurious open connection with cluster client
    if not instance['cluster_mode']:
        assert clients_left == []
    else:
        print("Cluster clients left: ", len(clients_left))


@pytest.fixture(scope="class")
def connection(df_server: DflyInstance):
    return redis.Connection(port=df_server.port)


# @pytest.fixture(scope="class")
# def sync_pool(df_server: DflyInstance):
#     pool = redis.ConnectionPool(decode_responses=True, port=df_server.port)
#     yield pool
#     pool.disconnect()


# @pytest.fixture(scope="class")
# def client(sync_pool):
#     """
#     Return a client to the default instance with all entries flushed.
#     """
#     client = redis.Redis(connection_pool=sync_pool)
#     client.flushall()
#     return client


@pytest.fixture(scope="function")
def cluster_client(df_server):
    """
    Return a cluster client to the default instance with all entries flushed.
    """
    client = redis.RedisCluster(decode_responses=True, host="localhost",
                                port=df_server.port)
    client.client_setname("default-cluster-fixture")
    client.flushall()

    yield client
    client.disconnect_connection_pools()


@pytest_asyncio.fixture(scope="function")
async def async_pool(df_server: DflyInstance):
    pool = aioredis.ConnectionPool(host="localhost", port=df_server.port,
                                   db=DATABASE_INDEX, decode_responses=True, max_connections=32)
    yield pool
    await pool.disconnect(inuse_connections=True)

@pytest_asyncio.fixture(scope="function")
async def async_client(async_pool):
    """
    Return an async client to the default instance with all entries flushed.
    """
    client = aioredis.Redis(connection_pool=async_pool)
    await client.client_setname("default-async-fixture")
    await client.flushall()
    yield client


def pytest_addoption(parser):
    """
    Custom pytest options:
        --gdb - start all instances inside gdb
        --df arg - pass arg to all instances, can be used multiple times
        --log-seeder file - to log commands of last seeder run
        --existing-port - to provide a port to an existing process instead of starting a new instance
        --rand-seed - to set the global random seed
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
    parser.addoption(
        '--rand-seed', action='store', default=None, help='Set seed for global random. Makes seeder predictable'
    )
    parser.addoption(
        '--existing-port', action='store', default=None, help='Provide a port to the existing process for the test')

    parser.addoption(
        '--existing-mc-port', action='store', default=None, help='Provide a port to the existing memcached process for the test'
    )


@pytest.fixture(scope="session")
def port_picker():
    yield PortPicker()


@pytest.fixture(scope="class")
def memcached_connection(df_server: DflyInstance):
    return pymemcache.Client(f"localhost:{df_server.mc_port}")
