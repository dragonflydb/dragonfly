"""
Pytest fixtures to be provided for all tests without import
"""

import logging
import os
import sys
from time import sleep
from typing import Dict, List, Union
from redis import asyncio as aioredis
import pytest
import pytest_asyncio
import redis
import pymemcache
import random
import subprocess
import shutil
import time
from copy import deepcopy

from pathlib import Path
from tempfile import TemporaryDirectory

from .instance import DflyInstance, DflyParams, DflyInstanceFactory, RedisServer
from . import PortPicker, dfly_args
from .utility import DflySeederFactory, gen_ca_cert, gen_certificate

logging.getLogger("asyncio").setLevel(logging.WARNING)

DATABASE_INDEX = 0

TEST_FAILED = False


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


def parse_args(args: List[str]) -> Dict[str, Union[str, None]]:
    args_dict = {}
    for arg in args:
        if "=" in arg:
            pos = arg.find("=")
            name, value = arg[:pos], arg[pos + 1 :]
            args_dict[name] = value
        else:
            args_dict[arg] = None
    return args_dict


@pytest.fixture(scope="session", params=[{}])
def df_factory(request, tmp_dir, test_env) -> DflyInstanceFactory:
    """
    Create an instance factory with supplied params.
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.environ.get("DRAGONFLY_PATH", os.path.join(scripts_dir, "../../build-dbg/dragonfly"))

    args = request.param if request.param else {}
    existing = request.config.getoption("--existing-port")
    existing_admin = request.config.getoption("--existing-admin-port")
    existing_mc = request.config.getoption("--existing-mc-port")
    params = DflyParams(
        path=path,
        cwd=tmp_dir,
        gdb=request.config.getoption("--gdb"),
        buffered_out=request.config.getoption("--buffered-output"),
        args=parse_args(request.config.getoption("--df")),
        existing_port=int(existing) if existing else None,
        existing_admin_port=int(existing_admin) if existing_admin else None,
        existing_mc_port=int(existing_mc) if existing_mc else None,
        env=test_env,
    )

    factory = DflyInstanceFactory(params, args)
    yield factory
    factory.stop_all()


# Differs from df_factory in that its scope is function
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
    # if not instance['cluster_mode']:
    # TODO: Investigate adding fine grain control over the pool by
    # by adding a cache ontop of the clients connection pool and then evict
    # properly with client.connection_pool.disconnect() avoiding non synced
    # side effects
    # assert clients_left == []
    # else:
    #    print("Cluster clients left: ", len(clients_left))

    if instance["cluster_mode"]:
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
    client = redis.RedisCluster(decode_responses=True, host="localhost", port=df_server.port)
    client.client_setname("default-cluster-fixture")
    client.flushall()

    yield client
    client.disconnect_connection_pools()


@pytest_asyncio.fixture(scope="function")
async def async_pool(df_server: DflyInstance):
    pool = aioredis.ConnectionPool(
        host="localhost",
        port=df_server.port,
        db=DATABASE_INDEX,
        decode_responses=True,
        max_connections=32,
    )
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
    await client.select(DATABASE_INDEX)
    yield client


def pytest_addoption(parser):
    parser.addoption("--gdb", action="store_true", default=False, help="Run instances in gdb")
    parser.addoption("--df", action="append", default=[], help="Add arguments to dragonfly")
    parser.addoption(
        "--buffered-output",
        action="store_true",
        default=False,
        help="Makes instance output buffered, grouping it together",
    )
    parser.addoption(
        "--log-seeder", action="store", default=None, help="Store last generator commands in file"
    )
    parser.addoption(
        "--rand-seed",
        action="store",
        default=None,
        help="Set seed for global random. Makes seeder predictable",
    )
    parser.addoption(
        "--existing-port",
        action="store",
        default=None,
        help="Provide a port to the existing process for the test",
    )
    parser.addoption(
        "--existing-admin-port",
        action="store",
        default=None,
        help="Provide an admin port to the existing process for the test",
    )
    parser.addoption(
        "--existing-mc-port",
        action="store",
        default=None,
        help="Provide a port to the existing memcached process for the test",
    )


@pytest.fixture(scope="session")
def port_picker():
    yield PortPicker()


@pytest.fixture(scope="function")
def memcached_client(df_server: DflyInstance):
    client = pymemcache.Client(f"127.0.0.1:{df_server.mc_port}", default_noreply=False)

    yield client

    client.flush_all()


@pytest.fixture(scope="session")
def with_tls_ca_cert_args(tmp_dir):
    ca_key = os.path.join(tmp_dir, "ca-key.pem")
    ca_cert = os.path.join(tmp_dir, "ca-cert.pem")
    gen_ca_cert(ca_key, ca_cert)
    return {"ca_key": ca_key, "ca_cert": ca_cert}


@pytest.fixture(scope="session")
def with_tls_server_args(tmp_dir, with_tls_ca_cert_args):
    tls_server_key = os.path.join(tmp_dir, "df-key.pem")
    tls_server_req = os.path.join(tmp_dir, "df-req.pem")
    tls_server_cert = os.path.join(tmp_dir, "df-cert.pem")

    gen_certificate(
        with_tls_ca_cert_args["ca_key"],
        with_tls_ca_cert_args["ca_cert"],
        tls_server_req,
        tls_server_key,
        tls_server_cert,
    )

    args = {"tls": None, "tls_key_file": tls_server_key, "tls_cert_file": tls_server_cert}
    return args


@pytest.fixture(scope="session")
def with_ca_tls_server_args(with_tls_server_args, with_tls_ca_cert_args):
    args = deepcopy(with_tls_server_args)
    args["tls_ca_cert_file"] = with_tls_ca_cert_args["ca_cert"]
    return args


@pytest.fixture(scope="session")
def with_ca_dir_tls_server_args(with_tls_server_args, with_tls_ca_cert_args):
    args = deepcopy(with_tls_server_args)
    ca_cert = with_tls_ca_cert_args["ca_cert"]
    ca_dir = os.path.dirname(ca_cert)
    # We need this because any program that uses OpenSSL requires directories to be set up like this
    # in order to find the certificates. This command, creates the necessary symlinks to the files
    # such that they can be consumed by OpenSSL when loaded from the directory.
    # For more info see: https://www.openssl.org/docs/man3.0/man1/c_rehash.html
    command = f"c_rehash {ca_dir}"
    subprocess.run(command, shell=True)
    args["tls_ca_cert_dir"] = ca_dir
    return args, ca_cert


@pytest.fixture(scope="session")
def with_tls_client_args(tmp_dir, with_tls_ca_cert_args):
    tls_client_key = os.path.join(tmp_dir, "client-key.pem")
    tls_client_req = os.path.join(tmp_dir, "client-req.pem")
    tls_client_cert = os.path.join(tmp_dir, "client-cert.pem")

    gen_certificate(
        with_tls_ca_cert_args["ca_key"],
        with_tls_ca_cert_args["ca_cert"],
        tls_client_req,
        tls_client_key,
        tls_client_cert,
    )

    args = {"ssl": True, "ssl_keyfile": tls_client_key, "ssl_certfile": tls_client_cert}
    return args


@pytest.fixture(scope="session")
def with_ca_tls_client_args(with_tls_client_args, with_tls_ca_cert_args):
    args = deepcopy(with_tls_client_args)
    args["ssl_ca_certs"] = with_tls_ca_cert_args["ca_cert"]
    return args


def copy_failed_logs_and_clean_tmp_folder():
    failed_path = "/tmp/failed"
    path_exists = os.path.exists(failed_path)
    if not path_exists:
        os.makedirs(failed_path)

    last_log_file = open("/tmp/failed_list.txt", "r")
    files = last_log_file.readlines()
    for file in files:
        # copy to failed folder
        shutil.copy(file.rstrip("\n"), failed_path)

    # Clean up everything
    last_log_file = open("/tmp/failed_list.txt", "w").close()
    last_log_file = open("/tmp/last_test_log_files.txt", "w").close()


def pytest_exception_interact(node, call, report):
    if report.failed:
        # To print the test that currently failed
        last_log_file = open("/tmp/last_test_log_files.txt", "r")
        # Global tracking of all failed tests/logs
        failed_list = open("/tmp/failed_list.txt", "a")
        files = last_log_file.readlines()
        logging.error(f"Test failed {report.nodeid} with logs: ")
        for file in files:
            failed_list.write(file)
            file = file.rstrip("\n")
            logging.error(f"🪵🪵🪵🪵🪵🪵 {file} 🪵🪵🪵🪵🪵🪵")

        # Clean it
        last_log_file = open("/tmp/last_test_log_files.txt", "w").close()
        global TEST_FAILED
        TEST_FAILED = True


# Double consider any change to this function because the order
# of SetUp and TearDown might cause improper copy of the logs
# because the instance has not yet stopped. For example, df_factory
# has session scope so it won't be called unless the session has ended.
# On the other hand, df_local_factory has function scope, and therefore
# it is executed strictly before this (so the instance is stoped and we can
# safely copy the logs without loosing some of them).
# autouse means that this is run for all tests in the module.
# scope="session" allows to create the file only once and clean it
# at the end respecting the evaluation order mentioned above
@pytest.fixture(autouse=True, scope="session")
def run_before_and_after_test():
    logging.info("Session start for run_before_and_after_test")
    # Setup: at the start of the session
    last_log_file = open("/tmp/last_test_log_files.txt", "w").close()

    yield  # this is where the testing happens

    global TEST_FAILED
    # Teardown at the end of the session
    logging.info(f"Session end for run_before_and_after_test")
    if TEST_FAILED:
        logging.info(f"Copying failed tests to /tmp/failed")
        copy_failed_logs_and_clean_tmp_folder()


# The only issue here is that this also clears the logs for session wide fixtures which
# could be problematic for tests that failed way later in the session. Example:
# suppose a session fixture like dragonfly_factory run by 3 individual tests
# First one instantiates the dragonfly instance and writes its log path in the
# last_test_log_files.txt. Second test then clears this, and third one fails without any logs.
# This problem existed before but luckilly so far hasn't come up. There is no really good fix for
# this as it's hard to track which test used which fixture when it failed, since we only interact
# with the failed test via the pytest hook pytest_exception_interact.
@pytest.fixture(autouse=True, scope="function")
def clean_up_per_test():
    last_log_file = open("/tmp/last_test_log_files.txt", "w").close()
    yield


@pytest.fixture(scope="function")
def redis_server(port_picker) -> RedisServer:
    s = RedisServer(port_picker.get_available_port())
    try:
        s.start()
    except FileNotFoundError as e:
        pytest.skip("Redis server not found")
        return None
    time.sleep(1)
    yield s
    s.stop()


@pytest.fixture(scope="function")
def redis_local_server(port_picker) -> RedisServer:
    s = RedisServer(port_picker.get_available_port())
    time.sleep(1)
    yield s
    s.stop()
