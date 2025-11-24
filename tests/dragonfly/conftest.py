"""
Pytest fixtures to be provided for all tests without import
"""

import asyncio
import logging
import os
import random
import shutil
import subprocess
import sys
import time
import typing
from copy import deepcopy
from pathlib import Path
from tempfile import gettempdir, mkdtemp
from time import sleep
from typing import Dict, List, Union

import pymemcache
import pytest
import pytest_asyncio
import redis
from redis import asyncio as aioredis

from . import PortPicker
from .instance import DflyInstance, DflyParams, DflyInstanceFactory, RedisServer
from .utility import DflySeederFactory, gen_ca_cert, gen_certificate, skip_if_not_in_github

logging.getLogger("asyncio").setLevel(logging.WARNING)

DATABASE_INDEX = 0
BASE_LOG_DIR = "/tmp/dragonfly_logs/"
FAILED_PATH = "/tmp/failed/"
LAST_LOGS = "/tmp/last_test_log_dir.txt"


# runs on pytest start
def pytest_configure(config):
    # clean everything
    if os.path.exists(FAILED_PATH):
        shutil.rmtree(FAILED_PATH)
    if os.path.exists(BASE_LOG_DIR):
        shutil.rmtree(BASE_LOG_DIR)


@pytest.fixture(scope="class")
def df_log_dir(request):
    """
    Fixture to provide a log directory for the test class.
    This directory will be created before each test class and cleaned up after.
    """
    # Generate a unique directory name for the test class based on its nodeid
    translator = str.maketrans(":[]{}/ ", "_______", "\"*'")
    unique_dir = request.node.name.translate(translator)
    log_dir = os.path.join(BASE_LOG_DIR, unique_dir)

    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.makedirs(log_dir)

    # needs for action.yml to get logs if timedout is happen for test
    last_logs = open(LAST_LOGS, "w")
    last_logs.write(log_dir)
    last_logs.close()

    return log_dir


def determine_scope(fixture_name, config):
    drop_data_after_each_test = config.getoption("--drop-data-after-each-test", False)
    if drop_data_after_each_test:
        return "class"
    return "session"


@pytest.fixture(scope=determine_scope)
def tmp_dir():
    """
    Pytest fixture to provide the test temporary directory for the session
    where the Dragonfly executable will be run and where all test data
    should be stored. The directory will be cleaned up at the end of a session
    """
    tmp_name = mkdtemp()
    yield Path(tmp_name)
    if os.environ.get("DRAGONFLY_KEEP_TMP"):
        logging.info(f"Keeping tmp dir {tmp_name}")
        return
    shutil.rmtree(tmp_name, ignore_errors=True)


@pytest.fixture(scope=determine_scope)
def test_env(tmp_dir: Path):
    """
    Provide the environment the Dragonfly executable is running in as a
    python dictionary
    """
    env = os.environ.copy()
    env["DRAGONFLY_TMP"] = str(tmp_dir)
    return env


@pytest.fixture(scope="class", params=[{}])
def df_seeder_factory(request) -> DflySeederFactory:
    seed = request.config.getoption("--rand-seed")
    if seed is None:
        seed = random.randrange(sys.maxsize)

    random.seed(int(seed))
    logging.debug(f"Random seed: {seed}, check: {random.randrange(100)}")

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


@pytest_asyncio.fixture(scope="class")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="class", params=[{}])
async def df_factory(
    request,
    tmp_dir,
    test_env,
    df_log_dir,
) -> typing.AsyncGenerator[DflyInstanceFactory, None]:
    """
    Create an instance factory with supplied params.
    """
    os.makedirs(os.path.join(gettempdir(), "tiered"), exist_ok=True)
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
        direct_output=request.config.getoption("--direct-out"),
        buffered_out=request.config.getoption("--buffered-output"),
        args=parse_args(request.config.getoption("--df")),
        existing_port=int(existing) if existing else None,
        existing_admin_port=int(existing_admin) if existing_admin else None,
        existing_mc_port=int(existing_mc) if existing_mc else None,
        env=test_env,
        log_dir=df_log_dir,
    )

    factory = DflyInstanceFactory(params, args)
    yield factory
    await factory.stop_all()


@pytest.fixture(scope="class")
def df_server(df_factory: DflyInstanceFactory) -> typing.Generator[DflyInstance, None, None]:
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
    parser.addoption(
        "--direct-out",
        action="store_true",
        default=False,
        help="If true, does not post process dragonfly output",
    )

    parser.addoption("--repeat", action="store", help="Number of times to repeat each test")
    parser.addoption(
        "--drop-data-after-each-test",
        action="store_true",
        default=False,
        help="Remove test data after each test, instead of after each session, "
        "useful when running tests on repeat to avoid filling up disk",
    )


def pytest_generate_tests(metafunc):
    if metafunc.config.option.repeat is not None:
        count = int(metafunc.config.option.repeat)

        # We're going to duplicate these tests by parametrizing them,
        # which requires that each test has a fixture to accept the parameter.
        # We can add a new fixture like so:
        metafunc.fixturenames.append("tmp_ct")

        # Now we parametrize. This is what happens when we do e.g.,
        # @pytest.mark.parametrize('tmp_ct', range(count))
        # def test_foo(): pass
        metafunc.parametrize("tmp_ct", range(count))


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


def copy_failed_logs(log_dir, report):
    assert log_dir
    test_failed_path = os.path.join(FAILED_PATH, os.path.basename(log_dir))
    if not os.path.exists(test_failed_path):
        os.makedirs(test_failed_path)

    logging.error(f"Test failed {report.nodeid} with logs: ")

    for f in os.listdir(log_dir):
        file = os.path.join(log_dir, f)
        if os.path.isfile(file):
            file = file.rstrip("\n")
            logging.error(f"🪵🪵🪵🪵🪵🪵 {file} 🪵🪵🪵🪵🪵🪵")
            shutil.copy(file, test_failed_path)

    # Clean up
    try:
        os.remove(LAST_LOGS)
    except OSError:
        pass


# tests results we get on the "call" state
# but we can not copy logs until "teardown" state because the server isn't stoped
# so we save result of the "call" state and process it on the "teardown" when the server is stoped
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    if report.when == "call":
        # Store the result of the call phase in the item
        item.call_outcome = report

    if report.when == "teardown":
        call_outcome = getattr(item, "call_outcome", None)
        log_dir = item.funcargs.get("df_log_dir")
        if log_dir:
            if report.failed:
                copy_failed_logs(log_dir, report)
            if call_outcome and call_outcome.failed:
                copy_failed_logs(log_dir, call_outcome)


@pytest.fixture(scope="function")
def redis_server(port_picker) -> RedisServer:
    s = RedisServer(port_picker.get_available_port())
    try:
        s.start()
    except FileNotFoundError as e:
        skip_if_not_in_github()
        raise
    time.sleep(1)
    yield s
    s.stop()


@pytest.fixture(scope="function")
def redis_local_server(port_picker) -> RedisServer:
    s = RedisServer(port_picker.get_available_port())
    time.sleep(1)
    yield s
    s.stop()
