import os.path
from tempfile import NamedTemporaryFile
from typing import Callable

import yaml
from prometheus_client.samples import Sample
from pymemcache import Client

from . import dfly_args
from .instance import DflyInstance
from .utility import *


@pytest.fixture(scope="class")
def connection(df_server: DflyInstance):
    return redis.Connection(port=df_server.port)


class TestServer:
    def test_quit(self, connection: redis.Connection):
        connection.send_command("QUIT")
        assert connection.read_response() == b"OK"

        with pytest.raises(redis.exceptions.ConnectionError) as e:
            connection.read_response()

    def test_quit_after_sub(self, connection):
        connection.send_command("SUBSCRIBE", "foo")
        connection.read_response()

        connection.send_command("QUIT")
        assert connection.read_response() == b"OK"

        with pytest.raises(redis.exceptions.ConnectionError) as e:
            connection.read_response()

    async def test_multi_exec(self, async_client: aioredis.Redis):
        pipeline = async_client.pipeline()
        pipeline.set("foo", "bar")
        pipeline.get("foo")
        val = await pipeline.execute()
        assert val == [True, "bar"]


"""
see https://github.com/dragonflydb/dragonfly/issues/457
For now we would not allow for eval command inside multi
As this would create to level transactions (in effect recursive call
to Schedule function).
When this issue is fully fixed, this test would failed, and then it should
change to match the fact that we supporting this operation.
For now we are expecting to get an error
"""


async def test_multi_eval(async_client: aioredis.Redis):
    pipeline = async_client.pipeline()
    pipeline.set("foo", "bar")
    pipeline.get("foo")
    pipeline.eval("return 43", 0)
    val = await pipeline.execute()
    assert val == [True, "bar", 43]


async def test_connection_name(async_client: aioredis.Redis):
    name = await async_client.execute_command("CLIENT GETNAME")
    assert name == "default-async-fixture"
    await async_client.execute_command("CLIENT SETNAME test_conn_name")
    name = await async_client.execute_command("CLIENT GETNAME")
    assert name == "test_conn_name"


async def test_get_databases(async_client: aioredis.Redis):
    """
    make sure that the config get databases command is working
    to ensure compatibility with UI frameworks like AnotherRedisDesktopManager
    """
    dbnum = await async_client.config_get("databases")
    assert dbnum == {"databases": "16"}


@pytest.mark.exclude_epoll  # Failing test. It should be turned on as soon as it is fixed.
async def test_client_kill(df_factory):
    with df_factory.create(port=1111, admin_port=1112) as instance:
        client = aioredis.Redis(port=instance.port)
        admin_client = aioredis.Redis(port=instance.admin_port)
        await admin_client.ping()

        # This creates `client_conn` as a non-auto-reconnect client
        async with client.client() as client_conn:
            assert len(await client_conn.execute_command("CLIENT LIST")) == 2
            assert len(await admin_client.execute_command("CLIENT LIST")) == 2

            # Can't kill admin from regular connection
            with pytest.raises(Exception) as e_info:
                await client_conn.execute_command("CLIENT KILL LADDR 127.0.0.1:1112")

            assert len(await admin_client.execute_command("CLIENT LIST")) == 2
            await admin_client.execute_command("CLIENT KILL LADDR 127.0.0.1:1111")
            assert len(await admin_client.execute_command("CLIENT LIST")) == 1
            with pytest.raises(Exception) as e_info:
                await client_conn.ping()


async def test_scan(async_client: aioredis.Redis):
    """
    make sure that the scan command is working with python
    """

    def gen_test_data():
        for i in range(10):
            yield f"key-{i}", f"value-{i}"

    for key, val in gen_test_data():
        res = await async_client.set(key, val)
        assert res is not None
        cur, keys = await async_client.scan(cursor=0, match=key, count=2)
        assert cur == 0
        assert len(keys) == 1
        assert keys[0] == key


def configure_slowlog_parsing(async_client: aioredis.Redis):
    def parse_slowlog_get(response, **options):
        logging.info(f"slowlog response: {response}")

        def stringify(item):
            if isinstance(item, bytes):
                return item.decode()
            if isinstance(item, list):
                return [stringify(i) for i in item]
            return item

        def parse_item(item):
            item = stringify(item)
            result = {"id": item[0], "start_time": int(item[1]), "duration": int(item[2])}
            result["command"] = " ".join(item[3])
            result["client_address"] = item[4]
            result["client_name"] = item[5]
            return result

        return [parse_item(item) for item in response]

    async_client.set_response_callback("SLOWLOG GET", parse_slowlog_get)
    return async_client


@pytest.mark.asyncio
@dfly_args({"slowlog_log_slower_than": 0, "slowlog_max_len": 3})
async def test_slowlog_client_name_and_ip(df_factory, async_client: aioredis.Redis):
    df = df_factory.create()
    df.start()
    expected_clientname = "dragonfly"

    await async_client.client_setname(expected_clientname)
    async_client = configure_slowlog_parsing(async_client)

    client_list = await async_client.client_list()
    addr = client_list[0]["addr"]

    slowlog = await async_client.slowlog_get(1)
    assert slowlog[0]["client_name"] == expected_clientname
    assert slowlog[0]["client_address"] == addr


@pytest.mark.asyncio
@dfly_args({"slowlog_log_slower_than": 0, "slowlog_max_len": 3})
async def test_blocking_commands_should_not_show_up_in_slow_log(
    df_factory, async_client: aioredis.Redis
):
    await async_client.slowlog_reset()
    df = df_factory.create()
    df.start()
    async_client = configure_slowlog_parsing(async_client)

    await async_client.blpop("mykey", 0.5)
    reply = await async_client.slowlog_get()

    # blpop does not show up, only the previous reset
    assert reply[0]["command"] == "SLOWLOG RESET"


@dfly_args({"memcached_port": 11211, "admin_port": 1112})
async def test_metric_labels(
    df_server: DflyInstance, async_client: aioredis.Redis, memcached_client: Client
):
    result = await async_client.set("foo", "bar")
    assert result, "Failed to set key"

    result = await async_client.get("foo")
    assert result == "bar", "Failed to read value"

    def match_label_value(s: Sample, name, func):
        assert "listener" in s.labels
        if s.labels["listener"] == name:
            assert func(s.value)

    metrics = await df_server.metrics()
    for sample in metrics["dragonfly_commands_processed"].samples:
        match_label_value(sample, "main", lambda v: v > 0)
        match_label_value(sample, "other", lambda v: v == 0)
    for sample in metrics["dragonfly_connected_clients"].samples:
        match_label_value(sample, "main", lambda v: v == 1)
        match_label_value(sample, "other", lambda v: v == 0)

    # Memcached client also counts as main
    memcached_client.set("foo", "bar")

    metrics = await df_server.metrics()
    for sample in metrics["dragonfly_commands_processed"].samples:
        match_label_value(sample, "main", lambda v: v > 0)
        match_label_value(sample, "other", lambda v: v == 0)
    for sample in metrics["dragonfly_connected_clients"].samples:
        match_label_value(sample, "main", lambda v: v == 2)
        match_label_value(sample, "other", lambda v: v == 0)

    # admin client counts as other
    async with aioredis.Redis(port=1112) as admin:
        await admin.ping()

        metrics = await df_server.metrics()
        for sample in metrics["dragonfly_commands_processed"].samples:
            match_label_value(sample, "main", lambda v: v > 0)
            # memcached listener processes command as other
            match_label_value(sample, "other", lambda v: v > 0)
        for sample in metrics["dragonfly_connected_clients"].samples:
            match_label_value(sample, "main", lambda v: v == 2)
            match_label_value(sample, "other", lambda v: v == 1)


async def test_latency_stats(async_client: aioredis.Redis):
    for _ in range(100):
        await async_client.set("foo", "bar")
        await async_client.get("foo")
        await async_client.get("bar")
        await async_client.hgetall("missing")

    latency_stats = await async_client.info("LATENCYSTATS")
    for expected in {"hgetall", "set", "get"}:
        key = f"latency_percentiles_usec_{expected}"
        assert key in latency_stats
        assert latency_stats[key].keys() == {"p50", "p99", "p99.9"}

    await async_client.config_resetstat()
    latency_stats = await async_client.info("LATENCYSTATS")
    # Only stats for the `config resetstat` command should remain in stats
    assert (
        len(latency_stats) == 1 and "latency_percentiles_usec_config" in latency_stats
    ), f"unexpected latency stats after reset: {latency_stats}"


@dfly_args({"latency_tracking": False})
async def test_latency_stats_disabled(async_client: aioredis.Redis):
    for _ in range(100):
        await async_client.set("foo", "bar")
    assert await async_client.info("LATENCYSTATS") == {}


async def test_metrics_sanity_check(df_server: DflyInstance):
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    def on_container_output(container: DockerContainer, fn: Callable):
        for entry in container.get_logs():
            for row in entry.decode("utf-8").split("\n"):
                fn(row)

    def extract_msg(s: str):
        return re.search("""msg="([^"]*)""", s).group(1)

    def assert_no_error(entry: str):
        assert "level=ERROR" not in entry and "level=WARN" not in entry, extract_msg(entry)

    # Piggyback on the first known mounted path if running in CI, the container running the test will start another
    # container with prometheus. The prometheus container needs the file present on the host to be able to mount it.
    # Fall back to /tmp so the test can be run on the local machine without using root.
    parent = next((p for p in ("/var/crash", "/mnt", "/tmp") if os.access(p, os.W_OK)), None)

    # TODO use python-docker api to find a valid mounted volume instead of hardcoded list
    assert parent is not None, "Could not find a path to write prometheus config"
    with NamedTemporaryFile("w", dir=parent) as f:
        prometheus_config = {
            "scrape_configs": [
                {
                    "job_name": "dfly",
                    "scrape_interval": "1s",
                    "static_configs": [{"targets": [f"host.docker.internal:{df_server.port}"]}],
                }
            ]
        }
        prometheus_config_path = "/etc/prometheus/prometheus.yml"

        logging.info(f"Starting prometheus with file {f.name}:\n{yaml.dump(prometheus_config)}")

        yaml.dump(prometheus_config, f)
        path = os.path.abspath(f.name)
        os.chmod(path, 0o644)

        with (
            DockerContainer(image="prom/prometheus")
            .with_volume_mapping(path, prometheus_config_path)
            .with_kwargs(extra_hosts={"host.docker.internal": "host-gateway"})
        ) as prometheus:
            try:
                wait_for_logs(prometheus, "Server is ready to receive web requests.", timeout=5)

                # Wait for a few seconds for any potential warnings or errors to appear, it can take several seconds.
                wait_for_errors_sec, sleep_time_sec = 10, 0.5
                start = time.monotonic()
                while time.monotonic() < start + wait_for_errors_sec:
                    on_container_output(prometheus, assert_no_error)
                    await asyncio.sleep(sleep_time_sec)
            except AssertionError:
                # For assertion errors which we raise, skip printing full prometheus logs
                raise
            except Exception as e:
                # For any other error such as timeout when starting the container, print all container logs
                logging.error(f"failed to start prometheus: {e}")
                on_container_output(
                    prometheus, lambda entry: logging.info(f"prometheus log: {entry}")
                )


@pytest.mark.opt_only
@dfly_args({"proactor_threads": "2"})
async def test_huffman_tables_built(df_server: DflyInstance):
    async_client = df_server.client()
    # Insert enough data to trigger background huffman table building
    key_name = "keyfooobarrsoooooooooooooooooooooooooooooooooooooooooooooooo"
    await async_client.execute_command("DEBUG", "POPULATE", "1000000", key_name, "14")

    @assert_eventually(times=500)
    async def check_metrics():
        metrics = await df_server.metrics()
        m = metrics["dragonfly_huffman_tables_built"]
        assert m.samples[0].value > 0

    await check_metrics()
