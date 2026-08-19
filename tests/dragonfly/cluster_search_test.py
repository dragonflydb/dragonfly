import asyncio
import json
import logging
import random
import subprocess
import time
from binascii import crc_hqx
from dataclasses import dataclass

import pytest

from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstance, DflyInstanceFactory
from .utility import (
    skip_if_not_in_github,
    tick_timer,
)

BASE_PORT = 30001


def monotonically_increasing_port_number():
    port = BASE_PORT
    while True:
        yield port
        port = port + 1


# Create a generator object
next_port = monotonically_increasing_port_number()


async def get_memory(client, field):
    info = await client.info("memory")
    return info[field]


class RedisClusterNode:
    def __init__(self, port):
        self.port = port
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(
            [
                "redis-server-6.2.11",
                f"--port {self.port}",
                "--save ''",
                "--cluster-enabled yes",
                f"--cluster-config-file nodes_{self.port}.conf",
                "--cluster-node-timeout 5000",
                "--appendonly no",
                "--protected-mode no",
                "--repl-diskless-sync yes",
                "--repl-diskless-sync-delay 0",
            ]
        )
        logging.debug(self.proc.args)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except Exception:
            pass


@pytest.fixture(scope="function")
def redis_cluster(port_picker):
    # create redis client with 3 node with default slot configuration
    # node1 slots 0-5460
    # node2 slots 5461-10922
    # node3 slots 10923-16383
    ports = [port_picker.get_available_port() for i in range(3)]
    nodes = [RedisClusterNode(port) for port in ports]
    try:
        for node in nodes:
            node.start()
            time.sleep(1)
    except FileNotFoundError:
        skip_if_not_in_github()
        raise

    create_command = f'echo "yes" |redis-cli --cluster create {" ".join([f"127.0.0.1:{port}" for port in ports])}'
    subprocess.run(create_command, shell=True)
    time.sleep(4)
    yield nodes
    for node in nodes:
        node.stop()


@dataclass
class MigrationInfo:
    ip: str
    port: int
    slots: list
    node_id: str


@dataclass
class NodeInfo:
    id: str
    instance: DflyInstance
    client: aioredis.Redis
    admin_client: aioredis.Redis
    slots: list
    migrations: list
    replicas: list
    health: str


async def create_node_info(instance) -> NodeInfo:
    client = instance.client()
    node_id = await get_node_id(client)
    ninfo = NodeInfo(
        id=node_id,
        instance=instance,
        client=client,
        admin_client=instance.admin_client(),
        slots=[],
        migrations=[],
        replicas=[],
        health="online",
    )
    return ninfo


def generate_config(nodes):
    return [
        {
            "slot_ranges": [{"start": s, "end": e} for (s, e) in node.slots],
            "master": {
                "id": node.id,
                "ip": "127.0.0.1",
                "port": node.instance.port,
                "health": node.health,
            },
            "replicas": [
                {
                    "id": replica.id,
                    "ip": "127.0.0.1",
                    "port": replica.instance.port,
                    "health": node.health,
                }
                for replica in node.replicas
            ],
            "migrations": [
                {
                    "slot_ranges": [{"start": s, "end": e} for (s, e) in m.slots],
                    "node_id": m.node_id,
                    "ip": m.ip,
                    "port": m.port,
                }
                for m in node.migrations
            ],
        }
        for node in nodes
    ]


async def push_config(config, admin_connections):
    logging.debug("Pushing config %s", config)
    res = await asyncio.gather(
        *(c_admin.execute_command("DFLYCLUSTER", "CONFIG", config) for c_admin in admin_connections)
    )
    assert all([r == "OK" for r in res])


async def apply_config(nodes):
    """Push generated config to all nodes using their admin clients."""
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])


async def apply_config_via_client(nodes):
    """Push generated config to all nodes using their regular clients (for nodes without admin port)."""
    await push_config(json.dumps(generate_config(nodes)), [node.client for node in nodes])


async def finalize_migration(nodes, src_idx, dst_idx, src_slots, dst_slots):
    """Clear migrations on src, update slot ownership, push config."""
    nodes[src_idx].migrations = []
    nodes[src_idx].slots = src_slots
    nodes[dst_idx].slots = dst_slots
    await apply_config(nodes)


async def create_cluster(df_factory, count, **kwargs):
    """Create `count` cluster instances, start them, and return (instances, nodes)."""
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port), **kwargs)
        for _ in range(count)
    ]
    df_factory.start_all(instances)
    nodes = [await create_node_info(instance) for instance in instances]
    return instances, nodes


async def wait_for_status(admin_client, node_id, status, timeout=10):
    get_status = lambda: admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", node_id
    )

    if not isinstance(status, list):
        status = [status]

    async for states, breaker in tick_timer(get_status, timeout=timeout):
        with breaker:
            assert len(states) != 0 and all(state[2] in status for state in states), states


async def wait_for_ft_index_creation(client, idx_name, timeout=5):
    get_status = lambda: client.execute_command("FT.INFO", idx_name)

    async for states, breaker in tick_timer(get_status, timeout=timeout):
        with breaker:
            assert len(states) != 0, states


async def wait_for_error(admin_client, node_id, error, timeout=10):
    get_status = lambda: admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", node_id
    )

    async for states, breaker in tick_timer(get_status, timeout=timeout):
        with breaker:
            assert len(states) != 0 and all(error == state[4] for state in states), states


async def wait_for_migration_start(admin_client, node_id):
    while (
        len(await admin_client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS", node_id))
        == 0
    ):
        await asyncio.sleep(0.1)


async def check_for_no_state_status(admin_clients):
    for client in admin_clients:
        state = await client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
        if len(state) != 0:
            logging.debug(f"SLOT-MIGRATION-STATUS is {state}, instead of NO_STATE")
            assert False


def key_slot(key_str) -> int:
    key = str.encode(key_str)
    return crc_hqx(key, 0) % 16384


async def get_node_id(connection):
    id = await connection.execute_command("CLUSTER MYID")
    assert isinstance(id, str)
    return id


def stop_and_get_restore_log(instance):
    instance.stop()
    lines = instance.find_in_logs("RestoreStreamer LSN")
    assert len(lines) == 1
    line = lines[0]
    logging.debug(f"Streamer log line: {line}")
    return line


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "cluster_search": "yes"})
async def test_SearchRequestDistribution(df_factory: DflyInstanceFactory):
    """
    Create cluster of 3 nodes.
    Send FT.CREATE to first node and check that index was created on all nodes.
    Search for all documents from cluster.
    """

    instances, nodes = await create_cluster(
        df_factory, 3, vmodule="coordinator=2,search_family=3,protocol_client=3"
    )
    nodes[0].slots = [(0, 5259)]
    nodes[1].slots = [(5260, 10519)]
    nodes[2].slots = [(10520, 16383)]

    await apply_config(nodes)

    assert (
        await nodes[0].client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"
        )
        == "OK"
    )

    for node in nodes:
        await wait_for_ft_index_creation(node.client, "idx")

    cclient = instances[0].cluster_client()

    docs_num = 100
    for i in range(docs_num):
        assert await cclient.execute_command("HSET", f"s{i}", "title", f"test {i}") == 1

    async def search_test():
        res = await nodes[0].client.execute_command(
            "FT.SEARCH", "idx", "@title:test", "text", "LIMIT", "0", "1000"
        )
        assert res[0] == docs_num
        for i in range(docs_num):
            assert f"s{i}" in res

    await asyncio.gather(*(search_test() for _ in range(docs_num)))


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "cluster_search": "yes"})
async def test_SortedSearchRequest(df_factory: DflyInstanceFactory):
    """
    Create cluster of 3 nodes.
    Execute Search request with sorting on indexed field.
    """

    instances, nodes = await create_cluster(
        df_factory, 3, vmodule="coordinator=2,search_family=3,protocol_client=3"
    )
    nodes[0].slots = [(0, 5259)]
    nodes[1].slots = [(5260, 10519)]
    nodes[2].slots = [(10520, 16383)]

    await apply_config(nodes)

    assert (
        await nodes[0].client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT", "size", "NUMERIC"
        )
        == "OK"
    )

    for node in nodes:
        await wait_for_ft_index_creation(node.client, "idx")

    cclient = instances[0].cluster_client()

    docs_num = 100
    for i in range(docs_num):
        assert (
            await cclient.execute_command("HSET", f"s{i}", "title", f"test {i}", "size", f"{i}")
            == 2
        )

    async def search_test():
        limit_size = random.randint(1, docs_num // 2)
        offset = random.randint(0, docs_num // 2)
        res = await nodes[0].client.execute_command(
            "FT.SEARCH",
            "idx",
            "@title:test",
            "text",
            "SORTBY",
            "size",
            "ASC",
            "LIMIT",
            f"{offset}",
            f"{limit_size}",
        )
        assert res[0] == docs_num
        for i in range(offset, offset + limit_size):
            assert f"s{i}" in res, f"offset: {offset}, limit_size: {limit_size}, res: {res}"

        for i in range(offset):
            assert f"s{i}" not in res

        for i in range(offset + limit_size, docs_num):
            assert f"s{i}" not in res

    await asyncio.gather(*(search_test() for _ in range(2)))
