import asyncio
import json
import logging
import subprocess
import time
from binascii import crc_hqx
from dataclasses import dataclass

import pytest
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import MovedError

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


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_cluster_sharded_pub_sub(df_factory: DflyInstanceFactory):
    nodes = [df_factory.create(port=next(next_port)) for i in range(2)]
    df_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]

    nodes_info = [(await create_node_info(instance)) for instance in nodes]
    nodes_info[0].slots = [(0, 16383)]
    nodes_info[1].slots = []

    await apply_config_via_client(nodes_info)
    # channel name kostas crc is at slot 2883 which is part of the first node.
    with pytest.raises((MovedError, aioredis.ResponseError)) as moved_error:
        await c_nodes[1].execute_command("SSUBSCRIBE kostas")

    assert str(moved_error.value).endswith(f"2833 127.0.0.1:{nodes[0].port}")

    node_a = ClusterNode("localhost", nodes[0].port)
    node_b = ClusterNode("localhost", nodes[1].port)

    consumer_client = RedisCluster(startup_nodes=[node_a, node_b])
    consumer = consumer_client.pubsub()
    consumer.ssubscribe("kostas")

    await c_nodes[0].execute_command("SPUBLISH kostas hello")
    # We need to sleep cause we use DispatchBrief internally. Otherwise we can't really gurantee
    # that the client received the message
    await asyncio.sleep(2)

    # Consume subscription message result from above
    message = consumer.get_sharded_message(target_node=node_a)
    assert message == {"type": "ssubscribe", "pattern": None, "channel": b"kostas", "data": 1}

    message = consumer.get_sharded_message(target_node=node_a)
    assert message == {"type": "smessage", "pattern": None, "channel": b"kostas", "data": b"hello"}

    consumer.sunsubscribe("kostas")
    await asyncio.sleep(2)
    await c_nodes[0].execute_command("SPUBLISH kostas new_message")
    message = consumer.get_sharded_message(target_node=node_a)
    assert message == {"type": "sunsubscribe", "pattern": None, "channel": b"kostas", "data": 0}


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_cluster_sharded_pubsub_shard_commands(df_factory: DflyInstanceFactory):
    nodes = [df_factory.create(port=next(next_port)) for i in range(2)]
    df_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]

    nodes_info = [(await create_node_info(instance)) for instance in nodes]
    nodes_info[0].slots = [(0, 16383)]
    nodes_info[1].slots = []

    await apply_config_via_client(nodes_info)

    # We are executing SSUBSCRIBE commands and wait for them to be sure that
    # channels are created
    message = await c_nodes[0].execute_command("SSUBSCRIBE pubsub-shard-channel")
    message = await c_nodes[0].execute_command("SSUBSCRIBE shard-channel")

    message = await c_nodes[0].execute_command("PUBSUB SHARDCHANNELS")
    message.sort()
    assert message == ["pubsub-shard-channel", "shard-channel"]

    message = await c_nodes[0].execute_command("PUBSUB SHARDCHANNELS pubsub*")
    assert message == ["pubsub-shard-channel"]

    message = await c_nodes[0].execute_command("PUBSUB SHARDCHANNELS *channel")
    message.sort()
    assert message == ["pubsub-shard-channel", "shard-channel"]

    message = await c_nodes[0].execute_command("PUBSUB SHARDNUMSUB pubsub-shard-channel")
    assert message == ["pubsub-shard-channel", 1]

    message = await c_nodes[0].execute_command(
        "PUBSUB SHARDNUMSUB pubsub-shard-channel shard-channel"
    )
    assert message == ["pubsub-shard-channel", 1, "shard-channel", 1]

    message = await c_nodes[0].execute_command("PUBSUB SHARDNUMSUB")
    assert message == []


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_cluster_sharded_pub_sub_migration(df_factory: DflyInstanceFactory):
    instances = [df_factory.create(port=next(next_port)) for i in range(2)]
    df_factory.start_all(instances)

    c_nodes = [instance.client() for instance in instances]

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config_via_client(nodes)

    # Setup producer and consumer
    node_a = ClusterNode("localhost", instances[0].port)
    node_b = ClusterNode("localhost", instances[1].port)

    consumer_client = RedisCluster(startup_nodes=[node_a, node_b])
    consumer = consumer_client.pubsub()
    consumer.ssubscribe("kostas")

    # Push new config
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.port, [(0, 16383)], nodes[1].id)
    )
    await apply_config_via_client(nodes)

    await wait_for_status(nodes[0].client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("remove finished migrations")
    await apply_config_via_client(nodes)

    # channel name kostas crc is at slot 2883 which is part of the second now.
    with pytest.raises((MovedError, aioredis.ResponseError)) as moved_error:
        await c_nodes[0].execute_command("SSUBSCRIBE kostas")

    assert str(moved_error.value).endswith(f"2833 127.0.0.1:{instances[1].port}")

    # Consume subscription message result from above
    message = consumer.get_sharded_message(target_node=node_a)
    assert message == {"type": "ssubscribe", "pattern": None, "channel": b"kostas", "data": 1}
    message = consumer.get_sharded_message(target_node=node_a)
    assert message == {"type": "sunsubscribe", "pattern": None, "channel": b"kostas", "data": 0}
