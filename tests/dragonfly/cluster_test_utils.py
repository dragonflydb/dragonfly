"""Shared models and helpers used across the cluster_*_test.py modules."""

import asyncio
import json
import logging
from binascii import crc_hqx
from dataclasses import dataclass

from redis import asyncio as aioredis

from .instance import DflyInstance
from .utility import tick_timer

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
