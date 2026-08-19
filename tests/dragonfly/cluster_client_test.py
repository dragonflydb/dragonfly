import asyncio
import json
import logging
import random
import subprocess
import time
from binascii import crc_hqx
from dataclasses import dataclass

import pytest
from redis.exceptions import MovedError

from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstance, DflyInstanceFactory
from .utility import (
    DflySeederFactory,
    check_all_replicas_finished,
    skip_if_not_in_github,
    tick_timer,
    wait_available_async,
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


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "admin_port": next(next_port)})
async def test_cluster_blocking_command(df_server):
    c_master = df_server.client()
    c_master_admin = df_server.admin_client()

    config = [
        {
            "slot_ranges": [{"start": 0, "end": 8000}],
            "master": {"id": await get_node_id(c_master), "ip": "10.0.0.1", "port": 7000},
            "replicas": [],
        },
        {
            "slot_ranges": [{"start": 8001, "end": 16383}],
            "master": {"id": "other", "ip": "10.0.0.2", "port": 7000},
            "replicas": [],
        },
    ]

    assert (
        await c_master_admin.execute_command("DFLYCLUSTER", "CONFIG", json.dumps(config))
    ) == "OK"

    assert (await c_master.execute_command("CLUSTER", "KEYSLOT", "keep-local")) == 3479
    assert (await c_master.execute_command("CLUSTER", "KEYSLOT", "remove-key-4")) == 6103

    v1 = asyncio.create_task(c_master.blpop("keep-local", 2))
    v2 = asyncio.create_task(c_master.blpop("remove-key-4", 2))

    await asyncio.sleep(0.1)

    config[0]["slot_ranges"][0]["end"] = 5000
    config[1]["slot_ranges"][0]["start"] = 5001
    assert (
        await c_master_admin.execute_command("DFLYCLUSTER", "CONFIG", json.dumps(config))
    ) == "OK"

    await c_master.lpush("keep-local", "WORKS")

    assert (await v1) == ("keep-local", "WORKS")
    with pytest.raises(MovedError):
        await v2


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_blocking_commands_cancel(df_factory, df_seeder_factory):
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    set_task = asyncio.create_task(nodes[0].client.execute_command("BZPOPMIN set1 0"))
    list_task = asyncio.create_task(nodes[0].client.execute_command("BLPOP list1 0"))

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.port, [(0, 16383)], nodes[1].id)
    )
    await apply_config(nodes)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("remove finished migrations")
    await apply_config(nodes)

    with pytest.raises(MovedError) as set_e_info:
        await set_task
    assert f"3037 127.0.0.1:{instances[1].port}" == str(set_e_info.value)

    with pytest.raises(MovedError) as list_e_info:
        await list_task
    assert f"7141 127.0.0.1:{instances[1].port}" == str(list_e_info.value)


@pytest.mark.parametrize("set_cluster_node_id", [True, False])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_native_client(
    df_factory: DflyInstanceFactory,
    df_seeder_factory: DflySeederFactory,
    set_cluster_node_id: bool,
):
    # Start and configure cluster with 3 masters and 3 replicas
    masters = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            cluster_node_id=f"master{i}" if set_cluster_node_id else "",
        )
        for i in range(3)
    ]
    df_factory.start_all(masters)
    c_masters_admin = [master.admin_client() for master in masters]
    master_ids = await asyncio.gather(*(get_node_id(c) for c in c_masters_admin))

    replicas = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            cluster_node_id=f"replica{i}" if set_cluster_node_id else "",
            replicaof=f"localhost:{masters[i].port}",
        )
        for i in range(3)
    ]
    df_factory.start_all(replicas)
    c_replicas = [replica.client() for replica in replicas]
    await asyncio.gather(*(wait_available_async(c) for c in c_replicas))
    c_replicas_admin = [replica.admin_client() for replica in replicas]
    replica_ids = await asyncio.gather(*(get_node_id(c) for c in c_replicas))

    config = f"""
      [
        {{
          "slot_ranges": [
            {{
              "start": 0,
              "end": 5000
            }}
          ],
          "master": {{
            "id": "{master_ids[0]}",
            "ip": "localhost",
            "port": {masters[0].port}
          }},
          "replicas": [
              {{
                "id": "{replica_ids[0]}",
                "ip": "localhost",
                "port": {replicas[0].port}
              }}
          ]
        }},
        {{
          "slot_ranges": [
            {{
              "start": 5001,
              "end": 10000
            }}
          ],
          "master": {{
            "id": "{master_ids[1]}",
            "ip": "localhost",
            "port": {masters[1].port}
          }},
          "replicas": [
              {{
                "id": "{replica_ids[1]}",
                "ip": "localhost",
                "port": {replicas[1].port}
              }}
          ]
        }},
        {{
          "slot_ranges": [
            {{
              "start": 10001,
              "end": 16383
            }}
          ],
          "master": {{
            "id": "{master_ids[2]}",
            "ip": "localhost",
            "port": {masters[2].port}
          }},
          "replicas": [
              {{
                "id": "{replica_ids[2]}",
                "ip": "localhost",
                "port": {replicas[2].port}
              }}
          ]
        }}
      ]
    """
    await push_config(config, c_masters_admin + c_replicas_admin)

    seeder = df_seeder_factory.create(port=masters[0].port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    client = masters[0].cluster_client()

    assert await client.set("key0", "value") == True
    assert await client.get("key0") == "value"

    async def test_random_keys():
        for i in range(100):
            key = "key" + str(random.randint(0, 100_000))
            assert await client.set(key, "value") == True
            assert await client.get(key) == "value"

    await test_random_keys()

    for i in range(3):
        await check_all_replicas_finished([c_replicas[i]], c_masters_admin[i])

    await asyncio.gather(*(wait_available_async(c) for c in c_replicas))

    # Make sure that getting a value from a replica works as well.
    # We use connections directly to NOT follow 'MOVED' error, as that will redirect to the master.
    for c in c_replicas:
        try:
            assert await c.get("key0")
        except MovedError:
            pass

    # Push new config
    config = f"""
      [
        {{
          "slot_ranges": [
            {{
              "start": 0,
              "end": 4000
            }}
          ],
          "master": {{
            "id": "{master_ids[0]}",
            "ip": "localhost",
            "port": {masters[0].port}
          }},
          "replicas": [
              {{
                "id": "{replica_ids[0]}",
                "ip": "localhost",
                "port": {replicas[0].port}
              }}
          ]
        }},
        {{
          "slot_ranges": [
            {{
              "start": 4001,
              "end": 14000
            }}
          ],
          "master": {{
            "id": "{master_ids[1]}",
            "ip": "localhost",
            "port": {masters[1].port}
          }},
          "replicas": [
              {{
                "id": "{replica_ids[1]}",
                "ip": "localhost",
                "port": {replicas[1].port}
              }}
          ]
        }},
        {{
          "slot_ranges": [
            {{
              "start": 14001,
              "end": 16383
            }}
          ],
          "master": {{
            "id": "{master_ids[2]}",
            "ip": "localhost",
            "port": {masters[2].port}
          }},
          "replicas": [
              {{
                "id": "{replica_ids[2]}",
                "ip": "localhost",
                "port": {replicas[2].port}
              }}
          ]
        }}
      ]
    """
    await push_config(config, c_masters_admin + c_replicas_admin)

    await test_random_keys()
