import asyncio
import json
import logging
import re
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


# Tests that master commands to the replica are applied regardless of slot ownership
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_replica_sets_non_owned_keys(df_factory: DflyInstanceFactory):
    # Start and configure cluster with 1 master and 1 replica, both own all slots
    master = df_factory.create(admin_port=next(next_port))
    replica = df_factory.create(admin_port=next(next_port))
    df_factory.start_all([master, replica])

    async with master.client() as c_master, master.admin_client() as c_master_admin, replica.client() as c_replica, replica.admin_client() as c_replica_admin:
        master_id = await get_node_id(c_master)
        replica_id = await get_node_id(c_replica)

        config = f"""
        [
          {{
            "slot_ranges": [
              {{
                "start": 0,
                "end": 16383
              }}
            ],
            "master": {{
              "id": "{master_id}",
              "ip": "localhost",
              "port": {master.port}
            }},
            "replicas": [
              {{
                "id": "{replica_id}",
                "ip": "localhost",
                "port": {replica.port}
              }}
            ]
          }}
        ]
      """
        await push_config(config, [c_master_admin, c_replica_admin])

        # Setup replication and make sure that it works properly.
        await c_master.set("key", "value")
        await c_replica.execute_command("REPLICAOF", "localhost", master.port)
        await check_all_replicas_finished([c_replica], c_master)
        assert (await c_replica.get("key")) == "value"
        assert await c_replica.execute_command("dbsize") == 1

        # Tell the replica that it and the master no longer own any data, but don't tell that to the
        # master. This will allow us to set keys on the master and make sure that they are set in the
        # replica.

        replica_config = f"""
        [
          {{
            "slot_ranges": [],
            "master": {{
              "id": "{master_id}",
              "ip": "localhost",
              "port": {master.port}
            }},
            "replicas": [
              {{
                "id": "{replica_id}",
                "ip": "localhost",
                "port": {replica.port}
              }}
            ]
          }},
          {{
            "slot_ranges": [
              {{
                "start": 0,
                "end": 16383
              }}
            ],
            "master": {{
              "id": "non-existing-master",
              "ip": "localhost",
              "port": 1111
            }},
            "replicas": []
          }}
        ]
      """

        await push_config(replica_config, [c_replica_admin])

        # The replica should *not* have deleted the key.
        assert await c_replica.execute_command("dbsize") == 1

        # Set another key on the master, which it owns but the replica does not own.
        await c_master.set("key2", "value")
        await check_all_replicas_finished([c_replica], c_master)

        # See that the key exists in both replica and master
        assert await c_master.execute_command("dbsize") == 2
        assert await c_replica.execute_command("dbsize") == 2

        # The replica should still reply with MOVED, despite having that key.
        with pytest.raises((MovedError, aioredis.ResponseError)) as e:
            await c_replica.get("key2")
            assert False, "Should not be able to get key on non-owner cluster node"

        assert re.search(r"\d+ localhost:1111", e.value.args[0])

        await push_config(replica_config, [c_master_admin])
        await check_all_replicas_finished([c_replica], c_master)
        assert await c_master.execute_command("dbsize") == 0
        assert await c_replica.execute_command("dbsize") == 0


def parse_lag(replication_info: str):
    lags = re.findall("lag=([0-9]+)\r\n", replication_info)
    assert len(lags) == 1
    return int(lags[0])


async def await_no_lag(client: aioredis.Redis, timeout=10):
    start = time.time()
    while (time.time() - start) < timeout:
        lag = parse_lag(await client.execute_command("info replication"))
        print("current lag =", lag)
        if lag == 0:
            return
        await asyncio.sleep(0.05)

    raise RuntimeError("Lag did not reduced to 0!")


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4})
async def test_replicate_cluster(df_factory: DflyInstanceFactory, df_seeder_factory):
    """
    Create dragonfly cluster of 2 nodes.
    Create additional dragonfly server in emulated mode.
    Replicate the dragonfly cluster into a single dragonfly node.
    Send traffic before replication start and while replicating.
    Promote the replica to master and check data consistency between cluster and single node.
    """
    replica = df_factory.create(admin_port=next(next_port), cluster_mode="emulated")
    cluster_nodes = [
        df_factory.create(admin_port=next(next_port), cluster_mode="yes") for i in range(2)
    ]

    # Start instances and connect clients
    df_factory.start_all(cluster_nodes + [replica])
    c_nodes = [node.client() for node in cluster_nodes]

    c_replica = replica.client()

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes))
    config = f"""
      [
        {{
          "slot_ranges": [ {{ "start": 0, "end": LAST_SLOT_CUTOFF }} ],
          "master": {{ "id": "{node_ids[0]}", "ip": "localhost", "port": {cluster_nodes[0].port} }},
          "replicas": []
        }},
        {{
          "slot_ranges": [ {{ "start": NEXT_SLOT_CUTOFF, "end": 16383 }} ],
          "master": {{ "id": "{node_ids[1]}", "ip": "localhost", "port": {cluster_nodes[1].port} }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5259").replace("NEXT_SLOT_CUTOFF", "5260"),
        c_nodes,
    )

    # Fill instances with some data
    seeder = df_seeder_factory.create(
        keys=2000, port=cluster_nodes[0].port, cluster_mode=True, mirror_to_fake_redis=True
    )
    await seeder.run(target_deviation=0.1)

    fill_task = asyncio.create_task(seeder.run())

    # Start replication
    await c_replica.execute_command("REPLICAOF localhost " + str(cluster_nodes[0].port) + " 0 5259")
    await c_replica.execute_command(
        "ADDREPLICAOF localhost " + str(cluster_nodes[1].port) + " 5260 16383"
    )

    # give seeder time to run.
    await asyncio.sleep(1.0)
    # Stop seeder
    seeder.stop()
    await fill_task

    # wait for replication to finish
    await asyncio.gather(*(asyncio.create_task(await_no_lag(c)) for c in c_nodes))

    # promote replica to master and compare data
    await c_replica.execute_command("REPLICAOF NO ONE")
    capture = await seeder.capture()
    assert await seeder.compare(capture, replica.port)
    fake_capture = await seeder.capture_fake_redis()
    assert await seeder.compare(fake_capture, replica.port)


async def await_stable_sync(m_client: aioredis.Redis, replica_port, timeout=10):
    start = time.time()

    async def is_stable():
        role = await m_client.execute_command("role")
        return role == [
            "master",
            [["127.0.0.1", str(replica_port), "online"]],
        ]

    while (time.time() - start) < timeout:
        if await is_stable():
            return
        await asyncio.sleep(0.05)

    raise RuntimeError("Failed to reach stable sync")


@dfly_args({"proactor_threads": 4})
async def test_replicate_disconnect_cluster(
    df_factory: DflyInstanceFactory, df_seeder_factory, proxy_factory
):
    """
    Create dragonfly cluster of 2 nodes and additional dragonfly server in emulated mode.
    Populate the cluster with data
    Replicate the dragonfly cluster into a single dragonfly node and wait for stable sync
    Break connection between cluster node 0 and replica and reconnect
    Promote replica to master
    Compare cluster data and replica data
    """
    replica = df_factory.create(admin_port=next(next_port), cluster_mode="emulated")
    cluster_nodes = [
        df_factory.create(admin_port=next(next_port), cluster_mode="yes") for i in range(2)
    ]

    # Start instances and connect clients
    df_factory.start_all(cluster_nodes + [replica])
    c_nodes = [node.client() for node in cluster_nodes]

    c_replica = replica.client()

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes))
    config = f"""
      [
        {{
          "slot_ranges": [ {{ "start": 0, "end": LAST_SLOT_CUTOFF }} ],
          "master": {{ "id": "{node_ids[0]}", "ip": "localhost", "port": {cluster_nodes[0].port} }},
          "replicas": []
        }},
        {{
          "slot_ranges": [ {{ "start": NEXT_SLOT_CUTOFF, "end": 16383 }} ],
          "master": {{ "id": "{node_ids[1]}", "ip": "localhost", "port": {cluster_nodes[1].port} }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5259").replace("NEXT_SLOT_CUTOFF", "5260"),
        c_nodes,
    )

    # Fill instances with some data
    seeder = df_seeder_factory.create(
        keys=2000, port=cluster_nodes[0].port, cluster_mode=True, mirror_to_fake_redis=True
    )
    await seeder.run(target_deviation=0.1)

    fill_task = asyncio.create_task(seeder.run())

    proxy = await proxy_factory(cluster_nodes[0].port)

    # Start replication
    await c_replica.execute_command("REPLICAOF localhost " + str(proxy.port) + " 0 5259")
    await c_replica.execute_command(
        "ADDREPLICAOF localhost " + str(cluster_nodes[1].port) + " 5260 16383"
    )

    # wait for replication to reach stable state on all nodes
    await asyncio.gather(
        *(asyncio.create_task(await_stable_sync(c, replica.port)) for c in c_nodes)
    )

    # break connection between first node and replica
    await proxy.close()
    await asyncio.sleep(3)

    async def is_first_master_conn_down(conn):
        info = await conn.execute_command("INFO REPLICATION")
        print(info)
        statuses = re.findall("master_link_status:(down|up)\r\n", info)
        assert len(statuses) == 2
        assert statuses[0] == "down"
        assert statuses[1] == "up"

    await is_first_master_conn_down(c_replica)

    # start connection again
    await proxy.start_serving()

    seeder.stop()
    await fill_task

    # wait for stable sync on first master
    await await_stable_sync(c_nodes[0], replica.port)
    # wait for no lag on all cluster nodes
    await asyncio.gather(*(asyncio.create_task(await_no_lag(c)) for c in c_nodes))

    # promote replica to master and compare data
    await c_replica.execute_command("REPLICAOF NO ONE")
    capture = await seeder.capture()
    assert await seeder.compare(capture, replica.port)
    fake_capture = await seeder.capture_fake_redis()
    assert await seeder.compare(fake_capture, replica.port)


def is_offset_eq_master_repl_offset(replication_info: str):
    offset = re.findall("offset=([0-9]+),", replication_info)
    assert len(offset) == 1
    master_repl_offset = re.findall("master_repl_offset:([0-9]+)\r\n", replication_info)
    assert len(master_repl_offset) == 1
    return int(offset[0]) == int(master_repl_offset[0])


async def await_eq_offset(client: aioredis.Redis, timeout=20):
    start = time.time()
    while (time.time() - start) < timeout:
        if is_offset_eq_master_repl_offset(await client.execute_command("info replication")):
            return
        await asyncio.sleep(0.05)

    raise RuntimeError("offset not equal!")


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4})
async def test_replicate_redis_cluster(redis_cluster, df_factory, df_seeder_factory):
    """
    Create redis cluster of 3 nodes.
    Create dragonfly server in emulated mode.
    Replicate the redis cluster into a single dragonfly node.
    Send traffic before replication start and while replicating.
    Promote the replica to master and check data consistency between cluster and single dragonfly node.
    """
    replica = df_factory.create(admin_port=next(next_port), cluster_mode="emulated")

    # Start instances and connect clients
    df_factory.start_all([replica])

    redis_cluster_nodes = redis_cluster
    node_clients = [
        aioredis.Redis(decode_responses=True, host="localhost", port=node.port)
        for node in redis_cluster_nodes
    ]

    c_replica = replica.client()

    seeder = df_seeder_factory.create(
        keys=2000, port=redis_cluster_nodes[0].port, cluster_mode=True
    )
    await seeder.run(target_deviation=0.1)

    fill_task = asyncio.create_task(seeder.run())

    # Start replication
    await c_replica.execute_command(
        "REPLICAOF localhost " + str(redis_cluster_nodes[0].port) + " 0 5460"
    )
    await asyncio.sleep(0.5)
    await c_replica.execute_command(
        "ADDREPLICAOF localhost " + str(redis_cluster_nodes[1].port) + " 5461 10922"
    )
    await asyncio.sleep(0.5)
    await c_replica.execute_command(
        "ADDREPLICAOF localhost " + str(redis_cluster_nodes[2].port) + " 10923 16383"
    )

    # give seeder time to run.
    await asyncio.sleep(0.5)
    # Stop seeder
    seeder.stop()
    await fill_task

    # wait for replication to finish
    await asyncio.gather(*(asyncio.create_task(await_eq_offset(client)) for client in node_clients))

    await c_replica.execute_command("REPLICAOF NO ONE")
    capture = await seeder.capture()
    assert await seeder.compare(capture, replica.port)


@dfly_args({"proactor_threads": 4, "pause_wait_timeout": 10})
async def test_replicate_disconnect_redis_cluster(
    redis_cluster, df_factory, df_seeder_factory, proxy_factory
):
    """
    Create redis cluster of 3 nodes.
    Create dragonfly server in emulated mode.
    Replicate the redis cluster into a single dragonfly node.
    Send traffic before replication start and while replicating.
    Close connection between dfly replica and one of master nodes and reconnect
    Send more traffic
    Promote the replica to master and check data consistency between cluster and single dragonfly node.
    """
    replica = df_factory.create(admin_port=next(next_port), cluster_mode="emulated")

    # Start instances and connect clients
    df_factory.start_all([replica])

    redis_cluster_nodes = redis_cluster
    node_clients = [
        aioredis.Redis(decode_responses=True, host="localhost", port=node.port)
        for node in redis_cluster_nodes
    ]

    c_replica = replica.client()

    seeder = df_seeder_factory.create(
        keys=1000, port=redis_cluster_nodes[0].port, cluster_mode=True
    )
    await seeder.run(target_deviation=0.1)

    fill_task = asyncio.create_task(seeder.run())

    proxy = await proxy_factory(redis_cluster_nodes[1].port)

    # Start replication
    await c_replica.execute_command(
        "REPLICAOF localhost " + str(redis_cluster_nodes[0].port) + " 0 5460"
    )
    await c_replica.execute_command("ADDREPLICAOF localhost " + str(proxy.port) + " 5461 10922")
    await c_replica.execute_command(
        "ADDREPLICAOF localhost " + str(redis_cluster_nodes[2].port) + " 10923 16383"
    )

    # give seeder time to run.
    await asyncio.sleep(1)

    # break connection between second node and replica
    await proxy.close()
    await asyncio.sleep(3)

    # check second node connection is down
    info = await c_replica.execute_command("INFO REPLICATION")
    statuses = re.findall("master_link_status:(down|up)\r\n", info)
    assert len(statuses) == 3
    assert statuses[0] == "up"
    assert statuses[1] == "down"
    assert statuses[2] == "up"

    # start connection again
    await proxy.start_serving()

    # give seeder more time to run
    await asyncio.sleep(1)

    # check second node connection is up
    info = await c_replica.execute_command("INFO REPLICATION")
    statuses = re.findall("master_link_status:(down|up)\r\n", info)
    assert len(statuses) == 3
    assert statuses[0] == "up"
    assert statuses[1] == "up"
    assert statuses[2] == "up"

    # give seeder time to run.
    await asyncio.sleep(1)

    # Stop seeder
    seeder.stop()
    await fill_task

    # wait for replication to finish
    await asyncio.gather(*(asyncio.create_task(await_eq_offset(client)) for client in node_clients))

    await c_replica.execute_command("REPLICAOF NO ONE")
    capture = await seeder.capture()
    assert await seeder.compare(capture, replica.port)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_readonly_replication(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    # create cluster master and replica
    # For now replica always should work in read-only mode
    # READONLY command returns always OK without any impact
    # In the future we may decide to implement the same behavior as REDIS
    instances, nodes = await create_cluster(df_factory, 2)
    m1_node, r1_node = nodes
    master_nodes = [m1_node]

    m1_node.slots = [(0, 16383)]
    m1_node.replicas = [r1_node]

    logging.debug("Push initial config")
    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    logging.debug("create data")
    await m1_node.client.execute_command("SET X 1")

    logging.debug("start replication")
    await r1_node.admin_client.execute_command(f"replicaof localhost {m1_node.instance.admin_port}")

    await wait_available_async(r1_node.admin_client)

    assert await r1_node.client.execute_command("GET X") == "1"
    assert await r1_node.client.execute_command("READONLY")
    assert await r1_node.client.execute_command("GET X") == "1"

    # This behavior can be changed in the future
    assert await r1_node.client.execute_command("GET Y") == None

    m1_node.replicas = []

    logging.debug("Push config without replica")
    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    with pytest.raises((MovedError, aioredis.ResponseError)) as moved_error:
        await r1_node.client.execute_command("GET X")

    assert str(moved_error.value).endswith(f"7165 127.0.0.1:{instances[0].port}")

    with pytest.raises((MovedError, aioredis.ResponseError)) as moved_error:
        await r1_node.client.execute_command("GET Y")

    assert str(moved_error.value).endswith(f"3036 127.0.0.1:{instances[0].port}")


@pytest.mark.parametrize("set_cluster_node_id", [True, False])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_replica_takeover_moved(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory, set_cluster_node_id: bool
):
    node_names = ["takeover-m1", "takeover-r1", "takeover-m2", "takeover-r2"]
    instances = [
        df_factory.create(
            port=next(next_port),
            cluster_node_id=node_names[i] if set_cluster_node_id else "",
        )
        for i in range(4)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(n) for n in instances]
    m1, r1, m2, r2 = nodes
    master_nodes = [m1, m2]

    m1.slots = [(0, 9000)]
    m2.slots = [(9001, 16383)]

    m1.replicas = [r1]
    m2.replicas = [r2]

    await push_config(json.dumps(generate_config(master_nodes)), [node.client for node in nodes])

    logging.debug("create data")
    await m1.client.execute_command("SET X 1")
    # Slot number 16022
    await m2.client.execute_command("SET FOOX 1")

    logging.debug("start replication")
    await r1.client.execute_command(f"replicaof localhost {m1.instance.port}")
    await r2.client.execute_command(f"replicaof localhost {m2.instance.port}")

    await wait_available_async(r1.client)

    assert await r1.client.execute_command("GET X") == "1"
    assert await r1.client.execute_command("REPLTAKEOVER 20") == "OK"

    with pytest.raises((MovedError, aioredis.ResponseError)) as moved_error:
        await m1.client.execute_command("GET X")

    assert str(moved_error.value).endswith(f"7165 127.0.0.1:{r1.instance.port}")

    with pytest.raises((MovedError, aioredis.ResponseError)) as moved_error:
        await m1.client.execute_command("GET FOOX")

    assert str(moved_error.value).endswith(f"16022 127.0.0.1:{m2.instance.port}")

    # Try write command on the new master. It should succeed because during takeover,
    # we updated the config as well
    assert await r1.client.execute_command("SET X 2") == "OK"

    master_nodes = [r1, m2]
    r1.slots = [(0, 9000)]
    nodes.pop(0)
    await push_config(json.dumps(generate_config(master_nodes)), [node.client for node in nodes])

    assert await r1.client.execute_command("GET X") == "2"
    assert await m2.client.execute_command("GET FOOX") == "1"

    await r1.client.execute_command("flushall")
    assert await r1.client.dbsize() == 0
    await r1.client.execute_command("SET newk foo")
    # Now bring back m1 as a replica of r1
    nodes.append(m1)
    r1.replicas = [m1]
    await push_config(json.dumps(generate_config(master_nodes)), [node.client for node in nodes])
    await m1.client.execute_command(f"replicaof localhost {r1.instance.port}")
    await check_all_replicas_finished([m1.client], r1.client)
    assert await m1.client.execute_command("GET newk") == "foo"
