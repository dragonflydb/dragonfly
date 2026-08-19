import asyncio
import copy
import json
import logging
import subprocess
import time
from binascii import crc_hqx
from dataclasses import dataclass

import pytest
from redis.cluster import RedisCluster
from redis.exceptions import MovedError

from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstance, DflyInstanceFactory
from .utility import (
    assert_eventually,
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


@dfly_args({})
class TestNotEmulated:
    async def test_cluster_commands_fails_when_not_emulate(self, async_client: aioredis.Redis):
        with pytest.raises(aioredis.ResponseError) as respErr:
            await async_client.execute_command("CLUSTER HELP")
        assert "cluster_mode" in str(respErr.value)

        with pytest.raises(aioredis.ResponseError) as respErr:
            await async_client.execute_command("CLUSTER SLOTS")
        assert "emulated" in str(respErr.value)


@dfly_args({"cluster_mode": "emulated"})
class TestEmulated:
    def test_cluster_slots_command(self, df_server, cluster_client: RedisCluster):
        expected = {(0, 16383): {"primary": ("127.0.0.1", df_server.port), "replicas": []}}
        res = cluster_client.execute_command("CLUSTER SLOTS")
        assert expected == res

    def test_cluster_help_command(self, cluster_client: RedisCluster):
        # `target_nodes` is necessary because CLUSTER HELP is not mapped on redis-py
        res = cluster_client.execute_command("CLUSTER", "HELP", target_nodes=RedisCluster.RANDOM)
        assert "HELP" in res
        assert "SLOTS" in res

    def test_cluster_pipeline(self, cluster_client: RedisCluster):
        pipeline = cluster_client.pipeline()
        pipeline.set("foo", "bar")
        pipeline.get("foo")
        val = pipeline.execute()
        assert val == [True, "bar"]


# Unfortunately we can't test --announce_port here because that causes the Python Cluster client to
# throw if it can't access the port in `CLUSTER SLOTS` :|
@dfly_args({"cluster_mode": "emulated", "cluster_announce_ip": "127.0.0.2"})
class TestEmulatedWithAnnounceIp:
    def test_cluster_slots_command(self, df_server, cluster_client: RedisCluster):
        expected = {(0, 16383): {"primary": ("127.0.0.2", df_server.port), "replicas": []}}
        res = cluster_client.execute_command("CLUSTER SLOTS")
        assert expected == res


@dataclass
class ReplicaInfo:
    id: str
    port: int


def verify_slots_result(port: int, answer: list, replicas) -> bool:
    def is_local_host(ip: str) -> bool:
        return ip == "127.0.0.1" or ip == "localhost"

    assert answer[0] == 0  # start shard
    assert answer[1] == 16383  # last shard

    info = answer[2]
    assert len(info) == 3
    ip_addr = info[0]
    assert is_local_host(ip_addr)
    assert info[1] == port

    # Replicas
    assert len(answer) == 3 + len(replicas)
    for i in range(3, len(replicas)):
        replica = replicas[i - 3]
        rep_info = answer[i]
        assert len(rep_info) == 3
        ip_addr = rep_info[0]
        assert is_local_host(ip_addr)
        assert rep_info[1] == replica.port
        assert rep_info[2] == replica.id

    return True


# --managed_service_info means that Dragonfly is running in a managed service, so some details
# are hidden from users, see https://github.com/dragonflydb/dragonfly/issues/4173
@dfly_args({"proactor_threads": 4, "cluster_mode": "emulated", "managed_service_info": "true"})
async def test_emulated_cluster_with_replicas(df_factory):
    master = df_factory.create(port=next(next_port), admin_port=next(next_port))
    replicas = [df_factory.create(port=next(next_port)) for i in range(1, 3)]

    df_factory.start_all([master, *replicas])

    c_master = master.client()
    c_master_admin = master.admin_client()
    master_id = await c_master.execute_command("CLUSTER MYID")

    c_replicas = [replica.client() for replica in replicas]
    replica_ids = [(await c_replica.execute_command("CLUSTER MYID")) for c_replica in c_replicas]

    for replica, c_replica in zip(replicas, c_replicas):
        res = await c_replica.execute_command("CLUSTER SLOTS")
        assert len(res) == 1
        assert verify_slots_result(port=replica.port, answer=res[0], replicas=[])

    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(port=master.port, answer=res[0], replicas=[])

    # Connect replicas to master
    for replica, c_replica in zip(replicas, c_replicas):
        rc = await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        assert rc == "OK"

    await asyncio.sleep(0.5)

    for replica, c_replica in zip(replicas, c_replicas):
        res = await c_replica.execute_command("CLUSTER SLOTS")
        assert verify_slots_result(
            port=master.port, answer=res[0], replicas=[ReplicaInfo(replica.port, id)]
        )

    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        port=master.port,
        answer=res[0],
        replicas=[],
    )

    res = await c_master_admin.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        port=master.port,
        answer=res[0],
        replicas=[ReplicaInfo(id, replica.port) for id, replica in zip(replica_ids, replicas)],
    )

    assert await c_master.execute_command("CLUSTER NODES") == {
        f"127.0.0.1:{master.port}": {
            "connected": True,
            "epoch": "0",
            "flags": "myself,master",
            "hostname": "",
            "last_ping_sent": "0",
            "last_pong_rcvd": "0",
            "master_id": "-",
            "migrations": [],
            "node_id": master_id,
            "slots": [["0", "16383"]],
        },
    }

    assert await c_master_admin.execute_command("CLUSTER NODES") == {
        f"127.0.0.1:{master.port}": {
            "connected": True,
            "epoch": "0",
            "flags": "myself,master",
            "hostname": "",
            "last_ping_sent": "0",
            "last_pong_rcvd": "0",
            "master_id": "-",
            "migrations": [],
            "node_id": master_id,
            "slots": [["0", "16383"]],
        },
        f"127.0.0.1:{replicas[0].port}": {
            "connected": True,
            "epoch": "0",
            "flags": "slave",
            "hostname": "",
            "last_ping_sent": "0",
            "last_pong_rcvd": "0",
            "master_id": master_id,
            "migrations": [],
            "node_id": replica_ids[0],
            "slots": [],
        },
        f"127.0.0.1:{replicas[1].port}": {
            "connected": True,
            "epoch": "0",
            "flags": "slave",
            "hostname": "",
            "last_ping_sent": "0",
            "last_pong_rcvd": "0",
            "master_id": master_id,
            "migrations": [],
            "node_id": replica_ids[1],
            "slots": [],
        },
    }


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_managed_service_info(df_factory):
    master = df_factory.create(port=next(next_port), admin_port=next(next_port))
    replica = df_factory.create(port=next(next_port), admin_port=next(next_port))

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_master_admin = master.admin_client()
    master_id = await c_master.execute_command("CLUSTER MYID")

    c_replica = replica.client()
    c_replica_admin = replica.admin_client()
    replica_id = await c_replica.execute_command("CLUSTER MYID")

    # Connect replicas to master
    rc = await c_replica_admin.execute_command(f"REPLICAOF localhost {master.port}")
    assert rc == "OK"
    await wait_available_async(c_replica)

    nodes = [await create_node_info(master)]
    nodes[0].slots = [(0, 16383)]
    nodes[0].replicas = [await create_node_info(replica)]
    await push_config(json.dumps(generate_config(nodes)), [master.client(), replica.client()])

    expected_hidden_cluster_slots = [
        [
            0,
            16383,
            [
                "127.0.0.1",
                master.port,
                master_id,
            ],
        ],
    ]
    expected_full_cluster_slots = copy.deepcopy(expected_hidden_cluster_slots)
    expected_full_cluster_slots[0].append(
        [
            "127.0.0.1",
            replica.port,
            replica_id,
        ]
    )
    assert await c_master.execute_command("CLUSTER SLOTS") == expected_full_cluster_slots
    assert await c_master_admin.execute_command("CLUSTER SLOTS") == expected_full_cluster_slots

    expected_hidden_cluster_nodes = {
        f"127.0.0.1:{master.port}": {
            "connected": True,
            "epoch": "0",
            "flags": "myself,master",
            "hostname": "",
            "last_ping_sent": "0",
            "last_pong_rcvd": "0",
            "master_id": "-",
            "migrations": [],
            "node_id": master_id,
            "slots": [["0", "16383"]],
        },
    }
    expected_full_cluster_nodes = copy.deepcopy(expected_hidden_cluster_nodes)
    expected_full_cluster_nodes[f"127.0.0.1:{replica.port}"] = {
        "connected": True,
        "epoch": "0",
        "flags": "slave",
        "hostname": "",
        "last_ping_sent": "0",
        "last_pong_rcvd": "0",
        "master_id": master_id,
        "migrations": [],
        "node_id": replica_id,
        "slots": [],
    }
    assert await c_master.execute_command("CLUSTER NODES") == expected_full_cluster_nodes
    assert await c_master_admin.execute_command("CLUSTER NODES") == expected_full_cluster_nodes

    expected_hidden_cluster_shards = [
        [
            "slots",
            [0, 16383],
            "nodes",
            [
                [
                    "id",
                    master_id,
                    "endpoint",
                    "127.0.0.1",
                    "ip",
                    "127.0.0.1",
                    "port",
                    master.port,
                    "role",
                    "master",
                    "replication-offset",
                    0,
                    "health",
                    "online",
                ],
            ],
        ],
    ]
    expected_full_cluster_shards = copy.deepcopy(expected_hidden_cluster_shards)
    expected_full_cluster_shards[0][3].append(
        [
            "id",
            replica_id,
            "endpoint",
            "127.0.0.1",
            "ip",
            "127.0.0.1",
            "port",
            replica.port,
            "role",
            "replica",
            "replication-offset",
            0,
            "health",
            "online",
        ]
    )
    assert await c_master.execute_command("CLUSTER SHARDS") == expected_full_cluster_shards
    assert await c_master_admin.execute_command("CLUSTER SHARDS") == expected_full_cluster_shards

    # this flag doesn't affect cluster anymore so the results will be the same
    await c_master.execute_command("config set managed_service_info true")

    assert await c_master.execute_command("CLUSTER SLOTS") == expected_full_cluster_slots
    assert await c_master_admin.execute_command("CLUSTER SLOTS") == expected_full_cluster_slots

    assert await c_master.execute_command("CLUSTER NODES") == expected_full_cluster_nodes
    assert await c_master_admin.execute_command("CLUSTER NODES") == expected_full_cluster_nodes

    assert await c_master.execute_command("CLUSTER SHARDS") == expected_full_cluster_shards
    assert await c_master_admin.execute_command("CLUSTER SHARDS") == expected_full_cluster_shards


@dfly_args({"cluster_mode": "emulated"})
async def test_cluster_info(async_client):
    res = await async_client.execute_command("CLUSTER INFO")
    assert len(res) == 16
    assert res == {
        "cluster_current_epoch": "1",
        "cluster_known_nodes": "1",
        "cluster_my_epoch": "1",
        "cluster_size": "1",
        "cluster_slots_assigned": "16384",
        "cluster_slots_fail": "0",
        "cluster_slots_ok": "16384",
        "cluster_slots_pfail": "0",
        "cluster_state": "ok",
        "cluster_stats_messages_meet_received": "0",
        "cluster_stats_messages_ping_received": "1",
        "cluster_stats_messages_ping_sent": "1",
        "cluster_stats_messages_pong_received": "1",
        "cluster_stats_messages_pong_sent": "1",
        "cluster_stats_messages_received": "1",
        "cluster_stats_messages_sent": "1",
    }


@dfly_args({"cluster_mode": "emulated", "cluster_announce_ip": "127.0.0.2"})
@pytest.mark.asyncio
async def test_cluster_nodes(df_server, async_client):
    res = await async_client.execute_command("CLUSTER NODES")
    assert len(res) == 1
    info = res[f"127.0.0.2:{df_server.port}"]
    assert res is not None
    assert info["connected"] == True
    assert info["epoch"] == "0"
    assert info["flags"] == "myself,master"
    assert info["last_ping_sent"] == "0"
    assert info["slots"] == [["0", "16383"]]
    assert info["master_id"] == "-"


"""
Test that slot ownership changes correctly with config changes.

Add a key to node0, then move the slot ownership to node1 and see that they both behave as
intended.
Also add keys to each of them that are *not* moved, and see that they are unaffected by the move.
"""


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "cluster_node_id": "inigo montoya"})
async def test_cluster_node_id(df_factory: DflyInstanceFactory):
    node = df_factory.create(port=next(next_port))
    df_factory.start_all([node])

    conn = node.client()
    assert "inigo montoya" == await get_node_id(conn)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_slot_ownership_changes(df_factory: DflyInstanceFactory):
    # Start and configure cluster with 2 nodes
    nodes = [df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)]

    df_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]
    c_nodes_admin = [node.admin_client() for node in nodes]

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes))

    config = f"""
      [
        {{
          "slot_ranges": [
            {{
              "start": 0,
              "end": LAST_SLOT_CUTOFF
            }}
          ],
          "master": {{
            "id": "{node_ids[0]}",
            "ip": "localhost",
            "port": {nodes[0].port}
          }},
          "replicas": []
        }},
        {{
          "slot_ranges": [
            {{
              "start": NEXT_SLOT_CUTOFF,
              "end": 16383
            }}
          ],
          "master": {{
            "id": "{node_ids[1]}",
            "ip": "localhost",
            "port": {nodes[1].port}
          }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5259").replace("NEXT_SLOT_CUTOFF", "5260"),
        c_nodes_admin,
    )

    # Slot for "KEY1" is 5259

    # Insert a key that should stay in node0
    assert await c_nodes[0].set("KEY0", "value")

    # And to node1 (so it happens that 'KEY0' belongs to 0 and 'KEY2' to 1)
    assert await c_nodes[1].set("KEY2", "value")

    # Insert a key that we will move ownership of to node1 (but without migration yet)
    assert await c_nodes[0].set("KEY1", "value")
    assert await c_nodes[0].execute_command("DBSIZE") == 2

    # Make sure that node0 owns "KEY0"
    assert (await c_nodes[0].get("KEY0")) == "value"

    # Make sure that "KEY1" is not owned by node1
    with pytest.raises((MovedError, aioredis.ResponseError)) as e:
        await c_nodes[1].set("KEY1", "value")

    assert e.value.args[0].endswith(f"5259 localhost:{nodes[0].port}")

    # And that node1 only has 1 key ("KEY2")
    assert await c_nodes[1].execute_command("DBSIZE") == 1

    print("Moving ownership over 5259 ('KEY1') to other node")

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5258").replace("NEXT_SLOT_CUTOFF", "5259"),
        c_nodes_admin,
    )

    # node0 should have removed "KEY1" as it no longer owns it
    # deleting non owned keys is background operation therefore we add timeout to this check
    @assert_eventually(times=2)
    async def check_dbsize(node_index, expected_size):
        assert await c_nodes[node_index].execute_command("DBSIZE") == expected_size

    await check_dbsize(node_index=0, expected_size=1)
    # node0 should still own "KEY0" though
    assert (await c_nodes[0].get("KEY0")) == "value"
    # node1 should still have "KEY2"
    assert await c_nodes[1].execute_command("DBSIZE") == 1

    # Now node0 should reply with MOVED for "KEY1"
    with pytest.raises((MovedError, aioredis.ResponseError)) as e:
        await c_nodes[0].set("KEY1", "value")

    assert e.value.args[0].endswith(f"5259 localhost:{nodes[1].port}")

    # And node1 should own it and allow using it
    assert await c_nodes[1].set("KEY1", "value")
    assert await c_nodes[1].execute_command("DBSIZE") == 2

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
            "id": "{node_ids[0]}",
            "ip": "localhost",
            "port": {nodes[0].port}
          }},
          "replicas": []
        }}
      ]
    """
    await push_config(config, c_nodes_admin)

    assert await c_nodes[0].execute_command("DBSIZE") == 1
    assert (await c_nodes[0].get("KEY0")) == "value"
    await check_dbsize(node_index=1, expected_size=0)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_flush_slots_after_config_change(df_factory: DflyInstanceFactory):
    # Start and configure cluster with 1 master and 1 replica, both own all slots
    master = df_factory.create(port=next(next_port), admin_port=next(next_port))
    replica = df_factory.create(port=next(next_port), admin_port=next(next_port))
    df_factory.start_all([master, replica])

    c_master = master.client()
    c_master_admin = master.admin_client()
    master_id = await get_node_id(c_master)

    c_replica = replica.client()
    c_replica_admin = replica.admin_client()
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

    await c_master.execute_command("debug", "populate", "100000")
    assert await c_master.execute_command("dbsize") == 100_000

    # Setup replication and make sure that it works properly.
    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await check_all_replicas_finished([c_replica], c_master)
    assert await c_replica.execute_command("dbsize") == 100_000

    resp = await c_master_admin.execute_command("dflycluster", "getslotinfo", "slots", "0")
    assert resp[0][0] == 0
    slot_0_size = resp[0][2]
    print(f"Slot 0 size = {slot_0_size}")
    assert slot_0_size > 0

    config = f"""
      [
        {{
          "slot_ranges": [
            {{
              "start": 1,
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
        }},
        {{
          "slot_ranges": [
            {{
              "start": 0,
              "end": 0
            }}
          ],
          "master": {{
            "id": "other-master",
            "ip": "localhost",
            "port": 9000
          }},
          "replicas": [
            {{
              "id": "other-replica",
              "ip": "localhost",
              "port": 9001
            }}
          ]
        }}
      ]
    """
    await push_config(config, [c_master_admin, c_replica_admin])

    await check_all_replicas_finished([c_replica], c_master)
    expected_size = 100_000 - slot_0_size
    cmd = ("DFLYCLUSTER", "GETSLOTINFO", "SLOTS", "0")

    @assert_eventually(timeout=5)
    async def slot_zero_flushed():
        m_slots, repl_slots, master_size, replica_size = await asyncio.gather(
            c_master_admin.execute_command(*cmd),
            c_replica_admin.execute_command(*cmd),
            c_master.execute_command("DBSIZE"),
            c_replica.execute_command("DBSIZE"),
        )

        assert m_slots[0][2] == repl_slots[0][2] == 0
        assert master_size == replica_size == expected_size

    await slot_zero_flushed()


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_config_consistency(df_factory: DflyInstanceFactory):
    # Check slot migration from one node to another
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 5259)]
    nodes[1].slots = [(5260, 16383)]

    await apply_config(nodes)

    await check_for_no_state_status([node.admin_client for node in nodes])

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(5200, 5259)], nodes[1].id)
    )

    # Push config to source node. Migration will not start until target node gets the config as well.
    logging.debug("Push migration config to source node")
    await push_config(json.dumps(generate_config(nodes)), [nodes[0].admin_client])

    # some delay to check that migration isn't started until we send config to target node
    await asyncio.sleep(0.2)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "CONNECTING")
    await check_for_no_state_status([nodes[1].admin_client])

    logging.debug("Push migration config to target node")
    await push_config(json.dumps(generate_config(nodes)), [nodes[1].admin_client])

    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = [(0, 5199)]
    nodes[1].slots = [(5200, 16383)]

    logging.debug("remove finished migrations")
    await apply_config(nodes)

    await check_for_no_state_status([node.admin_client for node in nodes])


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_config_reapply(df_factory: DflyInstanceFactory):
    """Check data migration from one node to another."""
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 8000)]
    nodes[1].slots = [(8001, 16383)]

    logging.debug("Pushing data to slot 6XXX")
    SIZE = 10_000
    await apply_config(nodes)
    for i in range(SIZE):
        assert await nodes[0].admin_client.set(f"{{key50}}:{i}", i)  # key50 belongs to slot 6686
    assert [SIZE, 0] == [await node.admin_client.dbsize() for node in nodes]

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(6000, 8000)], nodes[1].id)
    ]
    logging.debug("Migrating slots 6000-8000")
    await apply_config(nodes)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    assert [SIZE, SIZE] == [await node.client.dbsize() for node in nodes]

    logging.debug("Reapply config with migration")
    await apply_config(nodes)

    await asyncio.sleep(0.1)
    assert [SIZE, SIZE] == [await node.client.dbsize() for node in nodes]

    logging.debug("Finalizing migration")
    await finalize_migration(nodes, 0, 1, [(0, 6000)], [(6001, 16383)])
    logging.debug("Migration finalized")

    await asyncio.sleep(1)
    assert [0, SIZE] == [await node.client.dbsize() for node in nodes]

    for i in range(SIZE):
        assert str(i) == await nodes[1].client.get(f"{{key50}}:{i}")


@dfly_args(
    {
        "proactor_threads": 1,
        "cluster_mode": "yes",
        "cluster_node_id": "0" * 40,
    }
)
async def test_cluster_config_slot_overflow_doesnt_crash(df_factory: DflyInstanceFactory):
    instance = df_factory.create(port=next(next_port))
    df_factory.start_all([instance])
    client = instance.client()
    node_id = "0" * 40

    # Build invalid config JSON manually - 1E383 is a valid JSON number but overflows uint16_t.
    # We must NOT use json.dumps here because Python would reject 1e383 (infinity).
    invalid_config = (
        '[{"slot_ranges":[{"start":0,"end":8191}],'
        '"master":{"id":"' + node_id + '","ip":"127.0.0.1","port":' + str(instance.port) + "},"
        '"replicas":[]},'
        '{"slot_ranges":[{"start":8192,"end":1E383}],'
        '"master":{"id":"' + "1" * 40 + '","ip":"127.0.0.1","port":9999},'
        '"replicas":[]}]'
    )

    pipe = client.pipeline(transaction=False)
    pipe.execute_command("DFLYCLUSTER", "CONFIG", invalid_config)
    pipe.execute_command("CLUSTER", "MYID")
    results = await pipe.execute(raise_on_error=False)

    # CONFIG must return an error (not crash), MYID must still work
    assert isinstance(results[0], Exception)
    assert results[1] == node_id
