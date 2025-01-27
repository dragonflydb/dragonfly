import pytest
import copy
import re
import json
import redis
from binascii import crc_hqx
from redis import asyncio as aioredis
import asyncio
from dataclasses import dataclass

from .instance import DflyInstanceFactory, DflyInstance
from .utility import *
from .replication_test import check_all_replicas_finished
from redis.cluster import RedisCluster
from redis.cluster import ClusterNode
from .proxy import Proxy
from .seeder import Seeder, SeederBase, StaticSeeder

from . import dfly_args

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
        except Exception as e:
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
    except FileNotFoundError as e:
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
            },
            "replicas": [
                {
                    "id": replica.id,
                    "ip": "127.0.0.1",
                    "port": replica.instance.port,
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


async def wait_for_status(admin_client, node_id, status, timeout=10):
    get_status = lambda: admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", node_id
    )

    if not isinstance(status, list):
        status = [status]

    async for states, breaker in tick_timer(get_status, timeout=timeout):
        with breaker:
            assert len(states) != 0 and all(state[2] in status for state in states), states


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


def extract_int_after_prefix(prefix, line):
    match = re.search(prefix + "(\\d+)", line)
    assert match
    return int(match.group(1))


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
    def test_cluster_slots_command(self, df_server, cluster_client: redis.RedisCluster):
        expected = {(0, 16383): {"primary": ("127.0.0.1", df_server.port), "replicas": []}}
        res = cluster_client.execute_command("CLUSTER SLOTS")
        assert expected == res

    def test_cluster_help_command(self, cluster_client: redis.RedisCluster):
        # `target_nodes` is necessary because CLUSTER HELP is not mapped on redis-py
        res = cluster_client.execute_command("CLUSTER HELP", target_nodes=redis.RedisCluster.RANDOM)
        assert "HELP" in res
        assert "SLOTS" in res

    def test_cluster_pipeline(self, cluster_client: redis.RedisCluster):
        pipeline = cluster_client.pipeline()
        pipeline.set("foo", "bar")
        pipeline.get("foo")
        val = pipeline.execute()
        assert val == [True, "bar"]


# Unfortunately we can't test --announce_port here because that causes the Python Cluster client to
# throw if it can't access the port in `CLUSTER SLOTS` :|
@dfly_args({"cluster_mode": "emulated", "cluster_announce_ip": "127.0.0.2"})
class TestEmulatedWithAnnounceIp:
    def test_cluster_slots_command(self, df_server, cluster_client: redis.RedisCluster):
        expected = {(0, 16383): {"primary": ("127.0.0.2", df_server.port), "replicas": []}}
        res = cluster_client.execute_command("CLUSTER SLOTS")
        assert expected == res


@dataclass
class ReplicaInfo:
    id: string
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
    replicas = [df_factory.create(port=next(next_port), logtostdout=True) for i in range(1, 3)]

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

    await c_master.execute_command("config set managed_service_info true")

    assert await c_master.execute_command("CLUSTER SLOTS") == expected_hidden_cluster_slots
    assert await c_master_admin.execute_command("CLUSTER SLOTS") == expected_full_cluster_slots

    assert await c_master.execute_command("CLUSTER NODES") == expected_hidden_cluster_nodes
    assert await c_master_admin.execute_command("CLUSTER NODES") == expected_full_cluster_nodes

    assert await c_master.execute_command("CLUSTER SHARDS") == expected_hidden_cluster_shards
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
    try:
        await c_nodes[1].set("KEY1", "value")
        assert False, "Should not be able to set key on non-owner cluster node"
    except redis.exceptions.ResponseError as e:
        assert e.args[0] == f"MOVED 5259 localhost:{nodes[0].port}"

    # And that node1 only has 1 key ("KEY2")
    assert await c_nodes[1].execute_command("DBSIZE") == 1

    print("Moving ownership over 5259 ('KEY1') to other node")

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5258").replace("NEXT_SLOT_CUTOFF", "5259"),
        c_nodes_admin,
    )

    # node0 should have removed "KEY1" as it no longer owns it
    assert await c_nodes[0].execute_command("DBSIZE") == 1
    # node0 should still own "KEY0" though
    assert (await c_nodes[0].get("KEY0")) == "value"
    # node1 should still have "KEY2"
    assert await c_nodes[1].execute_command("DBSIZE") == 1

    # Now node0 should reply with MOVED for "KEY1"
    try:
        await c_nodes[0].set("KEY1", "value")
        assert False, "Should not be able to set key on non-owner cluster node"
    except redis.exceptions.ResponseError as e:
        assert e.args[0] == f"MOVED 5259 localhost:{nodes[1].port}"

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
    assert await c_nodes[1].execute_command("DBSIZE") == 0


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
        try:
            await c_replica.get("key2")
            assert False, "Should not be able to get key on non-owner cluster node"
        except redis.exceptions.ResponseError as e:
            assert re.match(r"MOVED \d+ localhost:1111", e.args[0])

        await push_config(replica_config, [c_master_admin])
        await asyncio.sleep(0.5)
        assert await c_master.execute_command("dbsize") == 0
        assert await c_replica.execute_command("dbsize") == 0


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

    await asyncio.sleep(0.5)

    assert await c_master.execute_command("dbsize") == (100_000 - slot_0_size)
    assert await c_replica.execute_command("dbsize") == (100_000 - slot_0_size)


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
    with pytest.raises(aioredis.ResponseError) as e_info:
        await v2
    assert "MOVED" in str(e_info.value)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_blocking_commands_cancel(df_factory, df_seeder_factory):
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    set_task = asyncio.create_task(nodes[0].client.execute_command("BZPOPMIN set1 0"))
    list_task = asyncio.create_task(nodes[0].client.execute_command("BLPOP list1 0"))

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.port, [(0, 16383)], nodes[1].id)
    )
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("remove finished migrations")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    with pytest.raises(aioredis.ResponseError) as set_e_info:
        await set_task
    assert f"MOVED 3037 127.0.0.1:{instances[1].port}" == str(set_e_info.value)

    with pytest.raises(aioredis.ResponseError) as list_e_info:
        await list_task
    assert f"MOVED 7141 127.0.0.1:{instances[1].port}" == str(list_e_info.value)


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
    c_masters = [master.client() for master in masters]
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
    await asyncio.gather(*(wait_available_async(c) for c in c_replicas))

    # Make sure that getting a value from a replica works as well.
    # We use connections directly to NOT follow 'MOVED' error, as that will redirect to the master.
    for c in c_replicas:
        try:
            assert await c.get("key0")
        except redis.exceptions.ResponseError as e:
            assert e.args[0].startswith("MOVED")

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


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_config_consistency(df_factory: DflyInstanceFactory):
    # Check slot migration from one node to another
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 5259)]
    nodes[1].slots = [(5260, 16383)]

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

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
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await check_for_no_state_status([node.admin_client for node in nodes])


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_flushall_during_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory
):
    # Check data migration from one node to another
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="cluster_family=2,outgoing_slot_migration=2,incoming_slot_migration=2,streamer=2",
            logtostdout=True,
        )
        for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    seeder = df_seeder_factory.create(keys=10_000, port=nodes[0].instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )

    logging.debug("Start migration")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await nodes[0].client.execute_command("flushall")

    status1 = await nodes[1].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[0].id
    )
    assert (
        len(status1) == 0 or "FINISHED" not in status1[0]
    ), "Weak test case - finished migration too early"

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    logging.debug("Finalizing migration")
    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    logging.debug("Migration finalized")

    assert await nodes[0].client.dbsize() == 0


@pytest.mark.parametrize("interrupt", [False, True])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_data_migration(df_factory: DflyInstanceFactory, interrupt: bool):
    # Check data migration from one node to another
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
        )
        for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 9000)]
    nodes[1].slots = [(9001, 16383)]

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    for i in range(20):
        key = "KEY" + str(i)
        assert await nodes[key_slot(key) // 9001].client.set(key, "value")

    assert await nodes[0].client.execute_command("DBSIZE") == 10

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(3000, 9000)], nodes[1].id)
    )

    logging.debug("Start migration")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    if interrupt:  # Test nodes properly shut down with pending migration
        await asyncio.sleep(random.random())

        # random instance
        stop = random.getrandbits(1)
        keep = 1 - stop

        nodes[stop].instance.stop()

        slots = await nodes[keep].admin_client.execute_command("CLUSTER SLOTS")
        slots.sort(key=lambda cfg: cfg[0])
        assert 0 in slots[0] and 9000 in slots[0]
        assert 9001 in slots[1] and 16383 in slots[1]

        return

    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED")

    for i in range(20, 22):
        key = "KEY" + str(i)
        assert await nodes[0 if (key_slot(key) // 3000) == 0 else 1].client.set(key, "value")

    status = await nodes[0].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[1].id
    )
    status[0].pop()
    assert status[0] == ["out", nodes[1].id, "FINISHED", 7]

    status = await nodes[1].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[0].id
    )
    status[0].pop()
    assert status[0] == ["in", nodes[0].id, "FINISHED", 7]

    nodes[0].migrations = []
    nodes[0].slots = [(0, 2999)]
    nodes[1].slots = [(3000, 16383)]
    logging.debug("remove finished migrations")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    for i in range(22):
        key = "KEY" + str(i)
        assert await nodes[0 if (key_slot(key) // 3000) == 0 else 1].client.set(key, "value")

    assert await nodes[1].client.execute_command("DBSIZE") == 19

    await check_for_no_state_status([node.admin_client for node in nodes])


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes", "cache_mode": "true"})
async def test_migration_with_key_ttl(df_factory):
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await nodes[0].client.execute_command("set k_with_ttl v1 EX 2")
    await nodes[0].client.execute_command("set k_without_ttl v2")
    await nodes[0].client.execute_command("set k_sticky v3")
    assert await nodes[0].client.execute_command("stick k_sticky") == 1

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].port, [(0, 16383)], nodes[1].id)
    )
    logging.debug("Start migration")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("finalize migration")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    assert await nodes[1].client.execute_command("get k_with_ttl") == "v1"
    assert await nodes[1].client.execute_command("get k_without_ttl") == "v2"
    assert await nodes[1].client.execute_command("get k_sticky") == "v3"
    assert await nodes[1].client.execute_command("ttl k_with_ttl") > 0
    assert await nodes[1].client.execute_command("ttl k_without_ttl") == -1
    assert await nodes[1].client.execute_command("stick k_sticky") == 0  # Sticky bit already set

    await asyncio.sleep(2)  # Force expiration

    assert await nodes[1].client.execute_command("get k_with_ttl") == None
    assert await nodes[1].client.execute_command("get k_without_ttl") == "v2"
    assert await nodes[1].client.execute_command("ttl k_with_ttl") == -2
    assert await nodes[1].client.execute_command("ttl k_without_ttl") == -1
    assert await nodes[1].client.execute_command("stick k_sticky") == 0


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "migration_finalization_timeout_ms": 5})
async def test_network_disconnect_during_migration(df_factory):
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="cluster_family=9,outgoing_slot_migration=9,incoming_slot_migration=9",
        )
        for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await StaticSeeder(key_target=100000).run(nodes[0].client)
    start_capture = await StaticSeeder.capture(nodes[0].client)

    proxy = Proxy("127.0.0.1", next(next_port), "127.0.0.1", nodes[1].instance.admin_port)
    await proxy.start()
    task = asyncio.create_task(proxy.serve())

    nodes[0].migrations.append(MigrationInfo("127.0.0.1", proxy.port, [(0, 16383)], nodes[1].id))
    try:
        logging.debug("Start migration")
        await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

        for _ in range(10):
            await asyncio.sleep(random.randint(0, 10) / 100)
            logging.debug("drop connections")
            proxy.drop_connection()
            logging.debug(
                await nodes[0].admin_client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
            )

        await wait_for_status(nodes[0].admin_client, nodes[1].id, "SYNC", 20)
    finally:
        await proxy.close(task)

    await proxy.start()
    task = asyncio.create_task(proxy.serve())
    try:
        await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)
        nodes[0].migrations = []
        nodes[0].slots = []
        nodes[1].slots = [(0, 16383)]
        logging.debug("remove finished migrations")
        await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

        assert (await StaticSeeder.capture(nodes[1].client)) == start_capture
    finally:
        await proxy.close(task)


@pytest.mark.parametrize(
    "node_count, segments, keys, huge_values, cache_mode",
    [
        pytest.param(3, 16, 20_000, 10, "false"),
        pytest.param(3, 16, 20_000, 10, "true"),
        # 1mb effectively disables breakdown of huge values.
        # TODO: add a test that mixes huge and small values, see
        # https://github.com/dragonflydb/dragonfly/pull/4144/files/11e5e387d31bcf1bc53dfbb28cf3bcaf094d77fa#r1850130930
        pytest.param(3, 16, 20_000, 1_000_000, "true"),
        pytest.param(3, 16, 20_000, 1_000_000, "false"),
        pytest.param(
            5, 20, 30_000, 1_000_000, "false", marks=[pytest.mark.slow, pytest.mark.opt_only]
        ),
    ],
)
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_fuzzymigration(
    df_factory: DflyInstanceFactory,
    df_seeder_factory,
    node_count: int,
    segments: int,
    keys: int,
    huge_values: int,
    cache_mode: string,
):
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
            serialization_max_chunk_size=huge_values,
            replication_stream_output_limit=10,
            cache_mode=cache_mode,
        )
        for i in range(node_count)
    ]
    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]

    # Generate equally sized ranges and distribute by nodes
    step = 16400 // segments
    for slot_range in [(s, min(s + step - 1, 16383)) for s in range(0, 16383, step)]:
        nodes[random.randint(0, node_count - 1)].slots.append(slot_range)

    # Push config to all nodes
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # Fill instances with some data
    seeder = df_seeder_factory.create(keys=keys, port=nodes[0].instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    # Counter that pushes values to a list
    async def list_counter(key, client: aioredis.RedisCluster):
        try:
            for i in itertools.count(start=1):
                await client.lpush(key, i)
        except asyncio.exceptions.CancelledError:
            return

    # Start ten counters
    counter_keys = [f"_counter{i}" for i in range(10)]
    counter_connections = [nodes[0].instance.cluster_client() for _ in range(10)]
    counters = [
        asyncio.create_task(list_counter(key, conn))
        for key, conn in zip(counter_keys, counter_connections)
    ]

    # Generate capture, capture ignores counter keys
    capture = await seeder.capture()

    # Generate migration plan
    for node_idx, node in enumerate(nodes):
        random.shuffle(node.slots)

        # Decide on number of outgoing slot ranges
        outgoing = [[] for _ in range(node_count)]
        num_outgoing = random.randint(0, len(node.slots))

        # Distribute first 0..num_outgoing
        for slot_range in node.slots[:num_outgoing]:
            dest_idx = random.randint(0, node_count - 1)
            while dest_idx == node_idx:
                dest_idx = random.randint(0, node_count - 1)
            outgoing[dest_idx].append(slot_range)

        for dest_idx, dest_slots in enumerate(outgoing):
            if len(dest_slots) == 0:
                continue

            print(node_idx, "migrates to", dest_idx, "slots", dest_slots)
            node.migrations.append(
                MigrationInfo(
                    ip="127.0.0.1",
                    port=nodes[dest_idx].instance.admin_port,
                    slots=dest_slots,
                    node_id=nodes[dest_idx].id,
                )
            )

    logging.debug("start migrations")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("finish migrations")

    async def all_finished():
        res = True
        for node in nodes:
            states = await node.admin_client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
            logging.debug(states)
            for state in states:
                direction, node_id, st, _, _ = state
                if direction == "out":
                    if st == "FINISHED":
                        m_id = [id for id, x in enumerate(node.migrations) if x.node_id == node_id][
                            0
                        ]
                        node.slots = [s for s in node.slots if s not in node.migrations[m_id].slots]
                        target_node = [n for n in nodes if n.id == node_id][0]
                        target_node.slots.extend(node.migrations[m_id].slots)
                        print(
                            "FINISH migration",
                            node.id,
                            ":",
                            node.migrations[m_id].node_id,
                            " slots:",
                            node.migrations[m_id].slots,
                        )
                        node.migrations.pop(m_id)
                        await push_config(
                            json.dumps(generate_config(nodes)),
                            [node.admin_client for node in nodes],
                        )
                    else:
                        res = False
        return res

    @assert_eventually(times=600)
    async def test_all_finished():
        assert await all_finished()

    await test_all_finished()

    for counter in counters:
        counter.cancel()
        await counter

    # Check counter consistency
    cluster_client = nodes[0].instance.cluster_client()
    for key in counter_keys:
        counter_list = await cluster_client.lrange(key, 0, -1)
        for i, j in zip(counter_list, counter_list[1:]):
            assert int(i) == int(j) + 1, f"Found inconsistent list in {key}: {counter_list}"

    # Compare capture
    assert await seeder.compare(capture, nodes[0].instance.port)

    await asyncio.gather(*[c.aclose() for c in counter_connections])


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_config_reapply(df_factory: DflyInstanceFactory):
    """Check data migration from one node to another."""
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 8000)]
    nodes[1].slots = [(8001, 16383)]

    logging.debug("Pushing data to slot 6XXX")
    SIZE = 10_000
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    for i in range(SIZE):
        assert await nodes[0].admin_client.set(f"{{key50}}:{i}", i)  # key50 belongs to slot 6686
    assert [SIZE, 0] == [await node.admin_client.dbsize() for node in nodes]

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(6000, 8000)], nodes[1].id)
    ]
    logging.debug("Migrating slots 6000-8000")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    assert [SIZE, SIZE] == [await node.client.dbsize() for node in nodes]

    logging.debug("Reapply config with migration")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await asyncio.sleep(0.1)
    assert [SIZE, SIZE] == [await node.client.dbsize() for node in nodes]

    logging.debug("Finalizing migration")
    nodes[0].migrations = []
    nodes[0].slots = [(0, 6000)]
    nodes[1].slots = [(6001, 16383)]
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    logging.debug("Migration finalized")

    await asyncio.sleep(1)
    assert [0, SIZE] == [await node.client.dbsize() for node in nodes]

    for i in range(SIZE):
        assert str(i) == await nodes[1].client.get(f"{{key50}}:{i}")


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_replication_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    """
    Test replication with migration. Create the following setup:

    master_1 -> replica_1, master_2 -> replica_2

    with each master owning half the slots. Let them then fully exchange their slots
    and make sure the captures on the replicas are equal.
    """
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(4)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(n) for n in instances]
    m1_node, r1_node, m2_node, r2_node = nodes
    master_nodes = [m1_node, m2_node]

    # divide node slots by half
    m1_node.slots = [(0, 8000)]
    m1_node.replicas = [r1_node]
    m2_node.slots = [(8001, 16383)]
    m2_node.replicas = [r2_node]

    logging.debug("Push initial config")
    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    logging.debug("create data")
    seeder = df_seeder_factory.create(keys=2000, port=m1_node.instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    logging.debug("start replication")
    await r1_node.admin_client.execute_command(f"replicaof localhost {m1_node.instance.port}")
    await r2_node.admin_client.execute_command(f"replicaof localhost {m2_node.instance.port}")

    await wait_available_async(r1_node.admin_client)
    await wait_available_async(r2_node.admin_client)

    r1_capture = await seeder.capture(r1_node.instance.port)
    r2_capture = await seeder.capture(r2_node.instance.port)

    logging.debug("start migration")
    m1_node.migrations = [
        MigrationInfo("127.0.0.1", m2_node.instance.admin_port, [(0, 8000)], m2_node.id)
    ]
    m2_node.migrations = [
        MigrationInfo("127.0.0.1", m1_node.instance.admin_port, [(8001, 16383)], m1_node.id)
    ]
    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    await wait_for_status(m1_node.admin_client, m2_node.id, "FINISHED")
    await wait_for_status(m2_node.admin_client, m1_node.id, "FINISHED")

    logging.debug("finish migration")
    m1_node.migrations = []
    m1_node.slots = [(8001, 16383)]
    m2_node.migrations = []
    m2_node.slots = [(0, 8000)]

    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    # wait for replicas to catch up
    await asyncio.sleep(2)

    # ensure captures got exchanged
    assert await seeder.compare(r2_capture, r1_node.instance.port)
    assert await seeder.compare(r1_capture, r2_node.instance.port)


@pytest.mark.skip("Flaky test")
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_start_replication_during_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    """
    Test replication with migration. Create the following setup:

    master_1 do migration to master_2 and we start replication for master_1 during this migration

    in the end master_1 and replica_1 should have the same data
    """
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(3)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(n) for n in instances]
    m1_node, r1_node, m2_node = nodes
    master_nodes = [m1_node, m2_node]

    m1_node.slots = [(0, 16383)]
    m1_node.replicas = [r1_node]
    m2_node.slots = []

    logging.debug("Push initial config")
    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    logging.debug("create data")
    seeder = df_seeder_factory.create(keys=10000, port=nodes[0].instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    logging.debug("start migration")
    m1_node.migrations = [
        MigrationInfo("127.0.0.1", m2_node.instance.admin_port, [(2001, 16383)], m2_node.id)
    ]
    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    logging.debug("start replication")
    await r1_node.admin_client.execute_command(f"replicaof localhost {m1_node.instance.port}")

    await wait_available_async(r1_node.admin_client)

    await wait_for_status(m1_node.admin_client, m2_node.id, "FINISHED")

    logging.debug("finish migration")
    m1_node.migrations = []
    m1_node.slots = [(0, 2000)]
    m2_node.migrations = []
    m2_node.slots = [(2001, 16383)]

    await push_config(
        json.dumps(generate_config(master_nodes)), [node.admin_client for node in nodes]
    )

    await check_all_replicas_finished([r1_node.client], m1_node.client)

    m1_capture = await seeder.capture(m1_node.instance.port)

    assert await seeder.compare(m1_capture, r1_node.instance.port)


@pytest.mark.parametrize("migration_first", [False, True])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "dbfilename": "snap_during_migration"})
async def test_snapshoting_during_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory, migration_first: bool
):
    """
    Test saving snapshot during migration. Create the following setups:

    1) Start saving and then run migration simultaneously
    2) Run migration and start saving simultaneously

    The result should be the same: snapshot contains all the data that existed before migration
    """
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(n) for n in instances]

    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    logging.debug("Push initial config")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("create data")
    seeder = df_seeder_factory.create(keys=10000, port=nodes[0].instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    capture_before_migration = await seeder.capture(nodes[0].instance.port)

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    ]

    async def start_migration():
        logging.debug("start migration")
        await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    async def start_save():
        logging.debug("BGSAVE")
        await nodes[0].client.execute_command(f"BGSAVE")

    if migration_first:
        await start_migration()
        await asyncio.sleep(random.randint(0, 10) / 100)
        await start_save()
    else:
        await start_save()
        await asyncio.sleep(random.randint(0, 10) / 100)
        await start_migration()

    logging.debug("wait for snapshot")
    while await is_saving(nodes[0].client):
        await asyncio.sleep(0.1)

    logging.debug("wait migration finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    logging.debug("finish migration")
    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[0].migrations = []
    nodes[0].slots = [(0, 16383)]

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    assert await seeder.compare(capture_before_migration, nodes[1].instance.port)

    await nodes[1].client.execute_command(
        "DFLY",
        "LOAD",
        "snap_during_migration-summary.dfs",
    )

    assert await seeder.compare(capture_before_migration, nodes[1].instance.port)


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
@pytest.mark.asyncio
async def test_cluster_migration_cancel(df_factory: DflyInstanceFactory):
    """Check data migration from one node to another."""
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 8000)]
    nodes[1].slots = [(8001, 16383)]

    logging.debug("Pushing data to slot 6XXX")
    SIZE = 10_000
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    for i in range(SIZE):
        assert await nodes[0].client.set(f"{{key50}}:{i}", i)  # key50 belongs to slot 6686
    assert [SIZE, 0] == [await node.client.dbsize() for node in nodes]

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(6000, 8000)], nodes[1].id)
    ]
    logging.debug("Migrating slots 6000-8000")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Cancelling migration")
    nodes[0].migrations = []
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    assert SIZE == await nodes[0].client.dbsize()

    @assert_eventually
    async def node1size0():
        if await nodes[1].client.dbsize() != 0:
            logging.debug(await nodes[1].client.execute_command("keys *"))
            assert False

    await node1size0()

    logging.debug("Reissuing migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(6001, 8000)], nodes[1].id)
    )
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")
    assert [SIZE, SIZE] == [await node.client.dbsize() for node in nodes]

    logging.debug("Finalizing migration")
    nodes[0].migrations = []
    nodes[0].slots = [(0, 6000)]
    nodes[1].slots = [(6001, 16383)]
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])
    logging.debug("Migration finalized")

    while 0 != await nodes[0].client.dbsize():
        logging.debug(f"wait until source dbsize is empty")
        await asyncio.sleep(0.1)

    for i in range(SIZE):
        assert str(i) == await nodes[1].client.get(f"{{key50}}:{i}")


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
@pytest.mark.asyncio
@pytest.mark.opt_only
@pytest.mark.exclude_epoll
async def test_cluster_migration_huge_container(df_factory: DflyInstanceFactory):
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for i in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Generating huge containers")
    seeder = StaticSeeder(
        key_target=100,
        data_size=10_000_000,
        collection_size=10_000,
        variance=1,
        samples=1,
        types=["LIST", "HASH", "SET", "ZSET", "STRING"],
    )
    await seeder.run(nodes[0].client)
    source_data = await StaticSeeder.capture(nodes[0].client)

    mem_before = await get_memory(nodes[0].client, "used_memory_rss")

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(0, 16383)], nodes[1].id)
    ]
    logging.debug("Migrating slots")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Waiting for migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=300)

    target_data = await StaticSeeder.capture(nodes[1].client)
    assert source_data == target_data

    # Get peak memory, because migration removes the data
    mem_after = await get_memory(nodes[0].client, "used_memory_peak_rss")
    logging.debug(f"Memory before {mem_before} after {mem_after}")
    assert mem_after < mem_before * 1.1

    line = stop_and_get_restore_log(nodes[0].instance)

    # 'with X commands' - how many breakdowns we used for the keys
    assert extract_int_after_prefix("with ", line) > 500_000

    assert extract_int_after_prefix("Keys skipped ", line) == 0
    assert extract_int_after_prefix("buckets skipped ", line) == 0
    assert extract_int_after_prefix("keys written ", line) > 90

    # We don't send updates during the migration
    assert extract_int_after_prefix("buckets on_db_update ", line) == 0


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
@pytest.mark.parametrize("chunk_size", [1_000_000, 30])
@pytest.mark.asyncio
async def test_cluster_migration_while_seeding(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory, chunk_size
):
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            serialization_max_chunk_size=chunk_size,
        )
        for _ in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    client0 = nodes[0].client

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Seeding cluster")
    seeder = df_seeder_factory.create(
        keys=10_000, port=instances[0].port, cluster_mode=True, mirror_to_fake_redis=True
    )
    await seeder.run(target_deviation=0.1)

    seed = asyncio.create_task(seeder.run())
    await asyncio.sleep(1)

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(0, 16383)], nodes[1].id)
    ]
    logging.debug("Migrating slots")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Waiting for migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=300)
    logging.debug("Migration finished")

    logging.debug("Finalizing migration")
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await asyncio.sleep(1)  # Let seeder feed dest before migration finishes

    seeder.stop()
    await seed
    logging.debug("Seeding finished")

    assert (
        await get_memory(client0, "used_memory_peak_rss")
        < await get_memory(client0, "used_memory_rss") * 1.1
    )

    capture = await seeder.capture_fake_redis()
    assert await seeder.compare(capture, instances[1].port)

    line = stop_and_get_restore_log(nodes[0].instance)
    assert extract_int_after_prefix("Keys skipped ", line) == 0
    assert extract_int_after_prefix("buckets skipped ", line) > 0
    assert extract_int_after_prefix("keys written ", line) >= 9_000
    assert extract_int_after_prefix("buckets on_db_update ", line) > 0


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
@pytest.mark.asyncio
async def test_cluster_migrations_sequence(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    instances = [
        df_factory.create(port=next(next_port), admin_port=next(next_port)) for _ in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Seeding cluster")
    seeder = df_seeder_factory.create(
        keys=10_000, port=instances[0].port, cluster_mode=True, mirror_to_fake_redis=True
    )
    await seeder.run(target_deviation=0.1)

    seed = asyncio.create_task(seeder.run())
    await asyncio.sleep(1)

    slot_step = 500
    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(0, slot_step - 1)], nodes[1].id)
    ]
    logging.debug("Migrating slots")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    for i in range(slot_step, 16301, slot_step):
        logging.debug("Waiting for migration to finish")
        await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=10)

        nodes[0].slots = [(i, 16383)]
        nodes[1].slots = [(0, i - 1)]
        end_slot = min(i + slot_step - 1, 16383)
        nodes[0].migrations = [
            MigrationInfo("127.0.0.1", instances[1].admin_port, [(i, end_slot)], nodes[1].id)
        ]

        await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Waiting for migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=10)
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("Finalizing migration")
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    nodes[0].migrations = []
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("stop seeding")
    seeder.stop()
    await seed

    capture = await seeder.capture_fake_redis()
    assert await seeder.compare(capture, instances[1].port)


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
    seeder = df_seeder_factory.create(keys=2000, port=cluster_nodes[0].port, cluster_mode=True)
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
async def test_replicate_disconnect_cluster(df_factory: DflyInstanceFactory, df_seeder_factory):
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
    seeder = df_seeder_factory.create(keys=2000, port=cluster_nodes[0].port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    fill_task = asyncio.create_task(seeder.run())

    proxy = Proxy("127.0.0.1", next(next_port), "127.0.0.1", cluster_nodes[0].port)
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

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
    await proxy.close(proxy_task)
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
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

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

    await proxy.close(proxy_task)


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


@pytest.mark.skip("Flaky test")
@dfly_args({"proactor_threads": 4})
async def test_replicate_disconnect_redis_cluster(redis_cluster, df_factory, df_seeder_factory):
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

    proxy = Proxy("127.0.0.1", next(next_port), "127.0.0.1", redis_cluster_nodes[1].port)
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

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
    await proxy.close(proxy_task)
    await asyncio.sleep(3)

    # check second node connection is down
    info = await c_replica.execute_command("INFO REPLICATION")
    statuses = re.findall("master_link_status:(down|up)\r\n", info)
    assert len(statuses) == 3
    assert statuses[0] == "up"
    assert statuses[1] == "down"
    assert statuses[2] == "up"

    # start connection again
    await proxy.start()
    proxy_task = asyncio.create_task(proxy.serve())

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
    await proxy.close(proxy_task)


@pytest.mark.skip("Takes more than 10 minutes")
@dfly_args({"cluster_mode": "yes"})
async def test_cluster_memory_consumption_migration(df_factory: DflyInstanceFactory):
    # Check data migration from one node to another
    instances = [
        df_factory.create(
            maxmemory="15G",
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="streamer=2",
        )
        for i in range(3)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    for i in range(1, len(instances)):
        nodes[i].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await nodes[0].client.execute_command("DEBUG POPULATE 5000000 test 1000 RAND SLOTS 0 16383")

    await asyncio.sleep(2)

    migration_nodes = len(instances) - 1
    slot_step = 16384 // migration_nodes
    ranges = []
    for i in range(0, migration_nodes):
        ranges.append(i * slot_step)
    ranges.append(16384)

    for i in range(1, len(instances)):
        nodes[0].migrations.append(
            MigrationInfo(
                "127.0.0.1",
                nodes[i].instance.admin_port,
                [(ranges[i - 1], ranges[i] - 1)],
                nodes[i].id,
            )
        )

    logging.debug("Start migration")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED", 1000)

    nodes[0].migrations = []
    nodes[0].slots = []
    for i in range(1, len(instances)):
        nodes[i].slots = [(ranges[i - 1], ranges[i] - 1)]
    logging.debug("remove finished migrations")
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await check_for_no_state_status([node.admin_client for node in nodes])


@pytest.mark.exclude_epoll
@pytest.mark.asyncio
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_migration_timeout_on_sync(df_factory: DflyInstanceFactory, df_seeder_factory):
    # Timeout set to 3 seconds because we must first saturate the socket before we get the timeout
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            replication_timeout=3000,
            vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
        )
        for i in range(2)
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("source node DEBUG POPULATE")

    await StaticSeeder(key_target=300000, data_size=1000).run(nodes[0].client)
    start_capture = await StaticSeeder.capture(nodes[0].client)

    logging.debug("Start migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    await asyncio.sleep(random.randint(0, 50) / 100)
    await wait_for_migration_start(nodes[1].admin_client, nodes[0].id)

    logging.debug("debug migration pause")
    await nodes[1].client.execute_command("debug migration pause")

    await wait_for_error(
        nodes[0].admin_client, nodes[1].id, "JournalStreamer write operation timeout", 30
    )

    logging.debug("debug migration resume")
    await nodes[1].client.execute_command("debug migration resume")

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    assert (await StaticSeeder.capture(nodes[1].client)) == start_capture


"""
Test cluster node distributing its slots into 2 other nodes.
In this test we start migrating to the second node only after the first one finished to
reproduce the bug found in issue #4455
"""


@pytest.mark.exclude_epoll
@pytest.mark.asyncio
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_migration_one_after_another(df_factory: DflyInstanceFactory, df_seeder_factory):
    # 1. Create cluster of 3 nodes with all slots allocated to first node.
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
        )
        for i in range(3)
    ]
    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    nodes[2].slots = []
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    logging.debug("DEBUG POPULATE first node")
    key_num = 100000
    await StaticSeeder(key_target=key_num, data_size=100).run(nodes[0].client)
    dbsize_node0 = await nodes[0].client.dbsize()
    assert dbsize_node0 > (key_num * 0.95)

    # 2. Start migrating part of the slots from first node to second
    logging.debug("Start first migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16300)], nodes[1].id)
    )
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # 3. Wait for migratin finish
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=50)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED", timeout=50)

    nodes[0].migrations = []
    nodes[0].slots = [(16301, 16383)]
    nodes[1].slots = [(0, 16300)]
    nodes[2].slots = []
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # 4. Start migrating remaind slots from first node to third node
    logging.debug("Start second migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[2].instance.admin_port, [(16301, 16383)], nodes[2].id)
    )
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # 5. Wait for migratin finish
    await wait_for_status(nodes[0].admin_client, nodes[2].id, "FINISHED", timeout=10)
    await wait_for_status(nodes[2].admin_client, nodes[0].id, "FINISHED", timeout=10)

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16300)]
    nodes[2].slots = [(16301, 16383)]
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # 6. Check all data was migrated
    # Using dbsize to check all the data was migrated to the other nodes.
    # Note: we can not use the seeder capture as we migrate the data to 2 different nodes.
    # TODO: improve the migration conrrectness by running the seeder capture on slot range (requiers changes in capture script).
    dbsize_node1 = await nodes[1].client.dbsize()
    dbsize_node2 = await nodes[2].client.dbsize()
    assert dbsize_node1 + dbsize_node2 == dbsize_node0
    assert dbsize_node2 > 0 and dbsize_node1 > 0


"""
Test cluster node distributing its slots into 3 other nodes.
In this test we randomize the slot ranges that are migrated to each node
For each migration we start migration, wait for it to finish and once it is finished we send migration finalization config
"""


@pytest.mark.slow
@pytest.mark.exclude_epoll
@pytest.mark.asyncio
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_migration_rebalance_node(df_factory: DflyInstanceFactory, df_seeder_factory):
    # 1. Create cluster of 3 nodes with all slots allocated to first node.
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
        )
        for i in range(4)
    ]
    df_factory.start_all(instances)

    def create_random_ranges():
        # Generate 2 random breakpoints within the range
        breakpoints = sorted(random.sample(range(1, 16382), 2))
        ranges = [
            (0, breakpoints[0] - 1),
            (breakpoints[0], breakpoints[1] - 1),
            (breakpoints[1], 16383),
        ]
        return ranges

    # Create 3 random ranges from 0 to 16383
    random_ranges = create_random_ranges()

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = random_ranges
    nodes[1].slots = []
    nodes[2].slots = []
    nodes[3].slots = []
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    key_num = 100000
    logging.debug(f"DEBUG POPULATE first node with number of keys: {key_num}")
    await StaticSeeder(key_target=key_num, data_size=100).run(nodes[0].client)
    dbsize_node0 = await nodes[0].client.dbsize()
    assert dbsize_node0 > (key_num * 0.95)

    logging.debug("start seeding")
    # Running seeder with pipeline mode when finalizing migrations leads to errors
    # TODO: I believe that changing the seeder to generate pipeline command only on specific slot will fix the problem
    seeder = df_seeder_factory.create(
        keys=50_000, port=instances[0].port, cluster_mode=True, pipeline=False
    )
    await seeder.run(target_deviation=0.1)
    seed = asyncio.create_task(seeder.run())

    migration_info = [
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [random_ranges[0]], nodes[1].id),
        MigrationInfo("127.0.0.1", nodes[2].instance.admin_port, [random_ranges[1]], nodes[2].id),
        MigrationInfo("127.0.0.1", nodes[3].instance.admin_port, [random_ranges[2]], nodes[3].id),
    ]

    nodes_lock = asyncio.Lock()

    async def do_migration(index):
        await asyncio.sleep(random.randint(1, 10) / 5)
        async with nodes_lock:
            logging.debug(f"Start migration from node {index}")
            nodes[0].migrations.append(migration_info[index - 1])
            await push_config(
                json.dumps(generate_config(nodes)), [node.admin_client for node in nodes]
            )

        logging.debug(f"wait migration from node {index}")
        await wait_for_status(nodes[0].admin_client, nodes[index].id, "FINISHED", timeout=50)
        await wait_for_status(nodes[index].admin_client, nodes[0].id, "FINISHED", timeout=50)
        logging.debug(f"finished migration from node {index}")
        await asyncio.sleep(random.randint(1, 5) / 5)
        async with nodes_lock:
            logging.debug(f"Finalize migration from node {index}")
            nodes[index].slots = migration_info[index - 1].slots
            nodes[0].slots.remove(migration_info[index - 1].slots[0])
            nodes[0].migrations.remove(migration_info[index - 1])
            await push_config(
                json.dumps(generate_config(nodes)), [node.admin_client for node in nodes]
            )

    all_migrations = [asyncio.create_task(do_migration(i)) for i in range(1, 4)]
    for migration in all_migrations:
        await migration

    logging.debug("stop seeding")
    seeder.stop()
    await seed
