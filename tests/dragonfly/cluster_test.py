import pytest
import re
import json
import redis
from redis import asyncio as aioredis
import asyncio
from dataclasses import dataclass

from .instance import DflyInstanceFactory, DflyInstance
from .utility import *
from .replication_test import check_all_replicas_finished
from redis.cluster import RedisCluster
from redis.cluster import ClusterNode
from .proxy import Proxy

from . import dfly_args

BASE_PORT = 30001


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
    for node in nodes:
        node.start()
        time.sleep(1)

    create_command = f'echo "yes" |redis-cli --cluster create {" ".join([f"127.0.0.1:{port}" for port in ports])}'
    subprocess.run(create_command, shell=True)
    time.sleep(4)
    yield nodes
    for node in nodes:
        node.stop()


async def push_config(config, admin_connections):
    logging.debug("Pushing config %s", config)
    res = await asyncio.gather(
        *(c_admin.execute_command("DFLYCLUSTER", "CONFIG", config) for c_admin in admin_connections)
    )
    assert all([r == "OK" for r in res])


async def get_node_id(admin_connection):
    id = await admin_connection.execute_command("DFLYCLUSTER MYID")
    assert isinstance(id, str)
    return id


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
    ip_addr = str(info[0], "utf-8")
    assert is_local_host(ip_addr)
    assert info[1] == port

    # Replicas
    assert len(answer) == 3 + len(replicas)
    for i in range(3, len(replicas)):
        replica = replicas[i - 3]
        rep_info = answer[i]
        assert len(rep_info) == 3
        ip_addr = str(rep_info[0], "utf-8")
        assert is_local_host(ip_addr)
        assert rep_info[1] == replica.port
        assert rep_info[2] == replica.id

    return True


@dfly_args({"proactor_threads": 4, "cluster_mode": "emulated"})
async def test_emulated_cluster_with_replicas(df_local_factory):
    master = df_local_factory.create(port=BASE_PORT)
    replicas = [df_local_factory.create(port=BASE_PORT + i, logtostdout=True) for i in range(1, 3)]

    df_local_factory.start_all([master, *replicas])

    c_master = aioredis.Redis(port=master.port)
    master_id = (await c_master.execute_command("dflycluster myid")).decode("utf-8")

    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]
    replica_ids = [
        (await c_replica.execute_command("dflycluster myid")).decode("utf-8")
        for c_replica in c_replicas
    ]

    for replica, c_replica in zip(replicas, c_replicas):
        res = await c_replica.execute_command("CLUSTER SLOTS")
        assert len(res) == 1
        assert verify_slots_result(port=replica.port, answer=res[0], replicas=[])

    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(port=master.port, answer=res[0], replicas=[])

    # Connect replicas to master
    for replica, c_replica in zip(replicas, c_replicas):
        rc = await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        assert str(rc, "utf-8") == "OK"

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
        replicas=[ReplicaInfo(id, replica.port) for id, replica in zip(replica_ids, replicas)],
    )

    assert await c_master.execute_command("CLUSTER NODES") == {
        f"127.0.0.1:{master.port}": {
            "connected": True,
            "epoch": "0",
            "flags": "myself,master",
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
            "last_ping_sent": "0",
            "last_pong_rcvd": "0",
            "master_id": master_id,
            "migrations": [],
            "node_id": replica_ids[1],
            "slots": [],
        },
    }

    await close_clients(c_master, *c_replicas)


@dfly_args({"cluster_mode": "emulated", "cluster_announce_ip": "127.0.0.2"})
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
async def test_cluster_node_id(df_local_factory: DflyInstanceFactory):
    node = df_local_factory.create(port=BASE_PORT)
    df_local_factory.start_all([node])

    conn = node.client()
    assert "inigo montoya" == await get_node_id(conn)

    await close_clients(conn)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_slot_ownership_changes(df_local_factory: DflyInstanceFactory):
    # Start and configure cluster with 2 nodes
    nodes = [
        df_local_factory.create(port=BASE_PORT + i, admin_port=BASE_PORT + i + 1000)
        for i in range(2)
    ]

    df_local_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]
    c_nodes_admin = [node.admin_client() for node in nodes]

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes_admin))

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
        assert e.args[0] == "MOVED 5259 localhost:30001"

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
        assert e.args[0] == "MOVED 5259 localhost:30002"

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

    await close_clients(*c_nodes, *c_nodes_admin)


# Tests that master commands to the replica are applied regardless of slot ownership
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_replica_sets_non_owned_keys(df_local_factory: DflyInstanceFactory):
    # Start and configure cluster with 1 master and 1 replica, both own all slots
    master = df_local_factory.create(admin_port=BASE_PORT + 1000)
    replica = df_local_factory.create(admin_port=BASE_PORT + 1001)
    df_local_factory.start_all([master, replica])

    async with master.client() as c_master, master.admin_client() as c_master_admin, replica.client() as c_replica, replica.admin_client() as c_replica_admin:
        master_id = await get_node_id(c_master_admin)
        replica_id = await get_node_id(c_replica_admin)

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

        await close_clients(c_master, c_master_admin, c_replica, c_replica_admin)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_flush_slots_after_config_change(df_local_factory: DflyInstanceFactory):
    # Start and configure cluster with 1 master and 1 replica, both own all slots
    master = df_local_factory.create(port=BASE_PORT, admin_port=BASE_PORT + 1000)
    replica = df_local_factory.create(port=BASE_PORT + 1, admin_port=BASE_PORT + 1001)
    df_local_factory.start_all([master, replica])

    c_master = master.client()
    c_master_admin = master.admin_client()
    master_id = await get_node_id(c_master_admin)

    c_replica = replica.client()
    c_replica_admin = replica.admin_client()
    replica_id = await get_node_id(c_replica_admin)

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

    await close_clients(c_master, c_master_admin, c_replica, c_replica_admin)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "admin_port": 30001})
async def test_cluster_blocking_command(df_server):
    c_master = df_server.client()
    c_master_admin = df_server.admin_client()

    config = [
        {
            "slot_ranges": [{"start": 0, "end": 8000}],
            "master": {"id": await get_node_id(c_master_admin), "ip": "10.0.0.1", "port": 7000},
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

    await close_clients(c_master, c_master_admin)


@pytest.mark.parametrize("set_cluster_node_id", [True, False])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_native_client(
    df_local_factory: DflyInstanceFactory,
    df_seeder_factory: DflySeederFactory,
    set_cluster_node_id: bool,
):
    # Start and configure cluster with 3 masters and 3 replicas
    masters = [
        df_local_factory.create(
            port=BASE_PORT + i,
            admin_port=BASE_PORT + i + 1000,
            cluster_node_id=f"master{i}" if set_cluster_node_id else "",
        )
        for i in range(3)
    ]
    df_local_factory.start_all(masters)
    c_masters = [aioredis.Redis(port=master.port) for master in masters]
    c_masters_admin = [master.admin_client() for master in masters]
    master_ids = await asyncio.gather(*(get_node_id(c) for c in c_masters_admin))

    replicas = [
        df_local_factory.create(
            port=BASE_PORT + 100 + i,
            admin_port=BASE_PORT + i + 1100,
            cluster_node_id=f"replica{i}" if set_cluster_node_id else "",
            replicaof=f"localhost:{BASE_PORT + i}",
        )
        for i in range(3)
    ]
    df_local_factory.start_all(replicas)
    c_replicas = [replica.client() for replica in replicas]
    await asyncio.gather(*(wait_available_async(c) for c in c_replicas))
    c_replicas_admin = [replica.admin_client() for replica in replicas]
    replica_ids = await asyncio.gather(*(get_node_id(c) for c in c_replicas_admin))

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

    client = aioredis.RedisCluster(decode_responses=True, host="localhost", port=masters[0].port)

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
    await close_clients(client, *c_masters, *c_masters_admin, *c_replicas, *c_replicas_admin)


@pytest.mark.skip(reason="Test needs refactoring because of cluster design change")
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_slot_migration(df_local_factory: DflyInstanceFactory):
    # Check slot migration from one node to another
    nodes = [
        df_local_factory.create(port=BASE_PORT + i, admin_port=BASE_PORT + i + 1000)
        for i in range(2)
    ]

    df_local_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]
    c_nodes_admin = [node.admin_client() for node in nodes]

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes_admin))

    config = f"""
      [
        {{
          "slot_ranges": [ {{ "start": 0, "end": LAST_SLOT_CUTOFF }} ],
          "master": {{ "id": "{node_ids[0]}", "ip": "localhost", "port": {nodes[0].port} }},
          "replicas": []
        }},
        {{
          "slot_ranges": [ {{ "start": NEXT_SLOT_CUTOFF, "end": 16383 }} ],
          "master": {{ "id": "{node_ids[1]}", "ip": "localhost", "port": {nodes[1].port} }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5259").replace("NEXT_SLOT_CUTOFF", "5260"),
        c_nodes_admin,
    )

    status = await c_nodes_admin[1].execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", "127.0.0.1", str(nodes[0].admin_port)
    )
    assert "NO_STATE" == status

    migation_config = f"""
      [
        {{
          "slot_ranges": [ {{ "start": 0, "end": LAST_SLOT_CUTOFF }} ],
          "master": {{ "id": "{node_ids[0]}", "ip": "localhost", "port": {nodes[0].port} }},
          "replicas": [],
          "migrations": [{{ "slot_ranges": [ {{ "start": 5200, "end": 5259 }} ]
                         , "ip": "127.0.0.1", "port" : {nodes[1].admin_port}, "node_id": "{node_ids[1]}" }}]
        }},
        {{
          "slot_ranges": [ {{ "start": NEXT_SLOT_CUTOFF, "end": 16383 }} ],
          "master": {{ "id": "{node_ids[1]}", "ip": "localhost", "port": {nodes[1].port} }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        migation_config.replace("LAST_SLOT_CUTOFF", "5259").replace("NEXT_SLOT_CUTOFF", "5260"),
        c_nodes_admin,
    )

    # TODO add a check for correct results after the same config apply

    await close_clients(*c_nodes, *c_nodes_admin)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_data_migration(df_local_factory: DflyInstanceFactory):
    # Check data migration from one node to another
    nodes = [
        df_local_factory.create(port=BASE_PORT + i, admin_port=BASE_PORT + i + 1000)
        for i in range(2)
    ]

    df_local_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]
    c_nodes_admin = [node.admin_client() for node in nodes]

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes_admin))

    config = f"""
      [
        {{
          "slot_ranges": [ {{ "start": 0, "end": LAST_SLOT_CUTOFF }} ],
          "master": {{ "id": "{node_ids[0]}", "ip": "localhost", "port": {nodes[0].port} }},
          "replicas": []
        }},
        {{
          "slot_ranges": [ {{ "start": NEXT_SLOT_CUTOFF, "end": 16383 }} ],
          "master": {{ "id": "{node_ids[1]}", "ip": "localhost", "port": {nodes[1].port} }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "9000").replace("NEXT_SLOT_CUTOFF", "9001"),
        c_nodes_admin,
    )

    assert await c_nodes[0].set("KEY0", "value")
    assert await c_nodes[0].set("KEY1", "value")
    assert await c_nodes[1].set("KEY2", "value")
    assert await c_nodes[1].set("KEY3", "value")
    assert await c_nodes[0].set("KEY4", "value")
    assert await c_nodes[0].set("KEY5", "value")
    assert await c_nodes[1].set("KEY6", "value")
    assert await c_nodes[1].set("KEY7", "value")
    assert await c_nodes[0].set("KEY8", "value")
    assert await c_nodes[0].set("KEY9", "value")
    assert await c_nodes[1].set("KEY10", "value")
    assert await c_nodes[1].set("KEY11", "value")
    assert await c_nodes[0].set("KEY12", "value")
    assert await c_nodes[0].set("KEY13", "value")
    assert await c_nodes[1].set("KEY14", "value")
    assert await c_nodes[1].set("KEY15", "value")
    assert await c_nodes[0].set("KEY16", "value")
    assert await c_nodes[0].set("KEY17", "value")
    assert await c_nodes[1].set("KEY18", "value")
    assert await c_nodes[1].set("KEY19", "value")

    assert await c_nodes[0].execute_command("DBSIZE") == 10

    migation_config = f"""
      [
        {{
          "slot_ranges": [ {{ "start": 0, "end": LAST_SLOT_CUTOFF }} ],
          "master": {{ "id": "{node_ids[0]}", "ip": "localhost", "port": {nodes[0].port} }},
          "replicas": [],
          "migrations": [{{ "slot_ranges": [ {{ "start": 3000, "end": 9000 }} ]
                         , "ip": "127.0.0.1", "port" : {nodes[1].admin_port}, "node_id": "{node_ids[1]}" }}]
        }},
        {{
          "slot_ranges": [ {{ "start": NEXT_SLOT_CUTOFF, "end": 16383 }} ],
          "master": {{ "id": "{node_ids[1]}", "ip": "localhost", "port": {nodes[1].port} }},
          "replicas": []
        }}
      ]
    """

    await push_config(
        migation_config.replace("LAST_SLOT_CUTOFF", "9000").replace("NEXT_SLOT_CUTOFF", "9001"),
        c_nodes_admin,
    )

    await asyncio.sleep(0.5)

    while "FINISHED" not in await c_nodes_admin[1].execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", node_ids[0]
    ):
        await asyncio.sleep(0.05)

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "2999").replace("NEXT_SLOT_CUTOFF", "3000"),
        c_nodes_admin,
    )

    assert await c_nodes[0].get("KEY0") == "value"
    assert await c_nodes[1].get("KEY1") == "value"
    assert await c_nodes[1].get("KEY2") == "value"
    assert await c_nodes[1].get("KEY3") == "value"
    assert await c_nodes[0].get("KEY4") == "value"
    assert await c_nodes[1].get("KEY5") == "value"
    assert await c_nodes[1].get("KEY6") == "value"
    assert await c_nodes[1].get("KEY7") == "value"
    assert await c_nodes[0].get("KEY8") == "value"
    assert await c_nodes[1].get("KEY9") == "value"
    assert await c_nodes[1].get("KEY10") == "value"
    assert await c_nodes[1].get("KEY11") == "value"
    assert await c_nodes[1].get("KEY12") == "value"
    assert await c_nodes[1].get("KEY13") == "value"
    assert await c_nodes[1].get("KEY14") == "value"
    assert await c_nodes[1].get("KEY15") == "value"
    assert await c_nodes[1].get("KEY16") == "value"
    assert await c_nodes[1].get("KEY17") == "value"
    assert await c_nodes[1].get("KEY18") == "value"
    assert await c_nodes[1].get("KEY19") == "value"
    assert await c_nodes[1].execute_command("DBSIZE") == 17

    assert (
        await c_nodes_admin[1].execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS") == "NO_STATE"
    )
    assert (
        await c_nodes_admin[0].execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS") == "NO_STATE"
    )

    await close_clients(*c_nodes, *c_nodes_admin)


@dataclass
class MigrationInfo:
    ip: str
    port: int
    slots: list
    node_id: str


@dataclass
class NodeInfo:
    instance: DflyInstance
    client: aioredis.Redis
    admin_client: aioredis.Redis
    slots: list
    next_slots: list
    migrations: list
    id: str


def generate_config(nodes):
    return [
        {
            "slot_ranges": [{"start": s, "end": e} for (s, e) in node.slots],
            "master": {
                "id": node.id,
                "ip": "127.0.0.1",
                "port": node.instance.port,
            },
            "replicas": [],
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


@pytest.mark.skip(reason="Failing on github regression action")
@pytest.mark.parametrize(
    "node_count, segments, keys",
    [
        pytest.param(3, 16, 20_000),
        pytest.param(5, 20, 30_000, marks=[pytest.mark.slow, pytest.mark.opt_only]),
    ],
)
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_fuzzymigration(
    df_local_factory: DflyInstanceFactory,
    df_seeder_factory,
    node_count: int,
    segments: int,
    keys: int,
):
    instances = [
        df_local_factory.create(
            port=BASE_PORT + i,
            admin_port=BASE_PORT + i + 1000,
            vmodule="cluster_family=9,cluster_slot_migration=9",
        )
        for i in range(node_count)
    ]
    df_local_factory.start_all(instances)

    nodes = [
        NodeInfo(
            instance=instance,
            client=instance.client(),
            admin_client=instance.admin_client(),
            slots=[],
            next_slots=[],
            migrations=[],
            id=await get_node_id(instance.admin_client()),
        )
        for instance in instances
    ]

    # Generate equally sized ranges and distribute by nodes
    step = 16400 // segments
    for slot_range in [(s, min(s + step - 1, 16383)) for s in range(0, 16383, step)]:
        nodes[random.randint(0, node_count - 1)].slots.append(slot_range)

    # Push config to all nodes
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # Fill instances with some data
    seeder = df_seeder_factory.create(keys=keys, port=nodes[0].instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    fill_task = asyncio.create_task(seeder.run())

    # some  time fo seeder
    await asyncio.sleep(0.5)

    # Counter that pushes values to a list
    async def list_counter(key, client: aioredis.RedisCluster):
        for i in itertools.count(start=1):
            await client.lpush(key, i)

    # Start ten counters
    counter_keys = [f"_counter{i}" for i in range(10)]
    counter_connections = [
        aioredis.RedisCluster(host="localhost", port=nodes[0].instance.port) for _ in range(10)
    ]
    counters = [
        asyncio.create_task(list_counter(key, conn))
        for key, conn in zip(counter_keys, counter_connections)
    ]

    seeder.stop()
    await fill_task

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

            nodes[dest_idx].next_slots.extend(dest_slots)

        keeping = node.slots[num_outgoing:]
        node.next_slots.extend(keeping)

    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    iterations = 0
    while True:
        for node in nodes:
            states = await node.admin_client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
            print(states)
            if not all("FINISHED" in s for s in states) and not states == "NO_STATE":
                break
        else:
            break

        iterations += 1
        assert iterations < 100

        await asyncio.sleep(0.1)

    # Stop counters
    for counter in counters:
        counter.cancel()

    # clean migrations
    for node in nodes:
        node.migrations = []

    # TODO this config should be pushed with new slots
    # Push new config
    await push_config(json.dumps(generate_config(nodes)), [node.admin_client for node in nodes])

    # Transfer nodes
    for node in nodes:
        node.slots = node.next_slots
        node.new_slots = []

    # Check counter consistency
    cluster_client = aioredis.RedisCluster(host="localhost", port=nodes[0].instance.port)
    for key in counter_keys:
        counter_list = await cluster_client.lrange(key, 0, -1)
        for i, j in zip(counter_list, counter_list[1:]):
            assert int(i) == int(j) + 1, f"Found inconsistent list in {key}: {counter_list}"

    # Compare capture
    assert await seeder.compare(capture, nodes[0].instance.port)

    await asyncio.gather(*[c.close() for c in counter_connections])
    await close_clients(
        cluster_client, *[node.admin_client for node in nodes], *[node.client for node in nodes]
    )


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_migration_cancel(df_local_factory: DflyInstanceFactory):
    # Check data migration from one node to another
    instances = [
        df_local_factory.create(port=BASE_PORT + i, admin_port=BASE_PORT + i + 1000)
        for i in range(2)
    ]
    df_local_factory.start_all(instances)

    nodes = [
        NodeInfo(
            instance=instance,
            client=instance.client(),
            admin_client=instance.admin_client(),
            slots=[],
            next_slots=[],
            migrations=[],
            id=await get_node_id(instance.admin_client()),
        )
        for instance in instances
    ]
    nodes[0].slots = [(0, 8000)]
    nodes[1].slots = [(8001, 16383)]

    SIZE = 10_000
    await push_config(json.dumps(generate_config(nodes)), [node.client for node in nodes])
    for i in range(SIZE):
        assert await nodes[0].client.set(f"{{key50}}:{i}", i)  # key50 belongs to slot 6686
    assert [SIZE, 0] == [await node.client.dbsize() for node in nodes]

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].port, [(6000, 8000)], nodes[1].id)
    ]
    await push_config(json.dumps(generate_config(nodes)), [node.client for node in nodes])
    await asyncio.sleep(0.1)
    assert await nodes[0].client.dbsize() == SIZE
    assert await nodes[1].client.dbsize(), "weak test case" < SIZE

    logging.debug("Cancelling migration")
    nodes[0].migrations = []
    await push_config(json.dumps(generate_config(nodes)), [node.client for node in nodes])
    while True:
        db_size = await nodes[1].client.dbsize()
        if db_size < 20:
            break
        # logging.debug("db sizes ", ', '.join(map(str, db_sizes)))
        logging.debug(f"db sizes {db_size}")
        await asyncio.sleep(0.1)

    logging.debug("Reissuing migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].port, [(6001, 8000)], nodes[1].id)
    )
    await push_config(json.dumps(generate_config(nodes)), [node.client for node in nodes])
    await asyncio.sleep(0.1)
    assert SIZE == await nodes[0].client.dbsize()
    for i in range(100):
        if SIZE == await nodes[1].client.dbsize():
            break
        await asyncio.sleep(0.1)
    else:
        assert False, "Target node not synced"

    logging.debug("Finalizing migration")
    nodes[0].migrations = []
    nodes[0].slots = [(0, 6000)]
    nodes[1].slots = [(6001, 16383)]
    await push_config(json.dumps(generate_config(nodes)), [node.client for node in nodes])
    await asyncio.sleep(0.1)
    assert [0, SIZE] == [await node.client.dbsize() for node in nodes]
    for i in range(SIZE):
        assert str(i) == await nodes[1].client.get(f"{{key50}}:{i}")

    await close_clients(*[node.client for node in nodes], *[node.admin_client for node in nodes])


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
async def test_replicate_cluster(df_local_factory: DflyInstanceFactory, df_seeder_factory):
    """
    Create dragonfly cluster of 2 nodes.
    Create additional dragonfly server in emulated mode.
    Replicate the dragonfly cluster into a single dragonfly node.
    Send traffic before replication start and while replicating.
    Promote the replica to master and check data consistency between cluster and single node.
    """
    replica = df_local_factory.create(admin_port=BASE_PORT, cluster_mode="emulated")
    cluster_nodes = [
        df_local_factory.create(admin_port=BASE_PORT + i + 1, cluster_mode="yes") for i in range(2)
    ]

    # Start instances and connect clients
    df_local_factory.start_all(cluster_nodes + [replica])
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

    await disconnect_clients(*c_nodes, c_replica)


async def await_stable_sync(m_client: aioredis.Redis, replica_port, timeout=10):
    start = time.time()

    async def is_stable():
        role = await m_client.execute_command("role")
        return role == [
            "master",
            [["127.0.0.1", str(replica_port), "stable_sync"]],
        ]

    while (time.time() - start) < timeout:
        if await is_stable():
            return
        await asyncio.sleep(0.05)

    raise RuntimeError("Failed to reach stable sync")


@dfly_args({"proactor_threads": 4})
async def test_replicate_disconnect_cluster(
    df_local_factory: DflyInstanceFactory, df_seeder_factory
):
    """
    Create dragonfly cluster of 2 nodes and additional dragonfly server in emulated mode.
    Populate the cluster with data
    Replicate the dragonfly cluster into a single dragonfly node and wait for stable sync
    Break connection between cluster node 0 and replica and reconnect
    Promote replica to master
    Compare cluster data and replica data
    """
    replica = df_local_factory.create(admin_port=BASE_PORT, cluster_mode="emulated")
    cluster_nodes = [
        df_local_factory.create(admin_port=BASE_PORT + i + 1, cluster_mode="yes") for i in range(2)
    ]

    # Start instances and connect clients
    df_local_factory.start_all(cluster_nodes + [replica])
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

    proxy = Proxy("127.0.0.1", 1114, "127.0.0.1", cluster_nodes[0].port)
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

    await disconnect_clients(*c_nodes, c_replica)
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
async def test_replicate_redis_cluster(redis_cluster, df_local_factory, df_seeder_factory):
    """
    Create redis cluster of 3 nodes.
    Create dragonfly server in emulated mode.
    Replicate the redis cluster into a single dragonfly node.
    Send traffic before replication start and while replicating.
    Promote the replica to master and check data consistency between cluster and single dragonfly node.
    """
    replica = df_local_factory.create(admin_port=BASE_PORT, cluster_mode="emulated")

    # Start instances and connect clients
    df_local_factory.start_all([replica])

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

    await disconnect_clients(c_replica, *node_clients)


@dfly_args({"proactor_threads": 4})
async def test_replicate_disconnect_redis_cluster(
    redis_cluster, df_local_factory, df_seeder_factory
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
    replica = df_local_factory.create(admin_port=BASE_PORT, cluster_mode="emulated")

    # Start instances and connect clients
    df_local_factory.start_all([replica])

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

    proxy = Proxy("127.0.0.1", 1114, "127.0.0.1", redis_cluster_nodes[1].port)
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

    await disconnect_clients(c_replica, *node_clients)
