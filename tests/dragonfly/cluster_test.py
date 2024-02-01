import pytest
import re
import json
import redis
from redis import asyncio as aioredis
import asyncio

from .instance import DflyInstanceFactory
from .utility import *
from .replication_test import check_all_replicas_finished

from . import dfly_args

BASE_PORT = 30001


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


def verify_slots_result(
    ip: str, port: int, answer: list, rep_ip: str = None, rep_port: int = None
) -> bool:
    def is_local_host(ip: str) -> bool:
        return ip == "127.0.0.1" or ip == "localhost"

    assert answer[0] == 0  # start shard
    assert answer[1] == 16383  # last shard
    if rep_ip is not None:
        assert len(answer) == 4  # the network info
        rep_info = answer[3]
        assert len(rep_info) == 3
        ip_addr = str(rep_info[0], "utf-8")
        assert ip_addr == rep_ip or (is_local_host(ip_addr) and is_local_host(ip))
        assert rep_info[1] == rep_port
    else:
        assert len(answer) == 3
    info = answer[2]
    assert len(info) == 3
    ip_addr = str(info[0], "utf-8")
    assert ip_addr == ip or (is_local_host(ip_addr) and is_local_host(ip))
    assert info[1] == port
    return True


@dfly_args({"proactor_threads": 4, "cluster_mode": "emulated"})
async def test_cluster_slots_in_replicas(df_local_factory):
    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(port=BASE_PORT + 1, logtostdout=True)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

    res = await c_replica.execute_command("CLUSTER SLOTS")
    assert len(res) == 1
    assert verify_slots_result(ip="127.0.0.1", port=replica.port, answer=res[0])
    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(ip="127.0.0.1", port=master.port, answer=res[0])

    # Connect replica to master
    rc = await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    assert str(rc, "utf-8") == "OK"
    await asyncio.sleep(0.5)
    res = await c_replica.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        ip="127.0.0.1", port=master.port, answer=res[0], rep_ip="127.0.0.1", rep_port=replica.port
    )
    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        ip="127.0.0.1", port=master.port, answer=res[0], rep_ip="127.0.0.1", rep_port=replica.port
    )


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


# Tests that master commands to the replica are applied regardless of slot ownership
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_replica_sets_non_owned_keys(df_local_factory):
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


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_native_client(
    df_local_factory: DflyInstanceFactory,
    df_seeder_factory: DflySeederFactory,
):
    # Start and configure cluster with 3 masters and 3 replicas
    masters = [
        df_local_factory.create(port=BASE_PORT + i, admin_port=BASE_PORT + i + 1000)
        for i in range(3)
    ]
    df_local_factory.start_all(masters)
    c_masters = [aioredis.Redis(port=master.port) for master in masters]
    c_masters_admin = [master.admin_client() for master in masters]
    master_ids = await asyncio.gather(*(get_node_id(c) for c in c_masters_admin))

    replicas = [
        df_local_factory.create(port=BASE_PORT + 100 + i, admin_port=BASE_PORT + i + 1100)
        for i in range(3)
    ]
    df_local_factory.start_all(replicas)
    c_replicas = [replica.client() for replica in replicas]
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
    replica_response = await client.execute_command(
        "get", "key0", target_nodes=aioredis.RedisCluster.REPLICAS
    )
    assert "value" in replica_response.values()

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
    await client.close()


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

    res = await c_nodes_admin[1].execute_command(
        "DFLYCLUSTER", "START-SLOT-MIGRATION", "127.0.0.1", str(nodes[0].admin_port), "5200", "5259"
    )
    assert 1 == res

    while (
        await c_nodes_admin[1].execute_command(
            "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", "127.0.0.1", str(nodes[0].admin_port)
        )
        != "STABLE_SYNC"
    ):
        await asyncio.sleep(0.05)

    status = await c_nodes_admin[0].execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", "127.0.0.1", str(nodes[1].port)
    )
    assert "STABLE_SYNC" == status

    status = await c_nodes_admin[0].execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
    assert ["out 127.0.0.1:30002 STABLE_SYNC"] == status

    try:
        await c_nodes_admin[1].execute_command(
            "DFLYCLUSTER",
            "START-SLOT-MIGRATION",
            "127.0.0.1",
            str(nodes[0].admin_port),
            "5000",
            "5200",
        )
        assert False, "Should not be able to start slot migration"
    except redis.exceptions.ResponseError as e:
        assert e.args[0] == "Can't start the migration, another one is in progress"

    await push_config(
        config.replace("LAST_SLOT_CUTOFF", "5259").replace("NEXT_SLOT_CUTOFF", "5260"),
        c_nodes_admin,
    )

    await c_nodes_admin[0].close()
    await c_nodes_admin[1].close()


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

    assert await c_nodes[1].set("KEY2", "value")
    assert await c_nodes[1].set("KEY3", "value")

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

    res = await c_nodes_admin[1].execute_command(
        "DFLYCLUSTER", "START-SLOT-MIGRATION", "127.0.0.1", str(nodes[0].admin_port), "3000", "9000"
    )
    assert 1 == res

    assert await c_nodes[0].set("KEY0", "value")
    assert await c_nodes[0].set("KEY1", "value")

    while (
        await c_nodes_admin[1].execute_command(
            "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", "127.0.0.1", str(nodes[0].admin_port)
        )
        != "STABLE_SYNC"
    ):
        await asyncio.sleep(0.05)

    assert await c_nodes[0].set("KEY4", "value")
    assert await c_nodes[0].set("KEY5", "value")
    assert await c_nodes[0].execute_command("DBSIZE") == 10

    # TODO remove when we add slot blocking
    await asyncio.sleep(0.5)

    res = await c_nodes_admin[0].execute_command("DFLYCLUSTER", "SLOT-MIGRATION-FINALIZE", "1")
    assert "OK" == res

    await asyncio.sleep(0.5)

    while (
        await c_nodes_admin[1].execute_command(
            "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", "127.0.0.1", str(nodes[0].admin_port)
        )
        != "FINISHED"
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

    await c_nodes_admin[0].close()
    await c_nodes_admin[1].close()
