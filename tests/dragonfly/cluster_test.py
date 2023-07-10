import pytest
import re
import redis
from redis import asyncio as aioredis
import asyncio

from .utility import *
from .replication_test import check_all_replicas_finished
from . import dfly_args

BASE_PORT = 30001


async def push_config(config, admin_connections):
    print("Pushing config ", config)
    await asyncio.gather(*(c_admin.execute_command(
        "DFLYCLUSTER", "CONFIG", config)
        for c_admin in admin_connections))


async def get_node_id(admin_connection):
    id = await admin_connection.execute_command("DFLYCLUSTER MYID")
    return id.decode()


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
    def test_cluster_slots_command(self, cluster_client: redis.RedisCluster):
        expected = {(0, 16383): {'primary': (
            '127.0.0.1', 6379), 'replicas': []}}
        res = cluster_client.execute_command("CLUSTER SLOTS")
        assert expected == res

    def test_cluster_help_command(self, cluster_client: redis.RedisCluster):
        # `target_nodes` is necessary because CLUSTER HELP is not mapped on redis-py
        res = cluster_client.execute_command(
            "CLUSTER HELP", target_nodes=redis.RedisCluster.RANDOM)
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
    def test_cluster_slots_command(self, cluster_client: redis.RedisCluster):
        expected = {(0, 16383): {'primary': (
            '127.0.0.2', 6379), 'replicas': []}}
        res = cluster_client.execute_command("CLUSTER SLOTS")
        assert expected == res


def verify_slots_result(ip: str, port: int, answer: list, rep_ip: str = None, rep_port: int = None) -> bool:
    def is_local_host(ip: str) -> bool:
        return ip == '127.0.0.1' or ip == 'localhost'

    assert answer[0] == 0       # start shard
    assert answer[1] == 16383   # last shard
    if rep_ip is not None:
        assert len(answer) == 4  # the network info
        rep_info = answer[3]
        assert len(rep_info) == 3
        ip_addr = str(rep_info[0], 'utf-8')
        assert ip_addr == rep_ip or (
            is_local_host(ip_addr) and is_local_host(ip))
        assert rep_info[1] == rep_port
    else:
        assert len(answer) == 3
    info = answer[2]
    assert len(info) == 3
    ip_addr = str(info[0], 'utf-8')
    assert ip_addr == ip or (is_local_host(ip_addr) and is_local_host(ip))
    assert info[1] == port
    return True


@dfly_args({"proactor_threads": 4, "cluster_mode": "emulated"})
async def test_cluster_slots_in_replicas(df_local_factory):
    master = df_local_factory.create(port=BASE_PORT)
    replica = df_local_factory.create(port=BASE_PORT+1, logtostdout=True)

    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_replica = aioredis.Redis(port=replica.port)

    res = await c_replica.execute_command("CLUSTER SLOTS")
    assert len(res) == 1
    assert verify_slots_result(
        ip="127.0.0.1", port=replica.port, answer=res[0])
    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        ip="127.0.0.1", port=master.port, answer=res[0])

    # Connect replica to master
    rc = await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    assert str(rc, 'utf-8') == "OK"
    await asyncio.sleep(0.5)
    res = await c_replica.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        ip="127.0.0.1", port=master.port, answer=res[0], rep_ip="127.0.0.1", rep_port=replica.port)
    res = await c_master.execute_command("CLUSTER SLOTS")
    assert verify_slots_result(
        ip="127.0.0.1", port=master.port, answer=res[0], rep_ip="127.0.0.1", rep_port=replica.port)


@dfly_args({"cluster_mode": "emulated", "cluster_announce_ip": "127.0.0.2"})
async def test_cluster_info(async_client):
    res = await async_client.execute_command("CLUSTER INFO")
    assert len(res) == 16
    assert res == {'cluster_current_epoch': '1',
                   'cluster_known_nodes': '1',
                   'cluster_my_epoch': '1',
                   'cluster_size': '1',
                   'cluster_slots_assigned': '16384',
                   'cluster_slots_fail': '0',
                   'cluster_slots_ok': '16384',
                   'cluster_slots_pfail': '0',
                   'cluster_state': 'ok',
                   'cluster_stats_messages_meet_received': '0',
                   'cluster_stats_messages_ping_received': '1',
                   'cluster_stats_messages_ping_sent': '1',
                   'cluster_stats_messages_pong_received': '1',
                   'cluster_stats_messages_pong_sent': '1',
                   'cluster_stats_messages_received': '1',
                   'cluster_stats_messages_sent': '1'
                   }


@dfly_args({"cluster_mode": "emulated", "cluster_announce_ip": "127.0.0.2"})
@pytest.mark.asyncio
async def test_cluster_nodes(async_client):
    res = await async_client.execute_command("CLUSTER NODES")
    assert len(res) == 1
    info = res['127.0.0.2:6379']
    assert res is not None
    assert info['connected'] == True
    assert info['epoch'] == '0'
    assert info['flags'] == 'myself,master'
    assert info['last_ping_sent'] == '0'
    assert info['slots'] == [['0', '16383']]
    assert info['master_id'] == "-"


"""
Test that slot ownership changes correctly with config changes.

Add a key to node0, then move the slot ownership to node1 and see that they both behave as
intended.
Also add keys to each of them that are *not* moved, and see that they are unaffected by the move.
"""
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_slot_ownership_changes(df_local_factory):
    # Start and configure cluster with 2 nodes
    nodes = [
        df_local_factory.create(port=BASE_PORT+i, admin_port=BASE_PORT+i+1000)
        for i in range(2)
    ]

    df_local_factory.start_all(nodes)

    c_nodes = [aioredis.Redis(port=node.port) for node in nodes]
    c_nodes_admin = [aioredis.Redis(port=node.admin_port) for node in nodes]

    node_ids = await asyncio.gather(*(get_node_id(c) for c in c_nodes_admin))

    config = f"""
      [
        {{
          "slot_ranges": [
            {{
              "start": 0,
              "end": 5259
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
              "start": 5260,
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

    await push_config(config, c_nodes_admin)

    # Slot for "KEY1" is 5259

    # Insert a key that should stay in node0
    assert await c_nodes[0].set("KEY0", "value")

    # And to node1 (so it happens that 'KEY0' belongs to 0 and 'KEY2' to 1)
    assert await c_nodes[1].set("KEY2", "value")

    # Insert a key that we will move ownership of to node1 (but without migration yet)
    assert await c_nodes[0].set("KEY1", "value")
    assert await c_nodes[0].execute_command("DBSIZE") == 2

    # Make sure that node0 owns "KEY0"
    assert (await c_nodes[0].get("KEY0")).decode() == "value"

    # Make sure that "KEY1" is not owned by node1
    try:
        await c_nodes[1].set("KEY1", "value")
        assert False, "Should not be able to set key on non-owner cluster node"
    except redis.exceptions.ResponseError as e:
        assert e.args[0] == "MOVED 5259 localhost:30001"

    # And that node1 only has 1 key ("KEY2")
    assert await c_nodes[1].execute_command("DBSIZE") == 1

    print("Moving ownership over 5259 ('KEY1') to other node")

    config = config.replace('5259', '5258').replace('5260', '5259')
    await push_config(config, c_nodes_admin)

    # node0 should have removed "KEY1" as it no longer owns it
    assert await c_nodes[0].execute_command("DBSIZE") == 1
    # node0 should still own "KEY0" though
    assert (await c_nodes[0].get("KEY0")).decode() == "value"
    # node1 should still have "KEY2"
    assert (await c_nodes[1].get("KEY2")).decode() == "value"
    # node1 should own *only* "KEY2"
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
    assert (await c_nodes[0].get("KEY0")).decode() == "value"
    assert await c_nodes[1].execute_command("DBSIZE") == 0


# Tests that master commands to the replica are applied regardless of slot ownership
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_replica_sets_non_owned_keys(df_local_factory):
    # Start and configure cluster with 1 master and 1 replica, both own all slots
    master = df_local_factory.create(port=BASE_PORT, admin_port=BASE_PORT+1000)
    replica = df_local_factory.create(port=BASE_PORT+1, admin_port=BASE_PORT+1001)
    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_master_admin = aioredis.Redis(port=master.admin_port)
    master_id = await get_node_id(c_master_admin)

    c_replica = aioredis.Redis(port=replica.port)
    c_replica_admin = aioredis.Redis(port=replica.admin_port)
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
    await c_master.set("key", "value");
    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await check_all_replicas_finished([c_replica], c_master)
    assert (await c_replica.get("key")).decode() == "value"
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
    await c_master.set("key2", "value");
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
async def test_cluster_flush_slots_after_config_change(df_local_factory):
    # Start and configure cluster with 1 master and 1 replica, both own all slots
    master = df_local_factory.create(port=BASE_PORT, admin_port=BASE_PORT+1000)
    replica = df_local_factory.create(port=BASE_PORT+1, admin_port=BASE_PORT+1001)
    df_local_factory.start_all([master, replica])

    c_master = aioredis.Redis(port=master.port)
    c_master_admin = aioredis.Redis(port=master.admin_port)
    master_id = await get_node_id(c_master_admin)

    c_replica = aioredis.Redis(port=replica.port)
    c_replica_admin = aioredis.Redis(port=replica.admin_port)
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

    await c_master.execute_command("debug", "populate", "100000");
    assert await c_master.execute_command("dbsize") == 100_000

    # Setup replication and make sure that it works properly.
    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await check_all_replicas_finished([c_replica], c_master)
    assert await c_replica.execute_command("dbsize") == 100_000

    resp = await c_master_admin.execute_command("dflycluster", "getslotinfo", "slots", "0")
    assert resp[0][0] == 0
    slot_0_size = resp[0][2]
    print(f'Slot 0 size = {slot_0_size}')
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


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_native_client(df_local_factory):
    # Start and configure cluster with 3 masters and 3 replicas
    masters = [
        df_local_factory.create(port=BASE_PORT+i, admin_port=BASE_PORT+i+1000)
        for i in range(3)
    ]
    df_local_factory.start_all(masters)
    c_masters = [aioredis.Redis(port=master.port) for master in masters]
    c_masters_admin = [aioredis.Redis(port=master.admin_port) for master in masters]
    master_ids = await asyncio.gather(*(get_node_id(c) for c in c_masters_admin))

    replicas = [
        df_local_factory.create(port=BASE_PORT+100+i, admin_port=BASE_PORT+i+1100)
        for i in range(3)
    ]
    df_local_factory.start_all(replicas)
    c_replicas = [aioredis.Redis(port=replica.port) for replica in replicas]
    c_replicas_admin = [aioredis.Redis(port=replica.admin_port) for replica in replicas]
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

    client = aioredis.RedisCluster(decode_responses=True, host="localhost", port=masters[0].port)

    assert await client.set('key0', 'value') == True
    assert await client.get('key0') == 'value'

    async def test_random_keys():
        for i in range(100):
            key = 'key' + str(random.randint(0, 100_000))
            assert await client.set(key, 'value') == True
            assert await client.get(key) == 'value'

    await test_random_keys()
    await asyncio.gather(*(wait_available_async(c) for c in c_replicas))

    # Make sure that getting a value from a replica works as well.
    replica_response = await client.execute_command(
            'get', 'key0', target_nodes=aioredis.RedisCluster.REPLICAS)
    assert 'value' in replica_response.values()

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
