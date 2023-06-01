import pytest
import redis
from redis import asyncio as aioredis
import asyncio

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
