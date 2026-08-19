import asyncio
import logging

import pytest
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import MovedError

from redis import asyncio as aioredis

from . import dfly_args
from .cluster_test_utils import (
    MigrationInfo,
    apply_config_via_client,
    create_node_info,
    next_port,
    wait_for_status,
)
from .instance import DflyInstanceFactory


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
