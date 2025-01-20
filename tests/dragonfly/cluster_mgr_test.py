import subprocess
import pytest
import redis
from redis import asyncio as aioredis
from .utility import *
from . import dfly_args

BASE_PORT = 30001


async def insert_cluster_data(cluster_client: redis.RedisCluster):
    for i in range(1_000):
        await cluster_client.set(i, i)


async def check_cluster_data(cluster_client: redis.RedisCluster):
    for i in range(1_000):
        assert await cluster_client.get(i) == str(i)


def run_cluster_mgr(args):
    print(f"Running cluster_mgr.py {args}")
    result = subprocess.run(["../tools/cluster_mgr.py", *args])
    return result.returncode == 0


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_cluster_mgr(df_factory):
    NODES = 3
    masters = [df_factory.create(port=BASE_PORT + i) for i in range(NODES)]
    replicas = [df_factory.create(port=BASE_PORT + 100 + i) for i in range(NODES)]
    df_factory.start_all([*masters, *replicas])

    # Initialize a cluster (all slots belong to node 0)
    assert run_cluster_mgr(["--action=config_single_remote", f"--target_port={BASE_PORT}"])
    for i in range(1, NODES):
        assert run_cluster_mgr(
            ["--action=attach", f"--target_port={BASE_PORT}", f"--attach_port={BASE_PORT+i}"]
        )

    # Feed the cluster with data and test that it works correctly
    client = aioredis.RedisCluster(decode_responses=True, host="127.0.0.1", port=masters[0].port)
    await insert_cluster_data(client)
    await check_cluster_data(client)

    # Migrate ~half of the slots to node 1
    assert run_cluster_mgr(
        [
            f"--action=migrate",
            f"--target_port={BASE_PORT + 1}",
            f"--slot_start=8000",
            f"--slot_end=16383",
        ]
    )
    await check_cluster_data(client)

    # Can only detach node 2 (with no assigned slots)
    assert not run_cluster_mgr(["--action=detach", f"--target_port={BASE_PORT}"])
    assert not run_cluster_mgr(["--action=detach", f"--target_port={BASE_PORT + 1}"])
    assert run_cluster_mgr(["--action=detach", f"--target_port={BASE_PORT + 2}"])
    await check_cluster_data(client)

    # Can't attach non-replica as replica
    assert not run_cluster_mgr(
        [
            f"--action=attach",
            f"--target_port={BASE_PORT}",
            f"--attach_port={BASE_PORT+2}",
            f"--attach_as_replica=True",
        ]
    )

    # Reattach node 2 and migrate some slots to it
    assert run_cluster_mgr(
        ["--action=attach", f"--target_port={BASE_PORT}", f"--attach_port={BASE_PORT+2}"]
    )
    await check_cluster_data(client)
    # Slots 7000-8000 belong to node0, while 8001-9000 belong to node1. cluster_mgr doesn't support
    # such a migration in a single command.
    assert not run_cluster_mgr(
        [
            f"--action=migrate",
            f"--target_port={BASE_PORT + 1}",
            f"--slot_start=7000",
            f"--slot_end=9000",
        ]
    )
    assert run_cluster_mgr(
        ["--action=migrate", f"--target_port={BASE_PORT + 2}", "--slot_start=0", "--slot_end=2000"]
    )
    await check_cluster_data(client)
    assert run_cluster_mgr(
        [
            f"--action=migrate",
            f"--target_port={BASE_PORT + 2}",
            f"--slot_start=8000",
            f"--slot_end=10000",
        ]
    )
    await check_cluster_data(client)

    # Can't attach replica before running REPLICAOF
    assert not run_cluster_mgr(
        [
            f"--action=attach",
            f"--attach_as_replica=True",
            f"--target_port={BASE_PORT}",
            f"--attach_port={replicas[0].port}",
        ]
    )

    # Add replicas
    replica_clients = [replica.client() for replica in replicas]
    for i in range(NODES):
        await replica_clients[i].execute_command(f"replicaof 127.0.0.1 {masters[i].port}")
        assert run_cluster_mgr(
            [
                f"--action=attach",
                f"--attach_as_replica=True",
                f"--target_port={masters[i].port}",
                f"--attach_port={replicas[i].port}",
            ]
        )

    # Can't take over when target is a master
    assert not run_cluster_mgr(["--action=takeover", f"--target_port={masters[i].port}"])

    # Take over replica 0
    assert run_cluster_mgr(["--action=takeover", f"--target_port={replicas[0].port}"])
    await replica_clients[0].execute_command("replicaof no one")
    await check_cluster_data(client)

    # Revert take over
    c_master0 = masters[0].client()
    await c_master0.execute_command(f"replicaof 127.0.0.1 {replicas[0].port}")
    assert run_cluster_mgr(
        [
            f"--action=attach",
            f"--attach_as_replica=True",
            f"--target_port={replicas[0].port}",
            f"--attach_port={masters[0].port}",
        ]
    )
    assert run_cluster_mgr(["--action=takeover", f"--target_port={masters[0].port}"])
    await c_master0.execute_command(f"replicaof no one")
    await replica_clients[0].execute_command(f"replicaof 127.0.0.1 {masters[0].port}")
    assert run_cluster_mgr(
        [
            f"--action=attach",
            f"--attach_as_replica=True",
            f"--target_port={masters[0].port}",
            f"--attach_port={replicas[0].port}",
        ]
    )
    await check_cluster_data(client)

    # Print the config - we don't really verify the output, but at least make sure there's no error
    assert run_cluster_mgr(["--action=print_config", f"--target_port={replicas[0].port}"])

    # Test detach replicas work
    for i in range(NODES):
        assert run_cluster_mgr(["--action=detach", f"--target_port={replicas[i].port}"])
    await check_cluster_data(client)
    await client.aclose()
