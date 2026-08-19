import asyncio
import json
import logging
import random

import pytest
from redis.exceptions import MovedError

from . import dfly_args
from .cluster_test_utils import (
    MigrationInfo,
    apply_config,
    create_cluster,
    get_node_id,
    next_port,
    push_config,
    wait_for_status,
)
from .instance import DflyInstanceFactory
from .utility import (
    DflySeederFactory,
    check_all_replicas_finished,
    wait_available_async,
)


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
