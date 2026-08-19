import asyncio
import json
import logging
import random
import re
import string
import time

import pytest

from . import dfly_args
from .cluster_test_utils import (
    MigrationInfo,
    apply_config,
    check_for_no_state_status,
    create_cluster,
    create_node_info,
    finalize_migration,
    generate_config,
    get_memory,
    get_node_id,
    key_slot,
    next_port,
    push_config,
    stop_and_get_restore_log,
    wait_for_status,
)
from .instance import DflyInstanceFactory
from .seeder import DebugPopulateSeeder
from .utility import (
    DflySeederFactory,
    ExpirySeeder,
    assert_eventually,
    check_all_replicas_finished,
    extract_int_after_prefix,
    info_tick_timer,
    is_saving,
    tmp_file_name,
    wait_available_async,
)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_flushall_during_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory
):
    # Check data migration from one node to another
    instances, nodes = await create_cluster(
        df_factory,
        2,
        vmodule="cluster_family=2,outgoing_slot_migration=2,incoming_slot_migration=2,streamer=2,server_family=1",
    )
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    seeder = df_seeder_factory.create(keys=10_000, port=nodes[0].instance.port, cluster_mode=True)
    await seeder.run(target_deviation=0.1)

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )

    logging.debug("Start migration")
    await apply_config(nodes)

    await nodes[0].client.execute_command("flushall")

    status1 = await nodes[1].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[0].id
    )
    assert (
        len(status1) == 0 or "FINISHED" not in status1[0]
    ), "Weak test case - finished migration too early"

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    logging.debug("Finalizing migration")
    await finalize_migration(nodes, 0, 1, [], [(0, 16383)])
    logging.debug("Migration finalized")

    assert await nodes[0].client.dbsize() == 0

    # Push config that causes mass async slot deletion on nodes[1]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    await apply_config(nodes)

    # Issue flushall right after pushing new config so it runs at the same time as disowned slots are flushed
    await nodes[1].client.execute_command("flushall")


@pytest.mark.parametrize("interrupt", [False, True])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_cluster_data_migration(df_factory: DflyInstanceFactory, interrupt: bool):
    # Check data migration from one node to another
    instances, nodes = await create_cluster(
        df_factory,
        2,
        vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
    )
    nodes[0].slots = [(0, 9000)]
    nodes[1].slots = [(9001, 16383)]

    await apply_config(nodes)

    for i in range(20):
        key = "KEY" + str(i)
        assert await nodes[key_slot(key) // 9001].client.set(key, "value")

    assert await nodes[0].client.execute_command("DBSIZE") == 10

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(3000, 9000)], nodes[1].id)
    )

    logging.debug("Start migration")
    await apply_config(nodes)

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
    assert status[0].pop() == "[3000, 9000]"  # slot_ranges
    status[0].pop()  # error
    assert status[0] == ["out", nodes[1].id, "FINISHED", 7]

    status = await nodes[1].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[0].id
    )
    assert status[0].pop() == "[3000, 9000]"  # slot_ranges
    status[0].pop()  # error
    assert status[0] == ["in", nodes[0].id, "FINISHED", 7]

    nodes[0].migrations = []
    nodes[0].slots = [(0, 2999)]
    nodes[1].slots = [(3000, 16383)]
    logging.debug("remove finished migrations")
    await apply_config(nodes)

    for i in range(22):
        key = "KEY" + str(i)
        assert await nodes[0 if (key_slot(key) // 3000) == 0 else 1].client.set(key, "value")

    assert await nodes[1].client.execute_command("DBSIZE") == 19

    await check_for_no_state_status([node.admin_client for node in nodes])


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_migration_serializer_expired_fields(df_factory):
    """
    CmdSerializer uses IterateMap/IterateSet during migration.  If time_now_
    was set by a prior command (HGET), the iteration triggers lazy expiry.
    After serialization the source has an empty hash — SAVE must not crash.
    """
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    await apply_config(nodes)

    # Many fields with short TTL spread across DenseSet buckets.
    for i in range(64):
        await nodes[0].client.execute_command("HSETEX", "hkey", "1", f"f{i}", "v")
        await nodes[0].client.execute_command("SADDEX", "skey", "1", f"m{i}")
    await nodes[0].client.execute_command("SET", "normal", "val")

    # TTL is 1s; wait generously to tolerate slow/loaded CI runners.
    await asyncio.sleep(2.0)

    # HGET/SISMEMBER update time_now_ but only partially expire (one bucket).
    # ExecuteRO sees UpperBoundSize > 0, doesn't clean up.
    await nodes[0].client.execute_command("HGET", "hkey", "f0")
    await nodes[0].client.execute_command("SISMEMBER", "skey", "m0")

    # Start migration — serializer iterates all DenseSet buckets, expiring rest.
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].port, [(0, 16383)], nodes[1].id)
    )
    await apply_config(nodes)
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    # Without the fix, SAVE on source crashes (DFATAL on empty hash).
    assert await nodes[0].admin_client.execute_command("SAVE", "RDB", "test_zombie.rdb")

    await finalize_migration(nodes, 0, 1, [], [(0, 16383)])

    assert await nodes[1].client.execute_command("GET", "normal") == "val"
    # Server survived — no zombie crash.


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes", "cache_mode": "true"})
async def test_migration_with_key_ttl(df_factory):
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    await nodes[0].client.execute_command("set k_with_ttl v1 EX 2")
    await nodes[0].client.execute_command("set k_without_ttl v2")
    await nodes[0].client.execute_command("set k_sticky v3")
    assert await nodes[0].client.execute_command("stick k_sticky") == 1

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].port, [(0, 16383)], nodes[1].id)
    )
    logging.debug("Start migration")
    await apply_config(nodes)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("finalize migration")
    await apply_config(nodes)

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
    instances, nodes = await create_cluster(df_factory, 4)
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
    seeder = df_seeder_factory.create(
        keys=2000, port=m1_node.instance.port, cluster_mode=True, mirror_to_fake_redis=True
    )
    seed = asyncio.create_task(seeder.run())

    logging.debug("start replication")
    await r1_node.admin_client.execute_command(f"replicaof localhost {m1_node.instance.port}")
    await r2_node.admin_client.execute_command(f"replicaof localhost {m2_node.instance.port}")

    await wait_available_async(r1_node.admin_client)
    await wait_available_async(r2_node.admin_client)

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
    seeder.stop()
    await seed
    fake_capture = await seeder.capture_fake_redis()
    assert await seeder.compare(fake_capture, r1_node.instance.port)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "pause_wait_timeout": 10})
async def test_start_replication_during_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    """
    Test replication with migration. Create the following setup:

    master_1 do migration to master_2 and we start replication for master_1 during this migration

    in the end master_1 and replica_1 should have the same data
    """
    instances, nodes = await create_cluster(df_factory, 3)
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
    seeder = df_seeder_factory.create(
        keys=10000, port=nodes[0].instance.port, cluster_mode=True, mirror_to_fake_redis=True
    )
    seed = asyncio.create_task(seeder.run())

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

    seeder.stop()
    await seed

    await check_all_replicas_finished([r1_node.client], m1_node.client)

    fake_capture = await seeder.capture_fake_redis()
    assert await seeder.compare(fake_capture, r1_node.instance.port)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_keys_expiration_during_migration(df_factory: DflyInstanceFactory):
    # Check data migration from one node to another with expiration
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    logging.debug("Start seeder")
    await nodes[0].client.execute_command("debug", "populate", "100", "foo", "100", "RAND")

    capture_before = await DebugPopulateSeeder.capture(nodes[0].client)

    seeder = ExpirySeeder(timeout=4)
    seeder_task = asyncio.create_task(seeder.run(nodes[0].client))
    await seeder.wait_until_n_inserts(500)

    logging.debug("Start migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )
    await apply_config(nodes)

    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED")

    logging.debug("Stop seeders")
    seeder.stop()
    await seeder_task

    logging.debug("finish migration")
    await finalize_migration(nodes, 0, 1, [], [(0, 16383)])

    # wait to expire all keys
    await asyncio.sleep(5)

    assert await DebugPopulateSeeder.capture(nodes[1].client) == capture_before

    stats = await nodes[1].client.info("STATS")
    assert stats["expired_keys"] > 0


@pytest.mark.parametrize("migration_first", [False, True])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_snapshoting_during_migration(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory, migration_first: bool
):
    """
    Test saving snapshot during migration. Create the following setups:

    1) Start saving and then run migration simultaneously
    2) Run migration and start saving simultaneously

    The result should be the same: snapshot contains all the data that existed before migration
    """
    dbfilename = f"snap_{tmp_file_name()}"
    instances = [
        df_factory.create(
            dbfilename=dbfilename if i == 0 else "",
            port=next(next_port),
            admin_port=next(next_port),
        )
        for i in range(2)
    ]
    df_factory.start_all(instances)

    nodes = [await create_node_info(n) for n in instances]

    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    logging.debug("Push initial config")
    await apply_config(nodes)

    logging.debug("create data")
    seeder = df_seeder_factory.create(
        keys=10000, port=nodes[0].instance.port, cluster_mode=True, mirror_to_fake_redis=True
    )
    seed = asyncio.create_task(seeder.run())

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    ]

    async def start_migration():
        logging.debug("start migration")
        await apply_config(nodes)

    async def start_save():
        logging.debug("BGSAVE")
        await nodes[0].client.execute_command("BGSAVE")

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
    nodes[1].migrations = []
    nodes[1].slots = [(0, 16383)]

    await apply_config(nodes)

    seeder.stop()
    await seed
    fake_capture = await seeder.capture_fake_redis()
    assert await seeder.compare(fake_capture, nodes[1].instance.port)

    await nodes[1].client.execute_command(
        "DFLY",
        "LOAD",
        f"{dbfilename}-summary.dfs",
    )

    # TODO: We can't compare the post-loaded data as is, because it might have changed by now.
    # We can try to use FakeRedis with the DebugPopulateSeeder comparison here.


@dfly_args(
    {"proactor_threads": 2, "cluster_mode": "yes", "migration_buckets_serialization_threshold": 1}
)
@pytest.mark.large
@pytest.mark.parametrize("chunk_size", [1_000_000, 30])
@pytest.mark.asyncio
@pytest.mark.exclude_epoll
async def test_cluster_migration_while_seeding(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory, chunk_size
):
    instances, nodes = await create_cluster(df_factory, 2, serialization_max_chunk_size=chunk_size)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    client0 = nodes[0].client

    await apply_config(nodes)

    logging.debug("Seeding cluster")
    seeder = df_seeder_factory.create(
        keys=20_000, port=instances[0].port, cluster_mode=True, mirror_to_fake_redis=True
    )
    await seeder.run(target_deviation=0.1)

    seed = asyncio.create_task(seeder.run())
    await asyncio.sleep(1)

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(0, 16383)], nodes[1].id)
    ]
    logging.debug("Migrating slots")
    await apply_config(nodes)

    logging.debug("Waiting for migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=300)
    logging.debug("Migration finished")

    logging.debug("Finalizing migration")
    await finalize_migration(nodes, 0, 1, [], [(0, 16383)])

    await asyncio.sleep(1)  # Let seeder feed dest before migration finishes

    seeder.stop()
    await seed
    logging.debug("Seeding finished")

    assert (
        await get_memory(client0, "used_memory_peak_rss")
        < await get_memory(client0, "used_memory_rss") * 1.2
    )

    capture = await seeder.capture_fake_redis()
    assert await seeder.compare(capture, instances[1].port)

    line = stop_and_get_restore_log(nodes[0].instance)
    assert extract_int_after_prefix("Keys skipped ", line) == 0
    assert extract_int_after_prefix("buckets skipped ", line) > 0
    assert extract_int_after_prefix("keys written ", line) >= 15_000
    # buckets on_db_update can be 0 once in a while because we can not predict keys distribution during migration
    assert extract_int_after_prefix("buckets on_db_update ", line) > 0


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
@pytest.mark.asyncio
async def test_cluster_migrations_sequence(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

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
    await apply_config(nodes)

    for i in range(slot_step, 16301, slot_step):
        logging.debug("Waiting for migration to finish")
        await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=10)

        nodes[0].slots = [(i, 16383)]
        nodes[1].slots = [(0, i - 1)]
        end_slot = min(i + slot_step - 1, 16383)
        nodes[0].migrations = [
            MigrationInfo("127.0.0.1", instances[1].admin_port, [(i, end_slot)], nodes[1].id)
        ]

        await apply_config(nodes)

    logging.debug("Waiting for migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=10)
    await apply_config(nodes)

    logging.debug("Finalizing migration")
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    nodes[0].migrations = []
    await apply_config(nodes)

    logging.debug("stop seeding")
    seeder.stop()
    await seed

    capture = await seeder.capture_fake_redis()
    assert await seeder.compare(capture, instances[1].port)


@pytest.mark.asyncio
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_migration_one_after_another(df_factory: DflyInstanceFactory, df_seeder_factory):
    # 1. Create cluster of 3 nodes with all slots allocated to first node.
    instances, nodes = await create_cluster(
        df_factory,
        3,
        vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
    )
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    nodes[2].slots = []
    await apply_config(nodes)

    logging.debug("DEBUG POPULATE first node")
    key_num = 100000
    await DebugPopulateSeeder(key_target=key_num, data_size=100).run(nodes[0].client)
    dbsize_node0 = await nodes[0].client.dbsize()
    assert dbsize_node0 > (key_num * 0.95)

    # 2. Start migrating part of the slots from first node to second
    logging.debug("Start first migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16300)], nodes[1].id)
    )
    await apply_config(nodes)

    # 3. Wait for migratin finish
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=50)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED", timeout=50)

    nodes[0].migrations = []
    nodes[0].slots = [(16301, 16383)]
    nodes[1].slots = [(0, 16300)]
    nodes[2].slots = []
    await apply_config(nodes)

    # 4. Start migrating remaind slots from first node to third node
    logging.debug("Start second migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[2].instance.admin_port, [(16301, 16383)], nodes[2].id)
    )
    await apply_config(nodes)

    # 5. Wait for migratin finish
    await wait_for_status(nodes[0].admin_client, nodes[2].id, "FINISHED", timeout=10)
    await wait_for_status(nodes[2].admin_client, nodes[0].id, "FINISHED", timeout=10)

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16300)]
    nodes[2].slots = [(16301, 16383)]
    await apply_config(nodes)

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


@pytest.mark.large
@pytest.mark.exclude_epoll
@pytest.mark.asyncio
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "pause_wait_timeout": 10})
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
    await apply_config(nodes)

    key_num = 100000
    logging.debug(f"DEBUG POPULATE first node with number of keys: {key_num}")
    await DebugPopulateSeeder(key_target=key_num, data_size=100).run(nodes[0].client)
    dbsize_node0 = await nodes[0].client.dbsize()
    assert dbsize_node0 > (key_num * 0.95)

    logging.debug("start seeding")
    # Running seeder with pipeline mode when finalizing migrations leads to errors
    # TODO: I believe that changing the seeder to generate pipeline command only on specific slot will fix the problem
    seeder = df_seeder_factory.create(
        keys=50_000,
        port=instances[0].port,
        cluster_mode=True,
        pipeline=False,
        mirror_to_fake_redis=True,
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
            await apply_config(nodes)

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
            await apply_config(nodes)

    all_migrations = [asyncio.create_task(do_migration(i)) for i in range(1, 4)]
    for migration in all_migrations:
        await migration

    logging.debug("stop seeding")
    seeder.stop()
    await seed
    await asyncio.sleep(0.5)  # wait untill all keys with ttl are expired
    capture = await seeder.capture_fake_redis()
    assert await seeder.compare(capture, nodes[1].instance.port)


async def verify_keys_match_number_of_index_docs(client, expected_num_keys):
    # Get number of docs in index
    index_info = await client.execute_command("FT.INFO idx")
    index_info_num_docs = index_info[9]

    # Get number of keys in database
    keyspace_info = await client.info("keyspace")
    keyspace_keys = keyspace_info["db0"]["keys"]

    assert index_info_num_docs == keyspace_keys
    assert index_info_num_docs == expected_num_keys
    assert keyspace_keys == expected_num_keys


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes", "cluster_search": "yes"})
async def test_remove_docs_on_cluster_migration(df_factory):
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    # Create index on both nodes
    await nodes[0].client.execute_command(
        "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "v", "TEXT"
    )

    # Populate node 0
    keys = 100
    for i in range(keys):
        random_string = "".join(random.choices(string.ascii_letters + string.digits, k=1_000))
        await nodes[0].client.execute_command("HSET", f"doc:{i}", "v", random_string)

    # Verify on node 0 that keys are added and index is populated
    await verify_keys_match_number_of_index_docs(nodes[0].client, keys)

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].port, [(0, 16383)], nodes[1].id)
    )
    logging.debug("Start migration")
    await apply_config(nodes)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("finalize migration")
    await apply_config(nodes)

    await asyncio.sleep(1)

    # Verify on node 1 that keys are moved and index is populated
    await verify_keys_match_number_of_index_docs(nodes[1].client, keys)

    # Verify that node 0 doesn't have any keys and no index docs
    await verify_keys_match_number_of_index_docs(nodes[0].client, 0)


async def _run_tiering_migration(
    df_factory,
    *,
    maxmemory,
    min_tiered_entries,
    delete_keys_count=0,
):
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            proactor_threads=2,
            tiered_prefix="/tmp/tiered/cluster_node",
            tiered_offload_threshold="0.2",
            tiered_experimental_cooling="False",
            maxmemory=maxmemory,
            tiered_max_pending_stash_bytes="128KB",
        ),
        df_factory.create(
            port=next(next_port), admin_port=next(next_port), proactor_threads=2, maxmemory="1024MB"
        ),
    ]
    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    keys = 1000000
    await nodes[0].client.execute_command(f"DEBUG POPULATE {keys} key 440")

    info = await nodes[0].client.info()
    assert info["oom_rejections"] == 0
    assert info["db0"]["keys"] == keys

    async for info, breaker in info_tick_timer(nodes[0].client, section="TIERED", timeout=20):
        with breaker:
            logging.info(f"Tiered entries: {info['tiered_entries']}")
            assert info["tiered_entries"] >= min_tiered_entries

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", instances[1].port, [(0, 16383)], nodes[1].id)
    )

    await apply_config(nodes)

    delete_succeded = 0
    if delete_keys_count:
        migration_done = False

        async def delete_job():
            nonlocal delete_succeded
            for i in range(delete_keys_count):
                if migration_done:
                    break
                try:
                    delete_succeded += await nodes[0].client.delete(f"key:{i}")
                except Exception:
                    pass

        delete_task = asyncio.create_task(delete_job())

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)

    if delete_keys_count:
        migration_done = True
        await delete_task

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("finalize migration")
    await apply_config(nodes)

    async for info, breaker in info_tick_timer(nodes[0].client, section="TIERED", timeout=60):
        with breaker:
            assert info["tiered_entries"] == 0

    info = await nodes[1].client.info("keyspace")
    assert info["db0"]["keys"] == keys - delete_succeded


@pytest.mark.large
@pytest.mark.exclude_epoll
@pytest.mark.opt_only
@dfly_args({"cluster_mode": "yes"})
async def test_cluster_migration_with_tiering(df_factory):
    await _run_tiering_migration(
        df_factory,
        maxmemory="800MB",
        min_tiered_entries=10_000,
    )


@pytest.mark.large
@pytest.mark.exclude_epoll
@pytest.mark.opt_only
@dfly_args({"cluster_mode": "yes"})
async def test_cluster_migration_with_tiering_and_deletes(df_factory: DflyInstanceFactory):
    await _run_tiering_migration(
        df_factory,
        maxmemory="800MB",
        min_tiered_entries=10_000,
        delete_keys_count=50_000,
    )


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "replication_timeout": 3000})
async def test_repeated_flushslots_with_replica(df_factory: DflyInstanceFactory):
    """
    Back to back DFLYCLUSTER CONFIG pushes that drop slots must journal one FLUSHSLOTS record
    per push, each with its own txid. Replicas rendezvous their flows on this record keyed by
    txid, so a shared txid leaves them waiting for each other forever.
    """
    # replica=2 traces the rendezvous txid, the only place it is observable. logbuflevel=-1
    # keeps those records out of the log buffer, which the SIGKILL below would discard.
    master = df_factory.create(port=next(next_port), admin_port=next(next_port), logbuflevel=-1)
    replica = df_factory.create(
        port=next(next_port),
        admin_port=next(next_port),
        logbuflevel=-1,
        vmodule="replica=2,cluster_family=1,db_slice=1,dflycmd=1",
    )
    df_factory.start_all([master, replica])

    c_master, c_master_admin = master.client(), master.admin_client()
    c_replica, c_replica_admin = replica.client(), replica.admin_client()
    master_id = await get_node_id(c_master)
    replica_id = await get_node_id(c_replica)

    def make_config(start_slot):
        # Slots below start_slot move to a placeholder owner, so the master has to flush them.
        dropped = (
            ""
            if start_slot == 0
            else f""",
            {{
              "slot_ranges": [{{ "start": 0, "end": {start_slot - 1} }}],
              "master": {{ "id": "other-master", "ip": "localhost", "port": 9000 }},
              "replicas": []
            }}"""
        )
        return f"""
          [
            {{
              "slot_ranges": [{{ "start": {start_slot}, "end": 16383 }}],
              "master": {{ "id": "{master_id}", "ip": "localhost", "port": {master.port} }},
              "replicas": [
                {{ "id": "{replica_id}", "ip": "localhost", "port": {replica.port} }}
              ]
            }}{dropped}
          ]
        """

    await push_config(make_config(0), [c_master_admin, c_replica_admin])
    await c_master.execute_command("debug", "populate", "100000")
    await c_replica.execute_command("REPLICAOF", "localhost", master.port)
    await check_all_replicas_finished([c_replica], c_master)
    assert await c_replica.execute_command("dbsize") == 100_000
    logging.info("stable sync reached with %d keys", await c_master.execute_command("dbsize"))

    stop = False

    async def write_load():
        # Keys land on slots that are never dropped, so they must survive every flush.
        value = "v" * 4096
        i = 0
        async with master.client() as c:
            while not stop:
                pipe = c.pipeline(transaction=False)
                for _ in range(50):
                    pipe.set(f"{{a}}:{i}", value)
                    i += 1
                try:
                    await pipe.execute()
                except Exception as e:
                    logging.warning("write load error: %s", e)
                    await asyncio.sleep(0.05)

    async def probe(name, client):
        t0 = time.perf_counter()
        try:
            await asyncio.wait_for(client.execute_command("PING"), timeout=5)
            logging.info("%s answers PING in %.1f ms", name, (time.perf_counter() - t0) * 1000)
            return True
        except Exception as e:
            logging.warning("%s does not answer PING: %r", name, e)
            return False

    load = asyncio.create_task(write_load())
    master_alive = replica_alive = False
    converged = True
    try:
        # Stress knob only - whether two records actually collide is a race. The txid
        # assertions at the end are the detector.
        pushes, step = 15, 500
        for start in range(step, step * pushes + 1, step):
            await push_config(make_config(start), [c_master_admin, c_replica_admin])
            logging.info("pushed config owning slots %d-16383", start)

        stop = True
        # Nothing else bounds this test, so do not wait on an in-flight pipeline forever.
        try:
            await asyncio.wait_for(load, timeout=30)
        except asyncio.TimeoutError:
            load.cancel()
            await asyncio.gather(load, return_exceptions=True)

        master_alive = await probe("master", c_master)
        replica_alive = await probe("replica", c_replica)

        for name, client in (("master", c_master), ("replica", c_replica)):
            try:
                info = await asyncio.wait_for(client.info("replication"), timeout=5)
                keep = {
                    k: v
                    for k, v in info.items()
                    if k
                    in (
                        "role",
                        "master_link_status",
                        "master_last_io_seconds_ago",
                        "master_sync_in_progress",
                        "connected_slaves",
                        "slave0",
                    )
                }
                logging.info("%s INFO replication: %s", name, keep)
            except Exception as e:
                logging.warning("%s INFO replication failed: %r", name, e)

        try:
            await asyncio.wait_for(
                check_all_replicas_finished([c_replica], c_master, timeout=30), 45
            )

            # FlushSlots runs in a detached fiber, so an acked record does not mean the keys
            # are gone, and dbsize equality alone can catch a value both sides pass through.
            dropped = f"0-{step * pushes - 1}"
            slotinfo = ("DFLYCLUSTER", "GETSLOTINFO", "SLOTS", dropped)

            @assert_eventually(timeout=30)
            async def dropped_slots_empty():
                m_slots, r_slots, m, r = await asyncio.gather(
                    c_master_admin.execute_command(*slotinfo),
                    c_replica_admin.execute_command(*slotinfo),
                    c_master.execute_command("dbsize"),
                    c_replica.execute_command("dbsize"),
                )
                m_left = sum(row[2] for row in m_slots)
                r_left = sum(row[2] for row in r_slots)
                assert m_left == 0, f"master still holds {m_left} keys in slots {dropped}"
                assert r_left == 0, f"replica still holds {r_left} keys in slots {dropped}"
                assert m == r, f"master and replica diverged across repeated FLUSHSLOTS: {m} vs {r}"
                logging.info("after the flush sequence: master dbsize=%d replica dbsize=%d", m, r)

            await dropped_slots_empty()
        except AssertionError:
            raise
        except Exception as e:
            converged = False
            logging.warning("replication never converged after the flush sequence: %r", e)
    finally:
        stop = True
        load.cancel()
        await asyncio.gather(load, return_exceptions=True)
        # A wedged replica ignores SIGTERM and the fixture would burn ~125s per instance.
        master.stop(kill=True)
        replica.stop(kill=True)

    def hits(inst, pat):
        return sorted({l.strip() for l in inst.find_in_logs(pat)})

    timeouts = hits(master, "Stream timed out")
    disconnects = hits(master, "Disconnecting from replica")
    logging.info("RESULT stream_timeouts=%d master_disconnects=%d", len(timeouts), len(disconnects))
    for line in timeouts:
        logging.info("  MASTER %s", line)
    logging.info(
        "RESULT master_alive=%s replica_alive=%s converged=%s",
        master_alive,
        replica_alive,
        converged,
    )

    # Each flush must show up under exactly one non-zero txid.
    txid_re = r"Execute txid: (\d+) waiting for data in all shards"
    txids = [int(re.search(txid_re, l).group(1)) for l in hits(replica, txid_re)]
    logging.info("RESULT flushslots_txids=%s", sorted(set(txids)))
    assert txids, "replica logged no global-command rendezvous - is vmodule=replica=2 set?"
    assert 0 not in txids, "FLUSHSLOTS was journaled with txid 0"
    assert (
        len(set(txids)) == pushes
    ), f"expected one distinct txid per FLUSHSLOTS, got {sorted(set(txids))}"

    assert not timeouts, "replication stalled past replication_timeout during repeated FLUSHSLOTS"
    assert converged, "replication never converged after repeated FLUSHSLOTS"
