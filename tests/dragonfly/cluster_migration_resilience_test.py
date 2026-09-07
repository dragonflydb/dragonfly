import asyncio
import itertools
import json
import logging
import random

import pytest
from redis.exceptions import MovedError

from redis import asyncio as aioredis

from . import dfly_args
from .cluster_test_utils import (
    MigrationInfo,
    apply_config,
    apply_config_via_client,
    check_for_no_state_status,
    create_cluster,
    create_node_info,
    finalize_migration,
    generate_config,
    get_memory,
    next_port,
    push_config,
    stop_and_get_restore_log,
    wait_for_error,
    wait_for_status,
)
from .instance import DflyInstanceFactory
from .seeder import DebugPopulateSeeder
from .utility import (
    assert_eventually,
    extract_int_after_prefix,
    tick_timer,
    wait_available_async,
)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "migration_finalization_timeout_ms": 50})
async def test_network_disconnect_during_migration(df_factory, proxy_factory):
    instances, nodes = await create_cluster(
        df_factory,
        2,
        vmodule="cluster_family=9,outgoing_slot_migration=9,incoming_slot_migration=9",
    )
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    await DebugPopulateSeeder(key_target=100000).run(nodes[0].client)
    start_capture = await DebugPopulateSeeder.capture(nodes[0].client)

    proxy = await proxy_factory(nodes[1].instance.admin_port)
    nodes[0].migrations.append(MigrationInfo("127.0.0.1", proxy.port, [(0, 16383)], nodes[1].id))
    logging.debug("Start migration")
    await apply_config(nodes)

    for _ in range(10):
        await asyncio.sleep(random.randint(0, 50) / 100)
        info = await nodes[0].admin_client.info("CLUSTER")
        logging.debug("drop connection: %s", info)
        proxy.drop_connection()
        logging.debug(
            await nodes[0].admin_client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
        )

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "SYNC", 20)

    await proxy.close()
    await proxy.start_serving()

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)
    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    logging.debug("remove finished migrations")
    await apply_config(nodes)

    assert (await DebugPopulateSeeder.capture(nodes[1].client)) == start_capture


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
            5, 20, 30_000, 1_000_000, "false", marks=[pytest.mark.large, pytest.mark.opt_only]
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
    cache_mode: str,
):
    instances, nodes = await create_cluster(
        df_factory,
        node_count,
        vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2,streamer=2",
        serialization_max_chunk_size=huge_values,
        replication_stream_output_limit=10,
        cache_mode=cache_mode,
    )

    # Generate equally sized ranges and distribute by nodes
    step = 16400 // segments
    for slot_range in [(s, min(s + step - 1, 16383)) for s in range(0, 16383, step)]:
        nodes[random.randint(0, node_count - 1)].slots.append(slot_range)

    # Push config to all nodes
    await apply_config(nodes)

    # Fill instances with some data
    seeder = df_seeder_factory.create(
        keys=keys, port=nodes[0].instance.port, cluster_mode=True, mirror_to_fake_redis=True
    )
    seed_task = asyncio.create_task(seeder.run())

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
    await apply_config(nodes)

    logging.debug("finish migrations")

    async def all_finished():
        res = True
        for node in nodes:
            states = await node.admin_client.execute_command("DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
            logging.debug(states)
            for state in states:
                direction, node_id, st, _, _, _ = state
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
                        await apply_config(nodes)
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

    # Compare to fake redis, capture ignores counter keys
    seeder.stop()
    await seed_task
    fake_capture = await seeder.capture_fake_redis()

    assert await seeder.compare(fake_capture, nodes[0].instance.port)

    await asyncio.gather(*[c.aclose() for c in counter_connections])


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
@pytest.mark.asyncio
async def test_cluster_migration_cancel(df_factory: DflyInstanceFactory):
    """Check data migration from one node to another."""
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 8000)]
    nodes[1].slots = [(8001, 16383)]

    logging.debug("Pushing data to slot 6XXX")
    SIZE = 10_000
    await apply_config(nodes)
    for i in range(SIZE):
        assert await nodes[0].client.set(f"{{key50}}:{i}", i)  # key50 belongs to slot 6686
    assert [SIZE, 0] == [await node.client.dbsize() for node in nodes]

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(6000, 8000)], nodes[1].id)
    ]
    logging.debug("Migrating slots 6000-8000")
    await apply_config(nodes)

    logging.debug("Cancelling migration")
    nodes[0].migrations = []
    await apply_config(nodes)
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
    await apply_config(nodes)
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")
    assert [SIZE, SIZE] == [await node.client.dbsize() for node in nodes]

    logging.debug("Finalizing migration")
    await finalize_migration(nodes, 0, 1, [(0, 6000)], [(6001, 16383)])
    logging.debug("Migration finalized")

    while 0 != await nodes[0].client.dbsize():
        logging.debug("wait until source dbsize is empty")
        await asyncio.sleep(0.1)

    for i in range(SIZE):
        assert str(i) == await nodes[1].client.get(f"{{key50}}:{i}")


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
@pytest.mark.asyncio
@pytest.mark.opt_only
@pytest.mark.exclude_epoll
async def test_cluster_migration_huge_container(df_factory: DflyInstanceFactory):
    instances, nodes = await create_cluster(df_factory, 2)
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    logging.debug("Generating huge containers")
    seeder = DebugPopulateSeeder(
        key_target=100,
        data_size=10_000_000,
        collection_size=10_000,
        variance=1,
        samples=1,
        types=["LIST", "HASH", "SET", "ZSET", "STREAM", "STRING"],
    )
    await seeder.run(nodes[0].client)
    source_data = await DebugPopulateSeeder.capture(nodes[0].client)

    mem_before = await get_memory(nodes[0].client, "used_memory_rss")

    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", instances[1].admin_port, [(0, 16383)], nodes[1].id)
    ]
    logging.debug("Migrating slots")
    await apply_config(nodes)

    logging.debug("Waiting for migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=300)

    target_data = await DebugPopulateSeeder.capture(nodes[1].client)
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


@pytest.mark.large
@dfly_args({"cluster_mode": "yes"})
async def test_cluster_memory_consumption_migration(df_factory: DflyInstanceFactory):
    # Check data migration from one node to another
    instances, nodes = await create_cluster(df_factory, 3, maxmemory="15G", vmodule="streamer=2")
    nodes[0].slots = [(0, 16383)]
    for i in range(1, len(instances)):
        nodes[i].slots = []

    await apply_config(nodes)

    await nodes[0].client.execute_command("DEBUG POPULATE 5000000 test 1000 RAND SLOTS 0 16383")

    await asyncio.sleep(2)

    migration_nodes = len(instances) - 1
    slot_step = 16384 // migration_nodes
    ranges = []
    for i in range(migration_nodes):
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
    await apply_config(nodes)

    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED", 1000)

    nodes[0].migrations = []
    nodes[0].slots = []
    for i in range(1, len(instances)):
        nodes[i].slots = [(ranges[i - 1], ranges[i] - 1)]
    logging.debug("remove finished migrations")
    await apply_config(nodes)

    await check_for_no_state_status([node.admin_client for node in nodes])


@pytest.mark.large
@pytest.mark.exclude_epoll
@pytest.mark.asyncio
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "migration_buckets_cpu_budget": 1})
async def test_migration_timeout_on_sync(df_factory: DflyInstanceFactory, df_seeder_factory):
    # Timeout set to 3 seconds because we must first saturate the socket before we get the timeout
    instances, nodes = await create_cluster(
        df_factory,
        2,
        replication_timeout=3000,
        vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2",
    )
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    logging.debug("source node DEBUG POPULATE")

    await DebugPopulateSeeder(key_target=300000, data_size=1000).run(nodes[0].client)

    # we use this seeder to saturate the pending_buf_ in streamer
    seeder = df_seeder_factory.create(port=nodes[0].instance.port, cluster_mode=True)
    fill_task = asyncio.create_task(seeder.run())

    logging.debug("Start migration")
    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )
    await apply_config(nodes)

    await asyncio.sleep(random.randint(0, 50) / 100)
    # to pause migration we need to be in sync state
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "SYNC", 1000)

    logging.debug("debug migration pause")
    await nodes[1].client.execute_command("debug migration pause")

    await wait_for_error(
        nodes[0].admin_client, nodes[1].id, "JournalStreamer write operation timeout", 30
    )

    logging.debug("debug migration resume")
    await nodes[1].client.execute_command("debug migration resume")

    # Stop seeder
    seeder.stop()
    await fill_task

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED")

    with pytest.raises(MovedError) as e_info:
        await nodes[0].client.get("x")

    assert f"16287 127.0.0.1:{instances[1].port}" == str(e_info.value)

    nodes[0].migrations = []
    # cancel migration for the source node to get the original data from it
    await push_config(json.dumps(generate_config(nodes)), [nodes[0].admin_client])

    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]
    # finish migration for the target node to get the migrated data from it
    await push_config(json.dumps(generate_config(nodes)), [nodes[1].admin_client])

    source_capture = await DebugPopulateSeeder.capture(nodes[0].client)
    assert (await DebugPopulateSeeder.capture(nodes[1].client)) == source_capture


"""
Test cluster node distributing its slots into 2 other nodes.
In this test we start migrating to the second node only after the first one finished to
reproduce the bug found in issue #4455
"""


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_migration_restart(df_factory: DflyInstanceFactory, df_seeder_factory):
    # 1. Start migration, and than restart it with another slots set
    instances, nodes = await create_cluster(
        df_factory,
        2,
        vmodule="outgoing_slot_migration=2,cluster_family=2,incoming_slot_migration=2",
    )
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    logging.debug("Start seeder")
    seeder = df_seeder_factory.create(
        keys=50_000,
        port=instances[0].port,
        cluster_mode=True,
    )
    await seeder.run(target_deviation=0.1)
    capture = await seeder.capture()

    logging.debug("Start migration")
    nodes[0].migrations.append(
        MigrationInfo(
            "127.0.0.1",
            nodes[1].instance.admin_port,
            [(random.randint(1, 8000), random.randint(8001, 16383))],
            nodes[1].id,
        )
    )
    await apply_config(nodes)

    await asyncio.sleep(random.randint(1, 10) / 5)
    logging.debug("Restart migration")
    final_migration_range = (random.randint(1, 8000), random.randint(8001, 16382))
    nodes[0].migrations[0] = MigrationInfo(
        "127.0.0.1", nodes[1].instance.admin_port, [final_migration_range], nodes[1].id
    )
    await apply_config(nodes)

    logging.debug("wait migration to finish")
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", timeout=50)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FINISHED", timeout=50)

    nodes[0].migrations = []
    nodes[0].slots = [(0, final_migration_range[0] - 1), (final_migration_range[1] + 1, 16383)]
    nodes[1].slots = [final_migration_range]
    await apply_config(nodes)

    assert await seeder.compare(capture, nodes[0].instance.port)


@pytest.mark.large
@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_cluster_migration_errors_num(df_factory: DflyInstanceFactory):
    # create cluster with several nodes and create migrations from one node to others
    # but config propagated only to source node to get errors for migrations
    # number of errors should be the same as number of target nodes
    nodes = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            vmodule="cluster_family=2,outgoing_slot_migration=2,incoming_slot_migration=2",
        )
        for i in range(3)
    ]
    df_factory.start_all(nodes)

    c_nodes = [node.client() for node in nodes]

    nodes_info = [(await create_node_info(instance)) for instance in nodes]
    nodes_info[0].slots = [(0, 16383)]
    nodes_info[1].slots = []
    nodes_info[2].slots = []

    await push_config(json.dumps(generate_config(nodes_info)), c_nodes)

    async def wait_for_errors_num(client, err_num, timeout=10):
        cluster_info = lambda: client.info("CLUSTER")

        async for info, breaker in tick_timer(cluster_info, timeout=timeout):
            with breaker:
                assert info["migration_errors_total"] == err_num

    await wait_for_errors_num(c_nodes[0], 0)

    nodes_info[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes_info[1].instance.admin_port, [(0, 100)], nodes_info[1].id)
    )

    await push_config(json.dumps(generate_config(nodes_info)), [c_nodes[0]])

    # the error will be reported after 30 seconds, because config is missing for target node
    await wait_for_errors_num(c_nodes[0], 1, timeout=40)
    # the migration process attempt to start migration in a second so we get more errors
    await wait_for_errors_num(c_nodes[0], 2)


@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_cancel_blocking_cmd_during_mygration_finalization(df_factory: DflyInstanceFactory):
    # blocking commands should be canceled during migration finalization
    instances = [df_factory.create(port=next(next_port)) for i in range(2)]
    df_factory.start_all(instances)

    c_nodes = [instance.client() for instance in instances]

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config_via_client(nodes)

    logging.debug("Start blpop task")
    blpop_task = asyncio.create_task(c_nodes[0].blpop("list", 0))

    await asyncio.sleep(0.5)

    assert not blpop_task.done()

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.port, [(0, 16383)], nodes[1].id)
    )
    await apply_config_via_client(nodes)

    await wait_for_status(nodes[0].client, nodes[1].id, "FINISHED")

    with pytest.raises(aioredis.ResponseError):
        await blpop_task

    assert await c_nodes[1].type("list") == "none"

    nodes[0].migrations = []
    nodes[0].slots = []
    nodes[1].slots = [(0, 16383)]

    logging.debug("remove finished migrations")
    await apply_config_via_client(nodes)

    assert await c_nodes[1].type("list") == "none"


@dfly_args({"cluster_mode": "yes"})
async def test_slot_migration_oom(df_factory):
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            proactor_threads=4,
            maxmemory="1024MB",
        ),
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            proactor_threads=2,
            maxmemory="512MB",
        ),
    ]

    df_factory.start_all(instances)

    nodes = [(await create_node_info(instance)) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []

    await apply_config(nodes)

    await nodes[0].client.execute_command("DEBUG POPULATE 100 test 10000000")

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )

    logging.info("Start migration")
    await apply_config(nodes)

    # Wait for FATAL status
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FATAL", 300)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "FATAL")

    # There's a rare timing issue if we don't wait here. Status can be set to FATAL
    # but error message is not still set for slot migration.
    await asyncio.sleep(1)

    # Node_0 slot-migration-status
    status = await nodes[0].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[1].id
    )
    # Direction
    assert status[0][0] == "out"
    # Error message
    assert status[0][4] == "Cannot allocate memory: INCOMING_MIGRATION_OOM"

    # Node_1 slot-migration-status
    status = await nodes[1].admin_client.execute_command(
        "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[0].id
    )
    # Direction
    assert status[0][0] == "in"
    # Error message
    assert status[0][4] == "INCOMING_MIGRATION_OOM"


@dfly_args({"cluster_mode": "yes"})
async def test_slot_migration_oom_replica_rollback(df_factory):
    """
    Regression test: when incoming slot migration fails with OOM, the target master rolls back the
    migrated keys via DeleteSlots, but that deletion must also be propagated to the target's replica
    via WriteFlushSlotsToJournal. Without the journal write, the replica retains the migrated keys
    while the master has already deleted them, causing master/replica divergence.

    After OOM rollback both target_master and target_replica must have 0 keys.
    """
    source = df_factory.create(
        port=next(next_port),
        admin_port=next(next_port),
        proactor_threads=4,
        maxmemory="1024MB",
    )
    target_master = df_factory.create(
        port=next(next_port),
        admin_port=next(next_port),
        proactor_threads=2,
        maxmemory="512MB",
    )
    target_replica = df_factory.create(
        port=next(next_port),
        admin_port=next(next_port),
        proactor_threads=2,
        maxmemory="512MB",
    )

    df_factory.start_all([source, target_master, target_replica])

    source_node = await create_node_info(source)
    target_node = await create_node_info(target_master)

    source_node.slots = [(0, 16383)]
    target_node.slots = []

    # Apply initial cluster config to source and target only (replica is not a cluster member)
    await apply_config([source_node, target_node])

    # Populate source with large values that will OOM the target during migration
    await source_node.client.execute_command("DEBUG POPULATE 100 test 10000000")

    # Start replication: target_replica follows target_master before migration begins
    c_replica_admin = target_replica.admin_client()
    await c_replica_admin.execute_command(f"replicaof localhost {target_master.port}")
    await wait_available_async(c_replica_admin)

    # Kick off migration from source -> target (expects OOM on target)
    source_node.migrations.append(
        MigrationInfo("127.0.0.1", target_master.admin_port, [(0, 16383)], target_node.id)
    )

    logging.info("Starting migration (expect OOM on target)")
    await apply_config([source_node, target_node])

    # Wait for both sides to reach FATAL
    await wait_for_status(source_node.admin_client, target_node.id, "FATAL", 300)
    await wait_for_status(target_node.admin_client, source_node.id, "FATAL")

    # Eventually, after the source's retry + second INIT, both nodes flush their slots and DBSIZE reaches 0.
    # We can't rely that the offsets are equal here because the slots might have not yet been flushed.
    @assert_eventually(timeout=5)
    async def rollback_finished():
        master_keys, replica_keys = await asyncio.gather(
            target_node.admin_client.execute_command("DBSIZE"),
            c_replica_admin.execute_command("DBSIZE"),
        )

        logging.info("target_master DBSIZE=%d, target_replica DBSIZE=%d", master_keys, replica_keys)

        assert master_keys == 0, f"target_master still has {master_keys} keys after OOM rollback"
        assert (
            replica_keys == 0
        ), f"target_replica has {replica_keys} keys but master has 0 - replica was not rolled back"

    await rollback_finished()
