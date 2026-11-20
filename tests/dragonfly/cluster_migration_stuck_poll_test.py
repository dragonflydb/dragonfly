import asyncio
import logging

from . import dfly_args
from .cluster_test_utils import (
    MigrationInfo,
    apply_config,
    create_node_info,
    next_port,
    wait_for_status,
)
from .instance import DflyInstanceFactory


# Repro: a v1 pipelined SET, squashed into SingleHopAsync (tiering on), stalls on the migration
# journal while holding running_tx_. An MGET on the same shard gets its poll deferred and, without
# a drain after the optimistic SingleHopAsync run, stays queued forever (tx_queue_len=1) and blocks
# the DispatchTracker during migration finalization.
@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_migration_finalize_stuck_on_deferred_poll(df_factory: DflyInstanceFactory):
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            tiered_prefix="/tmp/tiered/stuck_poll",
            enable_resp_io_loop_v2="false",
            replication_timeout=60000,
        ),
        df_factory.create(port=next(next_port), admin_port=next(next_port)),
    ]
    df_factory.start_all(instances)
    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    await apply_config(nodes)

    await nodes[0].client.execute_command("DEBUG POPULATE 200000 key 1000")

    # The MGET must share the SETs' shard but not their hashtag lock, so later SETs don't poll.
    async def shard_of(tag):
        await nodes[0].client.set(f"{{{tag}}}probe", "1")
        obj = await nodes[0].client.execute_command("DEBUG", "OBJECT", f"{{{tag}}}probe")
        return obj.split("shard:")[1].split()[0]

    set_shard = await shard_of("t")
    mget_tag = "u0"
    while await shard_of(mget_tag) != set_shard:
        mget_tag += "0"

    nodes[0].migrations.append(
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 16383)], nodes[1].id)
    )
    await apply_config(nodes)
    await wait_for_status(nodes[1].admin_client, nodes[0].id, "SYNC", 100)

    # Stop the target from reading; the pipelined 1MB SETs then fill the migration stream until
    # one of them stalls on the journal inside its squashed hop.
    await nodes[1].client.execute_command("DEBUG MIGRATION PAUSE")

    writer = instances[0].client()
    pipe = writer.pipeline(transaction=False)
    for i in range(50):
        pipe.set(f"{{t}}k{i}", "x" * 1_000_000)
    set_task = asyncio.create_task(pipe.execute())
    await asyncio.sleep(2)
    assert not set_task.done(), "pipelined SETs did not stall on the migration journal"

    reader = instances[0].client()
    mget_task = asyncio.create_task(reader.mget(f"{{{mget_tag}}}a", f"{{{mget_tag}}}b"))
    await asyncio.sleep(1)

    await nodes[1].client.execute_command("DEBUG MIGRATION RESUME")
    await asyncio.wait_for(set_task, 60)

    done, _ = await asyncio.wait([mget_task], timeout=10)
    stuck = not done
    info = await nodes[0].admin_client.info("TRANSACTION")
    logging.info(f"MGET stuck: {stuck}, tx_queue_len: {info['tx_queue_len']}")

    if stuck:
        # Let finalization run into the stranded MGET: its pause can't drain the dispatch tracker.
        await asyncio.sleep(20)
        status = await nodes[0].admin_client.execute_command(
            "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[1].id
        )
        logging.info(f"Migration status while MGET stranded: {status}")

        # A conflicting write from the (unpaused) admin port queues on the shard and polls it.
        await nodes[0].admin_client.set("{t}a", "1")
        await asyncio.wait_for(mget_task, 10)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)

    for instance in instances:
        instance.stop()
    failed_pauses = instances[0].find_in_logs("Couldn't wait for commands to finish dispatching")
    logging.info(f"Finalization pause timeouts: {len(failed_pauses)}")

    assert not stuck, (
        f"MGET was stranded after the stall cleared (tx_queue_len={info['tx_queue_len']}, "
        f"{len(failed_pauses)} finalization pause timeouts)"
    )


# Same strand, but formed inside the DispatchTracker wait. Two migrations run in parallel from one
# source: B's target is paused to stall the source journal (a slow target), while A finalizes and
# pauses clients. B is resumed during A's tracker wait; the stalled SET batch finishes and, without
# a drain after its optimistic SingleHopAsync run, the MGET queued behind it is never polled again.
@dfly_args({"proactor_threads": 2, "cluster_mode": "yes"})
async def test_migration_finalize_strands_tx_during_pause(df_factory: DflyInstanceFactory):
    # Source: tiering + v1 route pipelined commands through SingleHopAsync. A 5s tracker wait
    # leaves time to act, and a long replication_timeout keeps the stalled stream alive.
    instances = [
        df_factory.create(
            port=next(next_port),
            admin_port=next(next_port),
            tiered_prefix="/tmp/tiered/stuck_poll_pause",
            enable_resp_io_loop_v2="false",
            replication_timeout=60000,
            pause_wait_timeout=5,
        ),
        df_factory.create(port=next(next_port), admin_port=next(next_port)),
        df_factory.create(port=next(next_port), admin_port=next(next_port)),
    ]
    df_factory.start_all(instances)
    nodes = [await create_node_info(instance) for instance in instances]
    nodes[0].slots = [(0, 16383)]
    await apply_config(nodes)

    # Enough data that migration A is still syncing while the stall is set up.
    await nodes[0].client.execute_command("DEBUG POPULATE 400000 key 1000")

    async def shard_of(tag):
        await nodes[0].client.set(f"{{{tag}}}probe", "1")
        obj = await nodes[0].client.execute_command("DEBUG", "OBJECT", f"{{{tag}}}probe")
        return obj.split("shard:")[1].split()[0]

    # MGET shares the SETs' shard but not their hashtag lock, so later SETs don't queue behind it
    # and poll the shard (which would run the MGET and hide the bug).
    set_shard = await shard_of("t")
    tags = {f"u{i}": await shard_of(f"u{i}") for i in range(32)}
    mget_tag = next(tag for tag, shard in tags.items() if shard == set_shard)

    # A (slots 0-8191) -> node 1 finalizes; B (slots 8192-16383) -> node 2 provides the stall.
    nodes[0].migrations = [
        MigrationInfo("127.0.0.1", nodes[1].instance.admin_port, [(0, 8191)], nodes[1].id),
        MigrationInfo("127.0.0.1", nodes[2].instance.admin_port, [(8192, 16383)], nodes[2].id),
    ]
    await apply_config(nodes)
    await wait_for_status(nodes[2].admin_client, nodes[0].id, "SYNC", 100)

    # Node 2 stops reading, so B's stream on the source fills up. The journal throttles on every
    # consumer, so all journaled writes on the source now block; A's sync keeps going.
    await nodes[2].client.execute_command("DEBUG MIGRATION PAUSE")

    # v1 squashes the pipeline; each SET runs inline via SingleHopAsync holding running_tx_, and
    # one of them blocks on the full journal.
    writer = instances[0].client()
    pipe = writer.pipeline(transaction=False)
    for i in range(50):
        pipe.set(f"{{t}}k{i}", "x" * 1_000_000)
    set_task = asyncio.create_task(pipe.execute())
    await asyncio.sleep(2)
    assert not set_task.done(), "pipelined SETs did not stall on the migration journal"

    # The MGET is queued on the shard while running_tx_ is held, so its poll is only deferred.
    reader = instances[0].client()
    mget_task = asyncio.create_task(reader.mget(f"{{{mget_tag}}}a", f"{{{mget_tag}}}b"))
    await asyncio.sleep(1)
    assert not mget_task.done(), "MGET was not held behind the stalled SET"

    # Wait until migration A finalizes and pauses clients (PING stops being answered). PING
    # never touches a shard, so the probe can't poll it and free the MGET by accident.
    probe = instances[0].client()
    deadline = asyncio.get_running_loop().time() + 120
    while True:
        try:
            await asyncio.wait_for(probe.ping(), 0.2)
        except asyncio.TimeoutError:
            break
        status = await nodes[0].admin_client.execute_command(
            "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[1].id
        )
        assert asyncio.get_running_loop().time() < deadline, f"A never paused clients: {status}"
        await asyncio.sleep(0.1)
    logging.info("Clients paused by migration A finalization")

    # Clear the stall inside A's DispatchTracker wait. The SET batch was admitted before the pause,
    # so it runs to completion; with the fix its first finished SET drains the MGET's poll.
    await nodes[2].client.execute_command("DEBUG MIGRATION RESUME")
    await asyncio.wait_for(set_task, 60)
    done, _ = await asyncio.wait([mget_task], timeout=3)
    stuck = not done
    info = await nodes[0].admin_client.info("TRANSACTION")
    logging.info(f"MGET stuck: {stuck}, tx_queue_len: {info['tx_queue_len']}")

    if stuck:
        # Let A's finalization attempts keep failing on the stranded MGET (bc=1 each time).
        await asyncio.sleep(10)
        status = await nodes[0].admin_client.execute_command(
            "DFLYCLUSTER", "SLOT-MIGRATION-STATUS", nodes[1].id
        )
        logging.info(f"Migration A status while MGET stranded: {status}")

        # A conflicting write via the (unpaused) admin port queues on the shard and polls it.
        await nodes[0].admin_client.set(f"{{{mget_tag}}}a", "1")
        await asyncio.wait_for(mget_task, 30)

    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED", 300)
    await wait_for_status(nodes[0].admin_client, nodes[2].id, "FINISHED", 300)

    for instance in instances:
        instance.stop()
    failed_pauses = instances[0].find_in_logs("Couldn't wait for commands to finish dispatching")
    logging.info(f"Finalization pause timeouts: {len(failed_pauses)}")

    assert not stuck, (
        f"MGET was stranded inside the finalization pause (tx_queue_len={info['tx_queue_len']}, "
        f"{len(failed_pauses)} finalization pause timeouts)"
    )
