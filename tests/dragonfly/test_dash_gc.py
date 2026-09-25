import asyncio
import logging

import pytest
from redis import asyncio as aioredis

from . import dfly_args
from .seeder import Seeder


def _compact_table_stats(flat_reply):
    """DEBUG COMPACT-TABLE replies with a RESP map, which arrives as a flat
    [k1, v1, k2, v2, ...] list over RESP2."""
    return dict(zip(flat_reply[::2], flat_reply[1::2]))


async def _make_sparse_table(client):
    """Grow the dash table via DEBUG POPULATE, then delete those keys to leave sparse segments."""
    populate_prefix = "gc-init-"
    await client.execute_command("DEBUG", "POPULATE", 100_000, populate_prefix, 50)

    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor, match=f"{populate_prefix}*", count=1000)
        if keys:
            await client.delete(*keys)
        if cursor == 0:
            break


@dfly_args({"proactor_threads": 2, "maxmemory": "1G"})
async def test_gc_merges_segments_and_shrinks_capacity(async_client: aioredis.Redis):
    value_size = 50
    target_keys = 10_000
    value = "x" * value_size

    batch_size = 100
    for batch_start in range(0, target_keys, batch_size):
        batch_end = min(batch_start + batch_size, target_keys)
        pipeline = async_client.pipeline()
        for i in range(batch_start, batch_end):
            pipeline.set(f"key{i}", value)
        await pipeline.execute()

    await asyncio.sleep(0.5)

    stats_before = await async_client.info("MEMORY")

    # Delete 90% of keys to create very sparse segments
    keys_to_delete = [f"key{i}" for i in range(target_keys) if i % 10 != 0]
    keys_left = [f"key{i}" for i in range(target_keys) if i % 10 == 0]

    for batch_start in range(0, len(keys_to_delete), 1000):
        await async_client.delete(*keys_to_delete[batch_start : batch_start + 1000])

    # Run GC with aggressive threshold to trigger merges
    compact_stats = _compact_table_stats(
        await async_client.execute_command("DEBUG", "COMPACT-TABLE", "0.5")
    )

    stats_after = await async_client.info("MEMORY")
    assert compact_stats["merged"] > 0
    # Fewer segments means fewer buckets, so the table's total capacity must shrink
    assert stats_after["prime_capacity"] < stats_before["prime_capacity"], (
        f"Table capacity should shrink after GC: before={stats_before['prime_capacity']}, "
        f"after={stats_after['prime_capacity']}"
    )

    logging.info(
        f"COMPACT-TABLE stats: {compact_stats}, "
        f"capacity {stats_before['prime_capacity']} -> {stats_after['prime_capacity']}"
    )

    for key in keys_left:
        res = await async_client.get(key)
        assert res == value


# 500 is the default; 1 yields on every container element, so commands get preempted
# mid-iteration while COMPACT-TABLE relocates entries.
@pytest.mark.parametrize("yield_interval_usec", [500, 1])
async def test_gc_concurrent_with_seeding(df_factory, yield_interval_usec):
    """
    Verify COMPACT-TABLE running concurrently with data insertion doesn't corrupt seeded data.

    a) Grow the dash table via DEBUG POPULATE with a prefix
    b) Delete all populated keys to create sparse segments
    c) Run DEBUG COMPACT-TABLE concurrently with Seeder
    d) Assert all data seeded by Seeder exists in the dash table
    """
    instance = df_factory.create(
        proactor_threads=1,
        maxmemory="2G",
        container_iteration_yield_interval_usec=yield_interval_usec,
    )
    instance.start()
    async_client = instance.client()

    # a) + b) Grow the dash table, then delete everything to leave the segments sparse
    await _make_sparse_table(async_client)
    assert await async_client.dbsize() == 0

    # c) Run COMPACT-TABLE concurrently with Seeder so GC reclaims sparse segments
    #    while new data is being written
    key_target = 5_000
    seeder = Seeder(key_target=key_target, data_size=100)

    async def run_gc():
        for _ in range(10):
            await async_client.execute_command("DEBUG", "COMPACT-TABLE", "0.5")
            await asyncio.sleep(0.05)

    await asyncio.gather(
        seeder.run(async_client, target_deviation=0.05),
        run_gc(),
    )

    # d) Capture a reference snapshot of the data seeder wrote, then run GC again
    #    and verify the full dataset is unchanged (no corruption or partial loss).
    capture_before = await Seeder.capture(async_client)
    assert all(h != 0 for h in capture_before), "Seeder should have written data for all types"

    for _ in range(5):
        await async_client.execute_command("DEBUG", "COMPACT-TABLE", "0.5")
        await asyncio.sleep(0.05)

    capture_after = await Seeder.capture(async_client)
    assert (
        capture_before == capture_after
    ), "Data should be identical after GC: seeder dataset must survive concurrent GC runs"


# Each command iterates a container (may yield) and then reuses the entry while COMPACT-TABLE
# relocates it. ZRANDMEMBER with count * log2(size) < size takes the per-pick path.
@pytest.mark.parametrize(
    "cmd",
    [
        lambda p, k: p.zrandmember(f"z{k}", 5),
        lambda p, k: p.sunion(f"s{k}", f"s{k + 1}"),
        lambda p, k: p.sinter(f"s{k}"),
        lambda p, k: p.sdiff(f"s{k}", "missing"),
    ],
    ids=["zrandmember", "sunion", "sinter", "sdiff"],
)
async def test_gc_concurrent_with_reads(df_factory, cmd):
    instance = df_factory.create(
        proactor_threads=1, maxmemory="2G", container_iteration_yield_interval_usec=1
    )
    instance.start()
    client = instance.client()

    await _make_sparse_table(client)

    # More than 128 members, so zsets are skiplists and sets are dense.
    num_keys = 200
    members = [f"m{i}" for i in range(300)]
    pipe = client.pipeline(transaction=False)
    for k in range(num_keys + 1):
        pipe.zadd(f"z{k}", {m: i for i, m in enumerate(members)})
        pipe.sadd(f"s{k}", *members)
    await pipe.execute()

    gc_done = asyncio.Event()

    async def run_gc():
        # Re-sparsify each round, or later rounds have nothing to merge. Not DEBUG POPULATE: its
        # stub transactions skip scheduling and collide with the suspended reads.
        churn = [f"churn-{i}" for i in range(30_000)]
        for _ in range(5):
            for i in range(0, len(churn), 1000):
                await client.mset({k: "x" for k in churn[i : i + 1000]})
            for i in range(0, len(churn), 1000):
                await client.delete(*churn[i : i + 1000])
            await client.execute_command("DEBUG", "COMPACT-TABLE", "0.5")
        gc_done.set()

    async def run_reads():
        while not gc_done.is_set():
            pipe = client.pipeline(transaction=False)
            for k in range(num_keys):
                cmd(pipe, k)
            for res in await pipe.execute():
                assert res and set(res) <= set(members)

    await asyncio.gather(run_gc(), run_reads())
