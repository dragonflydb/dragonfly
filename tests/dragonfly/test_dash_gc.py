import asyncio
from redis import asyncio as aioredis
from . import dfly_args
from .seeder import Seeder
import logging


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
    segments_merged = await async_client.execute_command("DEBUG", "DASH_GC", "0.5")

    stats_after = await async_client.info("MEMORY")
    assert segments_merged > 0
    # Fewer segments means fewer buckets, so the table's total capacity must shrink
    assert stats_after["prime_capacity"] < stats_before["prime_capacity"], (
        f"Table capacity should shrink after GC: before={stats_before['prime_capacity']}, "
        f"after={stats_after['prime_capacity']}"
    )

    logging.info(
        f"DASH_GC merged {segments_merged} segments, "
        f"capacity {stats_before['prime_capacity']} -> {stats_after['prime_capacity']}"
    )

    for key in keys_left:
        res = await async_client.get(key)
        assert res == value


@dfly_args({"proactor_threads": 1, "maxmemory": "2G"})
async def test_gc_concurrent_with_seeding(async_client: aioredis.Redis):
    """
    Verify DASH_GC running concurrently with data insertion doesn't corrupt seeded data.

    a) Grow the dash table via DEBUG POPULATE with a prefix
    b) Delete all populated keys to create sparse segments
    c) Run DEBUG DASH_GC concurrently with SeederV2 (Seeder)
    d) Assert all data seeded by Seeder exists in the dash table
    """
    # a) Grow the dash table by seeding a large number of keys with a prefix
    populate_prefix = "gc-init-"
    await async_client.execute_command("DEBUG", "POPULATE", 100_000, populate_prefix, 50)

    # b) Delete all keys with the populate prefix to leave the segments sparse
    cursor = 0
    while True:
        cursor, keys = await async_client.scan(cursor, match=f"{populate_prefix}*", count=1000)
        if keys:
            await async_client.delete(*keys)
        if cursor == 0:
            break

    assert await async_client.dbsize() == 0

    # c) Run DASH_GC concurrently with Seeder so GC reclaims sparse segments
    #    while new data is being written
    key_target = 5_000
    seeder = Seeder(key_target=key_target, data_size=100)

    async def run_gc():
        for _ in range(10):
            await async_client.execute_command("DEBUG", "DASH_GC", "0.5")
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
        await async_client.execute_command("DEBUG", "DASH_GC", "0.5")
        await asyncio.sleep(0.05)

    capture_after = await Seeder.capture(async_client)
    assert (
        capture_before == capture_after
    ), "Data should be identical after GC: seeder dataset must survive concurrent GC runs"
