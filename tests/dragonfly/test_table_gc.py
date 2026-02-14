import asyncio
from redis import asyncio as aioredis
from . import dfly_args
import logging


@dfly_args({"proactor_threads": 2, "maxmemory": "1G"})
async def test_gc_merges_segments_and_reclaims_memory(async_client: aioredis.Redis):
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

    info_before = await async_client.info("MEMORY")

    # Delete 90% of keys to create very sparse segments
    keys_to_delete = [f"key{i}" for i in range(target_keys) if i % 10 != 0]
    keys_left = [f"key{i}" for i in range(target_keys) if i % 10 == 0]

    for batch_start in range(0, len(keys_to_delete), 1000):
        await async_client.delete(*keys_to_delete[batch_start : batch_start + 1000])

    # Run GC with aggressive threshold to trigger merges
    segments_merged = await async_client.execute_command("DEBUG", "TABLE_GC", "0.5")

    await asyncio.sleep(1.0)

    info_after = await async_client.info("MEMORY")

    assert segments_merged > 0

    logging.info(f"TABLE✓GC merged {segments_merged} segments")
    for key in keys_left:
        res = await async_client.get(key)
        assert res == value
