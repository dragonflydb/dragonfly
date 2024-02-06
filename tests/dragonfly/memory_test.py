import pytest
from redis import asyncio as aioredis
from .utility import *


@pytest.mark.asyncio
@pytest.mark.slow
@pytest.mark.parametrize(
    "type, val_size",
    [
        (ValueType.JSON, 1_000),
        (ValueType.SET, 1_000),
        (ValueType.HSET, 1_000),
        (ValueType.ZSET, 1_000),
        (ValueType.LIST, 2_000),
        (ValueType.STRING, 1_000),
    ],
)
async def test_acl_setuser(df_local_factory, df_seeder_factory, type, val_size):
    # Create a Dragonfly and fill it up with `type` until it reaches `min_rss`, then make sure that
    # the gap between used_memory and rss is no more than `max_unaccounted`.
    min_rss = 3 * 1024 * 1024 * 1024  # 2gb
    max_unaccounted = 200 * 1024 * 1024  # 200mb

    master = df_local_factory.create(proactor_threads=1)
    df_local_factory.start_all([master])
    c_master = master.client()

    unsupported_types = [v for v in ValueType if not v == type]
    seeder = df_seeder_factory.create(
        port=master.port,
        keys=10_000_000,  # Overkill because we stop when we reach min_rss
        val_size=val_size,
        batch_size=10_000,
        unsupported_types=unsupported_types,
    )
    seed_task = asyncio.create_task(seeder.run(target_deviation=0.1))
    while True:
        await asyncio.sleep(1)
        rss = (await c_master.info("memory"))["used_memory_rss"]
        if rss > min_rss:
            seed_task.cancel()
            break

    info = await c_master.info("memory")
    print(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert info["used_memory_rss"] - info["used_memory"] < max_unaccounted

    await disconnect_clients(c_master)
