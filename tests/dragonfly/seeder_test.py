import asyncio
import async_timeout
import string
from redis import asyncio as aioredis
from . import dfly_args
from .seeder import Seeder, StaticSeeder
from .instance import DflyInstanceFactory, DflyInstance
from .utility import *


@dfly_args({"proactor_threads": 4})
async def test_static_seeder(async_client: aioredis.Redis):
    s = StaticSeeder(key_target=10_000, data_size=100)
    await s.run(async_client)

    assert abs(await async_client.dbsize() - 10_000) <= 50


@dfly_args({"proactor_threads": 4})
async def test_static_collection_size(async_client: aioredis.Redis):
    async def check_list():
        keys = await async_client.keys()
        for key in keys:
            assert await async_client.llen(key) == 1
            assert len(await async_client.lpop(key)) == 10_000

    s = StaticSeeder(
        key_target=10, data_size=10_000, variance=1, samples=1, collection_size=1, types=["LIST"]
    )
    await s.run(async_client)
    await check_list()

    await async_client.flushall()

    s = Seeder(
        units=1,
        key_target=10,
        data_size=10_000,
        collection_size=1,
        types=["LIST"],
        huge_value_target=0,
        huge_value_size=0,
    )
    await s.run(async_client)


@dfly_args({"proactor_threads": 4})
async def test_seeder_key_target(async_client: aioredis.Redis):
    """Ensure seeder reaches its key targets"""
    s = Seeder(units=len(Seeder.DEFAULT_TYPES) * 2, key_target=5000)

    # Ensure tests are not reasonably slow
    async with async_timeout.timeout(1 + 4):
        # Fill with 5k keys, 1% derivation = 50
        await s.run(async_client, target_deviation=0.01)
        assert abs(await async_client.dbsize() - 5000) <= 50

        # Run 1k ops, ensure key balance stays the "more or less" the same
        await s.run(async_client, target_ops=1000)
        assert abs(await async_client.dbsize() - 5000) <= 100

        # Run one second until stopped
        task = asyncio.create_task(s.run(async_client))
        await asyncio.sleep(1.0)
        await s.stop(async_client)
        await task

        # Change key target, 100 is actual minimum because "math breaks"
        s.change_key_target(0)
        await s.run(async_client, target_deviation=0.5)  # don't set low precision with low values
        assert await async_client.dbsize() < 200

        # Get cmdstat calls
        info = await async_client.info("ALL")
        calls = {
            k.split("_")[1]: v["calls"]
            for k, v in info.items()
            if k.startswith("cmdstat_") and v["calls"] > 50
        }
        assert len(calls) > 15  # we use at least 15 different commands


@dfly_args({"proactor_threads": 4})
async def test_seeder_capture(async_client: aioredis.Redis):
    """Ensure same data produces same state hashes"""

    async def set_data():
        p = async_client.pipeline()
        p.mset(mapping={f"string{i}": f"{i}" for i in range(100)})
        p.lpush("list1", *list(string.ascii_letters))
        p.sadd("set1", *list(string.ascii_letters))
        p.hset("hash1", mapping={f"{i}": l for i, l in enumerate(string.ascii_letters)})
        p.zadd("zset1", mapping={l: i for i, l in enumerate(string.ascii_letters)})
        await p.execute()

    # Capture with filled data
    await set_data()
    capture = await Seeder.capture(async_client)

    # Check hashes are 0 without data
    await async_client.flushall()
    assert all(h == 0 for h in (await Seeder.capture(async_client)))

    # Check setting the same data results in same hashes
    await set_data()
    assert capture == await Seeder.capture(async_client)

    # Check changing the data gives different hahses
    await async_client.lpush("list1", "NEW")
    assert capture != await Seeder.capture(async_client)

    # Undo our change
    await async_client.lpop("list1")
    assert capture == await Seeder.capture(async_client)

    # Do another change
    await async_client.spop("set1")
    assert capture != await Seeder.capture(async_client)


@pytest.mark.asyncio
@dfly_args({"proactor_threads": 2})
async def test_seeder_fake_redis(
    df_factory: DflyInstanceFactory, df_seeder_factory: DflySeederFactory
):
    instance = df_factory.create()
    df_factory.start_all([instance])

    seeder = df_seeder_factory.create(
        keys=100, port=instance.port, unsupported_types=[ValueType.JSON], mirror_to_fake_redis=True
    )

    await seeder.run(target_ops=5_000)

    capture = await seeder.capture_fake_redis()

    assert await seeder.compare(capture, instance.port)
