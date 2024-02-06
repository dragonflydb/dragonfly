import asyncio
import async_timeout
import string
from . import dfly_args
from redis import asyncio as aioredis
from .seeder import Seeder


@dfly_args({"proactor_threads": 4})
async def test_seeder_key_target(async_client: aioredis.Redis):
    """Ensure seeder reaches its key targets"""
    s = Seeder(units=5, key_target=5000)

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

        # Change key target, 10 is actual minimum because "math breaks"
        s.change_key_target(0)
        await s.run(async_client, target_deviation=0.01)
        assert await async_client.dbsize() < 50


@dfly_args({"proactor_threads": 4})
async def test_seeder_capture(async_client: aioredis.Redis):
    """Ensure same data produces same state hashes"""

    async def set_data():
        p = async_client.pipeline()
        p.mset(mapping={f"string{i}": f"{i}" for i in range(100)})
        p.lpush("list1", *(list(string.ascii_letters) + list(string.ascii_letters)))
        p.sadd("set1", *(list(string.ascii_letters)))
        await p.execute()

    await set_data()
    c1 = await Seeder.capture(async_client)

    await async_client.flushall()
    assert all(h == 0 for h in (await Seeder.capture(async_client)))

    await set_data()
    c2 = await Seeder.capture(async_client)

    assert c1 == c2
