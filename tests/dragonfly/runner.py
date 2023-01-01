import asyncio
import aioredis

from utility import DflySeeder


async def main():
    c = aioredis.Redis()
    await c.flushall()

    s = DflySeeder()
    await s.run(target_deviation=0.01)

asyncio.run(main())
