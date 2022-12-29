import asyncio
import aioredis
from utility import *


async def main():
    client = aioredis.Redis()
    await client.flushdb()

    seeder = DflySeeder(port=6379, keys=20_000)
    await seeder.run(target_deviation=0.05)

    await seeder.run(target_times=50)

asyncio.run(main())
