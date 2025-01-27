#!/usr/bin/env python3

"""
This is the script that helped to reproduce https://github.com/dragonflydb/dragonfly/issues/150
The outcome - stalled code with all its connections deadlocked.
Reproduced only with dragonfly in release mode on multi-core machine.
"""

import asyncio
import aioredis

from loguru import logger as log
import sys
import random

connection_pool = aioredis.ConnectionPool(
    host="localhost", port=6379, db=1, decode_responses=True, max_connections=16
)


key_index = 1


async def post_to_redis(sem, db_name, index):
    global key_index
    async with sem:
        results = None
        try:
            redis_client = aioredis.Redis(connection_pool=connection_pool)
            async with redis_client.pipeline(transaction=True) as pipe:
                for i in range(1, 15):
                    pipe.hsetnx(name=f"key_{key_index}", key="name", value="bla")
                    key_index += 1
                # log.info(f"after first half {key_index}")
                for i in range(1, 15):
                    pipe.hsetnx(name=f"bla_{key_index}", key="name2", value="bla")
                    key_index += 1
                assert len(pipe.command_stack) > 0
                log.info(f"before pipe.execute {key_index}")
                results = await pipe.execute()
                log.info(f"after pipe.execute {key_index}")
        finally:
            # log.info(f"before close {index}")
            await redis_client.aclose()
            # log.info(f"after close {index} {len(results)}")


async def do_concurrent(db_name):
    tasks = []
    sem = asyncio.Semaphore(10)
    for i in range(1, 3000):
        tasks.append(post_to_redis(sem, db_name, i))
    res = await asyncio.gather(*tasks)


if __name__ == "__main__":
    log.remove()
    log.add(sys.stdout, enqueue=True, level="INFO")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_concurrent("my_db"))
