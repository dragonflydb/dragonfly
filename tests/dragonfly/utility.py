import redis
import aioredis
import itertools
import time
import asyncio


def grouper(n, iterable):
    """Transform iterable into iterator of chunks of size n"""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


BATCH_SIZE = 100


def gen_test_data(n, start=0, seed=None):
    for i in range(start, n):
        yield "k-"+str(i), "v-"+str(i) + ("-"+str(seed) if seed else "")


def batch_fill_data(client: redis.Redis, gen):
    for group in grouper(BATCH_SIZE, gen):
        client.mset({k: v for k, v, in group})


async def wait_available_async(client: aioredis.Redis):
    its = 0
    while True:
        try:
            await client.get('key')
            print("wait_available iterations:", its)
            return
        except aioredis.ResponseError as e:
            assert "Can not execute during LOADING" in str(e)

        await asyncio.sleep(0.01)
        its += 1
