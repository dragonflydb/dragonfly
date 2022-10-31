import redis
import aioredis
import itertools


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


BATCH_SIZE = 100


def gen_test_data(n):
    for i in range(n):
        yield "k-"+str(i), "v-"+str(i)


def batch_fill_data(client: redis.Redis, gen):
    for group in grouper(BATCH_SIZE, gen):
        client.mset({k: v for k, v, in group})


async def batch_fill_data_async(client: aioredis.Redis, gen):
    for group in grouper(BATCH_SIZE, gen):
        await client.mset({k: v for k, v in group})


def as_str_val(v) -> str:
    if isinstance(v, str):
        return v
    elif isinstance(v, bytes):
        return v.decode()
    else:
        return str(v)


def batch_check_data(client: redis.Redis, gen):
    for group in grouper(BATCH_SIZE, gen):
        vals = client.mget(k for k, _ in group)
        assert all(as_str_val(vals[i]) == v for i, (_, v) in enumerate(group))


async def batch_check_data_async(client: aioredis.Redis, gen):
    for group in grouper(BATCH_SIZE, gen):
        vals = await client.mget(k for k, _ in group)
        assert all(as_str_val(vals[i]) == v for i, (_, v) in enumerate(group))
