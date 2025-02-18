import logging
import pytest
import redis
import asyncio
from redis import asyncio as aioredis

from . import dfly_multi_test_args, dfly_args
from .instance import DflyInstance, DflyStartException
from .utility import batch_fill_data, gen_test_data, EnvironCntx
from .seeder import StaticSeeder


@dfly_multi_test_args({"keys_output_limit": 512}, {"keys_output_limit": 1024})
class TestKeys:
    async def test_max_keys(self, async_client: aioredis.Redis, df_server):
        max_keys = df_server["keys_output_limit"]
        pipe = async_client.pipeline()
        batch_fill_data(pipe, gen_test_data(max_keys * 3))
        await pipe.execute()
        keys = await async_client.keys()
        assert len(keys) in range(max_keys, max_keys + 512)


@pytest.fixture(scope="function")
def export_dfly_password() -> str:
    pwd = "flypwd"
    with EnvironCntx(DFLY_requirepass=pwd):
        yield pwd


async def test_password(df_factory, export_dfly_password):
    with df_factory.create() as dfly:
        # Expect password form environment variable
        with pytest.raises(redis.exceptions.AuthenticationError):
            async with aioredis.Redis(port=dfly.port) as client:
                await client.ping()
        async with aioredis.Redis(password=export_dfly_password, port=dfly.port) as client:
            await client.ping()

    # --requirepass should take precedence over environment variable
    requirepass = "requirepass"
    with df_factory.create(requirepass=requirepass) as dfly:
        # Expect password form flag
        with pytest.raises(redis.exceptions.AuthenticationError):
            async with aioredis.Redis(port=dfly.port, password=export_dfly_password) as client:
                await client.ping()
        async with aioredis.Redis(password=requirepass, port=dfly.port) as client:
            await client.ping()


"""
Make sure that multi-hop transactions can't run OOO.
"""

MULTI_HOPS = """
for i = 0, ARGV[1] do
  redis.call('INCR', KEYS[1])
end
"""


@dfly_args({"proactor_threads": 1})
async def test_txq_ooo(async_client: aioredis.Redis, df_server):
    async def task1(k, h):
        c = aioredis.Redis(port=df_server.port)
        for _ in range(100):
            await c.eval(MULTI_HOPS, 1, k, h)

    async def task2(k, n):
        c = aioredis.Redis(port=df_server.port)
        for _ in range(100):
            pipe = c.pipeline(transaction=False)
            pipe.lpush(k, 1)
            for _ in range(n):
                pipe.blpop(k, 0.001)
            await pipe.execute()

    await asyncio.gather(
        task1("i1", 2), task1("i2", 3), task2("l1", 2), task2("l1", 2), task2("l1", 5)
    )


@dfly_args({"proactor_threads": 2, "num_shards": 2})
async def test_blocking_multiple_dbs(async_client: aioredis.Redis, df_server: DflyInstance):
    active = True

    # A task to trigger the flow that eventually looses a transaction
    # blmove is used to trigger a global deadlock, but we could use any
    # command - the effect would be - a deadlocking locally that connection
    async def blmove_task_loose(num):
        async def run(id):
            c = df_server.client()
            await c.lpush(f"key{id}", "val")
            while active:
                await c.blmove(f"key{id}", f"key{id}", 0, "LEFT", "LEFT")
                await asyncio.sleep(0.01)

        tasks = []
        for i in range(num):
            tasks.append(run(i))

        await asyncio.gather(*tasks)

    # A task that creates continuation_trans_ by constantly timing out on
    # an empty set. We could probably use any 2-hop operation like rename.
    async def task_blocking(num):
        async def block(id):
            c = df_server.client()
            while active:
                await c.blmove(f"{{{id}}}from", f"{{{id}}}to", 0.1, "LEFT", "LEFT")

        tasks = []
        for i in range(num):
            tasks.append(block(i))
        await asyncio.gather(*tasks)


    # produce is constantly waking up consumers. It is used to trigger the
    # flow that creates wake ups on a differrent database in the
    # middle of continuation transaction.
    async def tasks_produce(num, iters):
        LPUSH_SCRIPT = """
            redis.call('LPUSH', KEYS[1], "val")
        """
        async def produce(id):
            c = df_server.client(db=1)  # important to be on a different db
            for i in range(iters):
                # Must be a lua script and not multi-exec for some reason.
                await c.eval(LPUSH_SCRIPT, 1,  f"list{{{id}}}")

        tasks = []
        for i in range(num):
            task = asyncio.create_task(produce(i))
            tasks.append(task)

        await asyncio.gather(*tasks)
        logging.info("Finished producing")

    # works with producer to constantly block and wake up
    async def tasks_consume(num, iters):
        async def drain(id, iters):
            client = df_server.client(db=1)
            for _ in range(iters):
                await client.blmove(f"list{{{id}}}", f"sink{{{id}}}", 0, "LEFT", "LEFT")

        tasks = []
        for i in range(num):
            task = asyncio.create_task(drain(i, iters))
            tasks.append(task)

        await asyncio.gather(*tasks)
        logging.info("Finished consuming")


    num_keys = 32
    num_iters = 200
    async_task1 = asyncio.create_task(blmove_task_loose(num_keys))
    async_task2 = asyncio.create_task(task_blocking(num_keys))
    logging.info("Starting tasks")
    await asyncio.gather(
        tasks_consume(num_keys, num_iters),
        tasks_produce(num_keys, num_iters),
    )
    logging.info("Finishing tasks")
    active = False
    await asyncio.gather(async_task1, async_task2)


async def test_arg_from_environ_overwritten_by_cli(df_factory):
    with EnvironCntx(DFLY_port="6378"):
        with df_factory.create(port=6377):
            client = aioredis.Redis(port=6377)
            await client.ping()


async def test_arg_from_environ(df_factory):
    with EnvironCntx(DFLY_requirepass="pass"):
        with df_factory.create() as dfly:
            # Expect password from environment variable
            with pytest.raises(redis.exceptions.AuthenticationError):
                client = aioredis.Redis(port=dfly.port)
                await client.ping()

            client = aioredis.Redis(password="pass", port=dfly.port)
            await client.ping()


async def test_unknown_dfly_env(df_factory, export_dfly_password):
    with EnvironCntx(DFLY_abcdef="xyz"):
        dfly = df_factory.create()
        with pytest.raises(DflyStartException):
            dfly.start()
        dfly.set_proc_to_none()


async def test_restricted_commands(df_factory):
    # Restrict GET and SET, then verify non-admin clients are blocked from
    # using these commands, though admin clients can use them.
    with df_factory.create(restricted_commands="get,set", admin_port=1112) as server:
        async with aioredis.Redis(port=server.port) as client:
            with pytest.raises(redis.exceptions.ResponseError):
                await client.get("foo")

            with pytest.raises(redis.exceptions.ResponseError):
                await client.set("foo", "bar")

        async with aioredis.Redis(port=server.admin_port) as admin_client:
            await admin_client.get("foo")
            await admin_client.set("foo", "bar")


@pytest.mark.asyncio
async def test_reply_guard_oom(df_factory, df_seeder_factory):
    master = df_factory.create(
        proactor_threads=1,
        cache_mode="true",
        maxmemory="256mb",
        enable_heartbeat_eviction="false",
        rss_oom_deny_ratio=2,
    )
    df_factory.start_all([master])
    c_master = master.client()
    await c_master.execute_command("DEBUG POPULATE 6000 size 40000")

    seeder = df_seeder_factory.create(
        port=master.port, keys=5000, val_size=1000, stop_on_failure=False
    )
    await seeder.run(target_deviation=0.1)

    info = await c_master.info("stats")
    assert info["evicted_keys"] > 0, "Weak testcase: policy based eviction was not triggered."


@pytest.mark.asyncio
async def test_denyoom_commands(df_factory):
    df_server = df_factory.create(proactor_threads=1, maxmemory="256mb", oom_deny_commands="get")
    df_server.start()
    client = df_server.client()
    await client.execute_command("DEBUG POPULATE 7000 size 44000")

    min_deny = 256 * 1024 * 1024  # 256mb
    info = await client.info("memory")
    print(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert info["used_memory"] > min_deny, "Weak testcase: too little used memory"

    # reject set due to oom
    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command("set x y")

    # reject get because it is set in oom_deny_commands
    with pytest.raises(redis.exceptions.ResponseError):
        await client.execute_command("get x")

    # mget should not be rejected
    await client.execute_command("mget x")


@pytest.mark.parametrize("type", ["LIST", "HASH", "SET", "ZSET", "STRING"])
@dfly_args({"proactor_threads": 4})
@pytest.mark.asyncio
async def test_rename_huge_values(df_factory, type):
    df_server = df_factory.create()
    df_server.start()
    client = df_server.client()

    logging.debug(f"Generating huge {type}")
    seeder = StaticSeeder(
        key_target=1,
        data_size=10_000_000,
        collection_size=10_000,
        variance=1,
        samples=1,
        types=[type],
    )
    await seeder.run(client)
    source_data = await StaticSeeder.capture(client)
    logging.debug(f"src {source_data}")

    # Rename multiple times to make sure the key moves between shards
    orig_name = (await client.execute_command("keys *"))[0]
    old_name = orig_name
    new_name = ""
    for i in range(10):
        new_name = f"new:{i}"
        await client.execute_command(f"rename {old_name} {new_name}")
        old_name = new_name
    await client.execute_command(f"rename {new_name} {orig_name}")
    target_data = await StaticSeeder.capture(client)

    assert source_data == target_data
