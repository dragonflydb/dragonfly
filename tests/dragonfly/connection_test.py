import random
import logging
import string
import pytest
import asyncio
import time
import socket
from threading import Thread
import random
import ssl
from redis import asyncio as aioredis
import redis as base_redis
import hiredis
from redis.cache import CacheConfig

from redis.exceptions import ConnectionError, ResponseError

import async_timeout
from dataclasses import dataclass
from aiohttp import ClientSession

from .utility import tick_timer, assert_eventually
from . import dfly_args
from .instance import DflyInstance, DflyInstanceFactory

BASE_PORT = 1111


@dataclass(frozen=True)
class CollectedRedisMsg:
    cmd: str
    src: str = "tcp"

    @staticmethod
    def all_from_src(*args, src="tcp"):
        return [CollectedRedisMsg(arg, src) for arg in args]


class CollectingMonitor:
    """Tracks all monitor messages between start() and stop()"""

    def __init__(self, client):
        self.client = client
        self.messages = []
        self._monitor_task = None

    async def _monitor(self):
        async with self.client.monitor() as monitor:
            async for message in monitor.listen():
                self.messages.append(CollectedRedisMsg(message["command"], message["client_type"]))

    async def start(self):
        if self._monitor_task is None:
            self._monitor_task = asyncio.create_task(self._monitor())
        await asyncio.sleep(0.1)

    async def stop(self, timeout=0.1):
        if self._monitor_task:
            # Wait for Dragonfly to send all async monitor messages
            await asyncio.sleep(timeout)
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None

        def should_exclude(cmd: str):
            cmd = cmd.upper()
            return "SELECT" in cmd or "CLIENT SETINFO" in cmd

        while len(self.messages) > 0 and should_exclude(self.messages[0].cmd):
            self.messages = self.messages[1:]
        return self.messages


"""
Test MONITOR command with basic use case
"""


@dfly_args({"proactor_threads": 4})
async def test_monitor_command(async_pool):
    monitor = CollectingMonitor(aioredis.Redis(connection_pool=async_pool))
    await monitor.start()

    c = aioredis.Redis(connection_pool=async_pool)
    await c.set("a", 1)
    await c.get("a")
    await c.lpush("l", "V")
    await c.lpop("l")

    collected = await monitor.stop()
    expected = CollectedRedisMsg.all_from_src("SET a 1", "GET a", "LPUSH l V", "LPOP l")

    assert expected == collected


"""
Test MONITOR command with MULTI/EXEC transaction with squashing
"""


@dfly_args({"proactor_threads": 4, "multi_exec_squash": "true"})
async def test_monitor_command_multi(async_pool):
    monitor = CollectingMonitor(aioredis.Redis(connection_pool=async_pool))
    await monitor.start()

    c = aioredis.Redis(connection_pool=async_pool)
    p = c.pipeline(transaction=True)

    expected = []
    for i in range(100):
        p.lpush(str(i), "V")
        expected.append(f"LPUSH {i} V")

    await p.execute()

    collected = await monitor.stop(0.3)
    expected = CollectedRedisMsg.all_from_src(*expected)

    # The order is random due to squashing
    assert set(expected) == set(collected[1:-1])


"""
Test MONITOR command preserves correct order for MULTI/EXEC sequences
Regression test for https://github.com/dragonflydb/dragonfly/issues/5953
"""


@dfly_args({"proactor_threads": 4})
async def test_monitor_command_multi_exec_order(async_pool):
    monitor = CollectingMonitor(aioredis.Redis(connection_pool=async_pool))
    await monitor.start()

    c = aioredis.Redis(connection_pool=async_pool)
    p = c.pipeline(transaction=True)
    p.ping()
    p.set("key1", "value1")
    p.get("key1")
    await p.execute()

    collected = await monitor.stop()

    # Verify the commands appear in the correct order: MULTI, PING, SET, GET, EXEC
    assert len(collected) == 5
    assert "MULTI" in collected[0].cmd.upper()
    assert "PING" in collected[1].cmd.upper()
    assert "SET" in collected[2].cmd.upper()
    assert "GET" in collected[3].cmd.upper()
    assert "EXEC" in collected[4].cmd.upper()


"""
Test MONITOR command with lua script
https://github.com/dragonflydb/dragonfly/issues/756
"""

TEST_MONITOR_SCRIPT = """
    redis.call('SET', 'A', 1)
    redis.call('GET', 'A')
    redis.call('SADD', 'S', 1, 2, 3)
    redis.call('LPUSH', 'L', 1)
    redis.call('LPOP', 'L')
"""


@dfly_args({"proactor_threads": 4, "lua_auto_async": "false"})
async def test_monitor_command_lua(async_pool):
    monitor = CollectingMonitor(aioredis.Redis(connection_pool=async_pool))
    await monitor.start()

    c = aioredis.Redis(connection_pool=async_pool)
    await c.eval(TEST_MONITOR_SCRIPT, 3, "A", "S", "L")

    collected = await monitor.stop()
    expected = CollectedRedisMsg.all_from_src(
        "SET A 1", "GET A", "SADD S 1 2 3", "LPUSH L 1", "LPOP L", src="lua"
    )

    assert expected == collected[1:]


"""
Run test in pipeline mode.
This is mostly how this is done with python - its more like a transaction that
the connections is running all commands in its context
"""


async def test_pipeline_support(async_client):
    def generate(max):
        for i in range(max):
            yield f"key{i}", f"value={i}"

    messages = {a: b for a, b in generate(5)}
    assert await run_pipeline_mode(async_client, messages)


async def reader(channel: aioredis.client.PubSub, messages, max: int):
    message_count = len(messages)
    while message_count > 0:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    message_count = message_count - 1
                    if message["data"] not in messages:
                        return False, f"got unexpected message from pubsub - {message['data']}"
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            pass
    return True, "success"


async def run_pipeline_mode(async_client: aioredis.Redis, messages):
    pipe = async_client.pipeline(transaction=False)
    for key, val in messages.items():
        pipe.set(key, val)
    result = await pipe.execute()

    print(f"got result from the pipeline of {result} with len = {len(result)}")
    if len(result) != len(messages):
        return False, f"number of results from pipe {len(result)} != expected {len(messages)}"
    elif False in result:
        return False, "expecting to successfully get all result good, but some failed"
    else:
        return True, "all command processed successfully"


"""
Test the pipeline command
Open connection to the subscriber and publish on the other end messages
Make sure that we are able to send all of them and that we are getting the
expected results on the subscriber side
"""


async def test_pubsub_command(async_client):
    def generate(max):
        for i in range(max):
            yield f"message number {i}"

    messages = [a for a in generate(5)]
    assert await run_pubsub(async_client, messages, "channel-1")


async def run_pubsub(async_client, messages, channel_name):
    pubsub = async_client.pubsub()
    await pubsub.subscribe(channel_name)

    future = asyncio.create_task(reader(pubsub, messages, len(messages)))
    success = True

    for message in messages:
        res = await async_client.publish(channel_name, message)
        if not res:
            success = False
            break

    await future
    status, message = future.result()

    await pubsub.close()
    if status and success:
        return True, "successfully completed all"
    else:
        return (
            False,
            f"subscriber result: {status}: {message},  publisher publish: success {success}",
        )


async def run_multi_pubsub(async_client, messages, channel_name):
    subs = [async_client.pubsub() for i in range(5)]
    for s in subs:
        await s.subscribe(channel_name)

    tasks = [
        asyncio.create_task(reader(s, messages, random.randint(0, len(messages)))) for s in subs
    ]

    success = True

    for message in messages:
        res = await async_client.publish(channel_name, message)
        if not res:
            success = False
            break

    for f in tasks:
        await f
    results = [f.result() for f in tasks]

    for s in subs:
        await s.close()
    if success:
        for status, message in results:
            if not status:
                return False, f"failed to process {message}"
        return True, "success"
    else:
        return False, "failed to publish"


"""
Test with multiple subscribers for a channel
We want to stress this to see if we have any issue
with the pub sub code since we are "sharing" the message
across multiple connections internally
"""


async def test_multi_pubsub(async_client):
    def generate(max):
        for i in range(max):
            yield f"this is message number {i} from the publisher on the channel"

    messages = [a for a in generate(500)]
    state, message = await run_multi_pubsub(async_client, messages, "my-channel")

    assert state, message


"""
Test PUBSUB NUMSUB command.
"""


async def test_pubsub_subcommand_for_numsub(async_client: aioredis.Redis):
    async def resub(s: "aioredis.PubSub", sub: bool, chan: str):
        if sub:
            await s.subscribe(chan)
        else:
            await s.unsubscribe(chan)
        # Wait for PUSH message to be parsed to make sure upadte was performed
        await s.get_message(timeout=0.1)

    # Subscribe 5 times to chan1
    subs1 = [async_client.pubsub() for i in range(5)]
    await asyncio.gather(*(resub(s, True, "chan1") for s in subs1))
    assert await async_client.pubsub_numsub("chan1") == [("chan1", 5)]

    # Unsubscribe all from chan1
    await asyncio.gather(*(resub(s, False, "chan1") for s in subs1))

    # Make sure numsub drops to 0
    async for numsub, breaker in tick_timer(lambda: async_client.pubsub_numsub("chan1")):
        with breaker:
            assert numsub[0][1] == 0

    # Check empty numsub
    assert await async_client.pubsub_numsub() == []

    subs2 = [async_client.pubsub() for i in range(5)]
    await asyncio.gather(*(resub(s, True, "chan2") for s in subs2))

    subs3 = [async_client.pubsub() for i in range(10)]
    await asyncio.gather(*(resub(s, True, "chan3") for s in subs3))

    assert await async_client.pubsub_numsub("chan2", "chan3") == [("chan2", 5), ("chan3", 10)]

    await asyncio.gather(*(s.unsubscribe() for s in subs2 + subs3))


"""
Test that pubsub clients who are stuck on backpressure from a slow client (the one in the test doesn't read messages at all)
will eventually unblock when it disconnects.
"""


@pytest.mark.slow
@dfly_args({"proactor_threads": "1", "publish_buffer_limit": "100"})
async def test_publish_stuck(df_server: DflyInstance, async_client: aioredis.Redis):
    reader, writer = await asyncio.open_connection("127.0.0.1", df_server.port, limit=10)
    writer.write(b"SUBSCRIBE channel\r\n")
    await writer.drain()

    async def pub_task():
        payload = "msg" * 1000
        p = async_client.pipeline()
        for _ in range(1000):
            p.publish("channel", payload)
        await p.execute()

    publishers = [asyncio.create_task(pub_task()) for _ in range(20)]

    await asyncio.sleep(5)

    # Check we reached the limit
    pub_bytes = int((await async_client.info())["dispatch_queue_subscriber_bytes"])
    assert pub_bytes >= 100

    await asyncio.sleep(0.1)

    # Make sure processing is stalled
    new_pub_bytes = int((await async_client.info())["dispatch_queue_subscriber_bytes"])
    assert new_pub_bytes == pub_bytes

    writer.write(b"QUIT\r\n")
    await writer.drain()
    writer.close()

    # Make sure all publishers unblock eventually
    for pub in asyncio.as_completed(publishers):
        await pub


@pytest.mark.slow
@dfly_args({"proactor_threads": "4"})
async def test_pubsub_busy_connections(df_server: DflyInstance):
    sleep = 60

    async def sub_thread():
        i = 0

        async def sub_task():
            nonlocal i
            sleep_task = asyncio.create_task(asyncio.sleep(sleep))
            while not sleep_task.done():
                client = df_server.client()
                pubsub = client.pubsub()
                await pubsub.subscribe("channel")
                # await pubsub.unsubscribe("channel")
                i = i + 1
                await client.close()

        subs = [asyncio.create_task(sub_task()) for _ in range(10)]
        for s in subs:
            await s
        logging.debug(f"Exiting thread after {i} subscriptions")

    async def pub_task():
        pub = df_server.client()
        i = 0
        sleep_task = asyncio.create_task(asyncio.sleep(sleep))
        while not sleep_task.done():
            await pub.publish("channel", f"message-{i}")
            i = i + 1

    def run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(sub_thread())

    threads = []
    for _ in range(10):
        thread = Thread(target=run_in_thread)
        thread.start()
        threads.append(thread)

    await pub_task()

    for thread in threads:
        thread.join()


async def test_subscribers_with_active_publisher(df_server: DflyInstance, max_connections=100):
    # TODO: I am not how to customize the max connections for the pool.
    async_pool = aioredis.ConnectionPool(
        host="localhost",
        port=df_server.port,
        db=0,
        decode_responses=True,
        max_connections=max_connections,
    )

    async def publish_worker():
        client = aioredis.Redis(connection_pool=async_pool)
        for i in range(0, 2000):
            await client.publish("channel", f"message-{i}")
        await client.aclose()

    async def channel_reader(channel: aioredis.client.PubSub):
        for i in range(0, 150):
            try:
                async with async_timeout.timeout(1):
                    message = await channel.get_message(ignore_subscribe_messages=True)
            except asyncio.TimeoutError:
                break

    async def subscribe_worker():
        client = aioredis.Redis(connection_pool=async_pool)
        pubsub = client.pubsub()
        async with pubsub as p:
            await pubsub.subscribe("channel")
            await channel_reader(pubsub)
            await pubsub.unsubscribe("channel")

    # Create a publisher that sends constantly messages to the channel
    # Then create subscribers that will subscribe to already active channel
    pub_task = asyncio.create_task(publish_worker())
    await asyncio.gather(*(subscribe_worker() for _ in range(max_connections - 10)))
    await pub_task
    await async_pool.disconnect()


# This test ensures that no messages are sent after a successful
# acknowledgement of a unsubscribe.
# Low publish_buffer_limit makes publishers block on memory backpressure


@dfly_args({"publish_buffer_limit": 100, "proactor_threads": 2})
async def test_pubsub_unsubscribe(df_server: DflyInstance):
    long_message = "a" * 100_000
    pub_sent = 0
    pub_ready_ev = asyncio.Event()

    async def publisher():
        nonlocal pub_sent
        async with df_server.client(single_connection_client=True) as c:
            for _ in range(32):
                await c.execute_command("PUBLISH", "chan", long_message)
                # Unblock subscriber after a sufficient amount of publish requests accumulated
                pub_sent += 1
                if pub_sent >= 16:
                    pub_ready_ev.set()

    # Get raw connection from the client and subscribe to chan
    cl = df_server.client(single_connection_client=True)
    await cl.ping()
    conn = cl.connection
    await conn.send_command("SUBSCRIBE chan")

    # Flood our only subscriber with large messages to make publishers stop
    tasks = [asyncio.create_task(publisher()) for _ in range(16)]

    # Unsubscribe in the process
    await pub_ready_ev.wait()
    await conn.send_command("UNSUBSCRIBE")

    # No messages should be received after we've read unsubscribe reply
    had_unsub = False
    while True:
        reply = await conn.read_response(timeout=0.2)
        if reply is None:
            break

        if reply[0] == "unsubscribe":
            assert reply[2] == 0  # zero subscriptions left
            had_unsub = True
        else:
            assert not had_unsub, "found message even after all subscriptions were removed"

    assert had_unsub
    await asyncio.gather(*tasks)
    await cl.aclose()


async def produce_expiring_keys(async_client: aioredis.Redis):
    keys = []
    for i in range(10, 50):
        keys.append(f"k{i}")
        await async_client.set(keys[-1], "X", px=200 + i * 10)
    return keys


async def collect_expiring_events(pclient, keys):
    events = []
    async for message in pclient.listen():
        if message["type"] == "subscribe":
            continue

        events.append(message)
        if len(events) >= len(keys):
            break
    return events


@dfly_args({"notify_keyspace_events": "Ex"})
async def test_keyspace_events(async_client: aioredis.Redis):
    pclient = async_client.pubsub()
    await pclient.subscribe("__keyevent@0__:expired")

    keys = await produce_expiring_keys(async_client)

    # We don't support immediate expiration:
    # keys += ['immediate']
    # await async_client.set(keys[-1], 'Y', exat=123) # expired 50 years ago

    events = await collect_expiring_events(pclient, keys)

    assert set(ev["data"] for ev in events) == set(keys)


async def test_keyspace_events_config_set(async_client: aioredis.Redis):
    # nonsense does not make sense as argument, we only accept ex or empty string
    with pytest.raises(ResponseError):
        await async_client.config_set("notify_keyspace_events", "nonsense")

    await async_client.config_set("notify_keyspace_events", "ex")
    pclient = async_client.pubsub()
    await pclient.subscribe("__keyevent@0__:expired")

    keys = await produce_expiring_keys(async_client)

    events = await collect_expiring_events(pclient, keys)

    assert set(ev["data"] for ev in events) == set(keys)

    keys = await produce_expiring_keys(async_client)
    await async_client.config_set("notify_keyspace_events", "")
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await collect_expiring_events(pclient, keys)


@dfly_args({"max_busy_read_usec": 50000})
async def test_reply_count(async_client: aioredis.Redis):
    """Make sure reply aggregations reduce reply counts for common cases"""

    async def get_reply_count():
        return (await async_client.info("STATS"))["reply_count"]

    async def measure(aw):
        before = await get_reply_count()
        await aw
        return await get_reply_count() - before - 1

    await async_client.config_resetstat()
    base = await get_reply_count()
    info_diff = await get_reply_count() - base
    assert info_diff == 1

    # Warm client buffer up
    await async_client.lpush("warmup", *(i for i in range(500)))
    await async_client.lrange("warmup", 0, -1)

    # Integer list
    await async_client.lpush("list-1", *(i for i in range(100)))
    assert await measure(async_client.lrange("list-1", 0, -1)) == 1

    # Integer set
    await async_client.sadd("set-1", *(i for i in range(100)))
    assert await measure(async_client.smembers("set-1")) == 1

    # Sorted sets
    await async_client.zadd("zset-1", mapping={str(i): i for i in range(50)})
    assert await measure(async_client.zrange("zset-1", 0, -1, withscores=True)) == 1

    # Exec call
    e = async_client.pipeline(transaction=True)
    for _ in range(100):
        e.incr("num-1")

    # one - for MULTI-OK, one for the rest. Depends on the squashing efficiency,
    # can be either 1 or 2 replies.
    assert await measure(e.execute()) <= 2

    # Just pipeline
    p = async_client.pipeline(transaction=False)
    for _ in range(100):
        p.incr("num-1")
    assert await measure(p.execute()) <= 2

    # Script result
    assert await measure(async_client.eval('return {1,2,{3,4},5,6,7,8,"nine"}', 0)) == 1

    # Search results
    await async_client.execute_command("FT.CREATE i1 SCHEMA name text")
    for i in range(50):
        await async_client.hset(f"key-{i}", "name", f"name number {i}")
    assert await measure(async_client.ft("i1").search("*")) == 1


async def test_big_command(df_server, size=8 * 1024):
    reader, writer = await asyncio.open_connection("127.0.0.1", df_server.port)

    writer.write(f"SET a {'v'*size}\n".encode())
    await writer.drain()

    assert "OK" in (await reader.readline()).decode()

    writer.close()
    await writer.wait_closed()


async def test_subscribe_pipelined(async_client: aioredis.Redis):
    pipe = async_client.pipeline(transaction=False)
    pipe.execute_command("subscribe channel").execute_command("subscribe channel")
    await pipe.echo("bye bye").execute()


async def test_subscribe_in_pipeline(async_client: aioredis.Redis):
    pipe = async_client.pipeline(transaction=False)
    pipe.echo("one")
    pipe.execute_command("SUBSCRIBE ch1")
    pipe.echo("two")
    pipe.execute_command("SUBSCRIBE ch2")
    pipe.echo("three")
    res = await pipe.execute()

    assert res == ["one", ["subscribe", "ch1", 1], "two", ["subscribe", "ch2", 2], "three"]


async def test_send_delay_metric(df_server: DflyInstance):
    client = df_server.client()
    await client.client_setname("client1")
    blob = "A" * 1000
    for j in range(10):
        await client.set(f"key-{j}", blob)

    await client.config_set("pipeline_queue_limit", 100)
    reader, writer = await asyncio.open_connection("localhost", df_server.port)

    async def send_data_noread():
        for j in range(500000):
            writer.write(f"GET key-{j % 10}\n".encode())
            await writer.drain()

    t1 = asyncio.create_task(send_data_noread())

    @assert_eventually
    async def wait_for_large_delay():
        info = await client.info("clients")
        assert int(info["send_delay_ms"]) > 100

    # Check that the delay metric indeed increases as we have a connection
    # that is not reading the data.
    await wait_for_large_delay()
    t1.cancel()
    writer.close()


async def test_match_http(df_server: DflyInstance):
    client = df_server.client()
    reader, writer = await asyncio.open_connection("localhost", df_server.port)
    for i in range(2000):
        writer.write(f"foo bar ".encode())
        await writer.drain()


"""
This test makes sure that Dragonfly can receive blocks of pipelined commands even
while a script is still executing. This is a dangerous scenario because both the dispatch fiber
and the connection fiber are actively using the context. What is more, the script execution injects
its own custom reply builder, which can't be used anywhere else, besides the lua script itself.
"""

BUSY_SCRIPT = """
for i=1,300 do
    redis.call('MGET', 'k1', 'k2', 'k3')
end
"""

PACKET1 = """
MGET s1 s2 s3
EVALSHA {sha} 3 k1 k2 k3
"""

PACKET2 = """
MGET m1 m2 m3
MGET m4 m5 m6
MGET m7 m8 m9\n
"""

PACKET3 = (
    """
PING
"""
    * 500
    + "ECHO DONE\n"
)


async def test_parser_while_script_running(async_client: aioredis.Redis, df_server: DflyInstance):
    sha = await async_client.script_load(BUSY_SCRIPT)

    # Use a raw tcp connection for strict control of sent commands
    # Below we send commands while the previous ones didn't finish
    reader, writer = await asyncio.open_connection("localhost", df_server.port)

    # Send first pipeline packet, last commands is a long executing script
    writer.write(PACKET1.format(sha=sha).encode())
    await writer.drain()

    # Give the script some time to start running
    await asyncio.sleep(0.01)

    # Send another packet that will be received while the script is running
    writer.write(PACKET2.encode())
    # The last batch has to be big enough, so the script will finish before it is fully consumed
    writer.write(PACKET3.encode())
    await writer.drain()

    await reader.readuntil(b"DONE")
    writer.close()
    await writer.wait_closed()


"""
    This test makes sure that we can migrate while handling pipelined commands and don't keep replies
    batched even if the stream suddenly stops.
"""


@dfly_args({"proactor_threads": "4", "pipeline_squash": 0})
async def test_pipeline_batching_while_migrating(
    async_client: aioredis.Redis, df_server: DflyInstance
):
    sha = await async_client.script_load("return redis.call('GET', KEYS[1])")

    reader, writer = await asyncio.open_connection("localhost", df_server.port)

    # First, write a EVALSHA that will ask for migration (75% it's on the wrong shard)
    # and some more pipelined commands that will keep Dragonfly busy
    incrs = "".join("INCR a\r\n" for _ in range(50))
    writer.write((f"EVALSHA {sha} 1 a\r\n" + incrs).encode())
    await writer.drain()
    # We migrate only when the socket wakes up, so send another batch to trigger migration
    writer.write("INCR a\r\n".encode())
    await writer.drain()

    # The data doesn't necessarily arrive in a single batch
    async def read():
        reply = ""
        while not reply.strip().endswith("51"):
            reply = (await reader.read(520)).decode()

    # Make sure we recived all replies
    await asyncio.wait_for(read(), timeout=2.0)

    writer.close()
    await writer.wait_closed()


@dfly_args({"proactor_threads": 1})
async def test_large_cmd(async_client: aioredis.Redis):
    MAX_ARR_SIZE = 65535
    res = await async_client.hset(
        "foo", mapping={f"key{i}": f"val{i}" for i in range(MAX_ARR_SIZE // 2)}
    )
    assert res == MAX_ARR_SIZE // 2

    res = await async_client.mset({f"key{i}": f"val{i}" for i in range(MAX_ARR_SIZE // 2)})
    assert res

    res = await async_client.mget([f"key{i}" for i in range(MAX_ARR_SIZE)])
    assert len(res) == MAX_ARR_SIZE


@dfly_args({"proactor_threads": 1})
async def test_parser_memory_stats(df_server, async_client: aioredis.Redis):
    reader, writer = await asyncio.open_connection("127.0.0.1", df_server.port, limit=10)
    writer.write(b"*1000\r\n")
    writer.write(b"$4\r\nmget\r\n")
    val = (b"a" * 100) + b"\r\n"
    for i in range(0, 900):
        writer.write(b"$100\r\n" + val)
    await writer.drain()  # writer is pending because the request is not finished.

    @assert_eventually
    async def check_stats():
        stats = await async_client.execute_command("memory stats")
        assert stats["connections.direct_bytes"] > 130000

    await check_stats()


async def test_reject_non_tls_connections_on_tls(with_tls_server_args, df_factory):
    server: DflyInstance = df_factory.create(
        no_tls_on_admin_port="true",
        admin_port=1111,
        port=1211,
        requirepass="XXX",
        **with_tls_server_args,
    )
    server.start()

    client = server.client(password="XXX")
    with pytest.raises(ResponseError):
        await client.dbsize()
    await client.aclose()

    client = server.admin_client(password="XXX")
    assert await client.dbsize() == 0


async def test_tls_insecure(with_ca_tls_server_args, with_tls_client_args, df_factory):
    server = df_factory.create(port=BASE_PORT, **with_ca_tls_server_args)
    server.start()

    client = aioredis.Redis(port=server.port, **with_tls_client_args, ssl_cert_reqs=None)
    assert await client.dbsize() == 0


async def test_tls_full_auth(with_ca_tls_server_args, with_ca_tls_client_args, df_factory):
    server = df_factory.create(port=BASE_PORT, **with_ca_tls_server_args)
    server.start()

    client = aioredis.Redis(port=server.port, **with_ca_tls_client_args)
    assert await client.dbsize() == 0


async def test_tls_reject(
    with_ca_tls_server_args, with_tls_client_args, df_factory: DflyInstanceFactory
):
    server: DflyInstance = df_factory.create(port=BASE_PORT, **with_ca_tls_server_args)
    server.start()

    client = server.client(**with_tls_client_args, ssl_cert_reqs=None)
    await client.ping()
    await client.aclose()

    client = server.client(**with_tls_client_args)
    with pytest.raises(ConnectionError):
        await client.ping()


@dfly_args({"proactor_threads": "4", "pipeline_squash": 1})
async def test_squashed_pipeline_eval(async_client: aioredis.Redis):
    p = async_client.pipeline(transaction=False)
    for _ in range(5):
        # Deliberately lowcase EVAL to test that it is not squashed
        p.execute_command("eval", "return redis.call('set', KEYS[1], 'value')", 1, "key")
    res = await p.execute()
    assert res == ["OK"] * 5


@dfly_args({"proactor_threads": "4", "pipeline_squash": 10})
async def test_squashed_pipeline(async_client: aioredis.Redis):
    p = async_client.pipeline(transaction=False)

    for j in range(50):
        for i in range(10):
            p.incr(f"k{i}")
        p.execute_command("NOTFOUND")

    res = await p.execute(raise_on_error=False)

    for j in range(50):
        assert res[0:10] == [j + 1] * 10
        assert isinstance(res[10], aioredis.ResponseError)
        res = res[11:]


@dfly_args({"proactor_threads": "4", "pipeline_squash": 10})
async def test_squashed_pipeline_seeder(df_server, df_seeder_factory):
    seeder = df_seeder_factory.create(port=df_server.port, keys=10_000)
    await seeder.run(target_deviation=0.1)


"""
This test makes sure that multi transactions can be integrated into pipeline squashing
"""


@dfly_args({"proactor_threads": "4", "pipeline_squash": 1})
async def test_squashed_pipeline_multi(async_client: aioredis.Redis):
    p = async_client.pipeline(transaction=False)
    for _ in range(5):
        # Series of squashable commands
        for _ in range(5):
            p.set("first", "true")
        # Non-squashable
        p.info()
        # Eval without at tx
        p.execute_command("MULTI")
        p.set("second", "true")
        p.execute_command("EXEC")
        # Finishing sequence
        for _ in range(5):
            p.set("third", "true")
    await p.execute()


async def test_unix_domain_socket(df_factory, tmp_dir):
    server = df_factory.create(proactor_threads=1, port=BASE_PORT, unixsocket="./df.sock")
    server.start()

    await asyncio.sleep(0.5)

    r = aioredis.Redis(unix_socket_path=tmp_dir / "df.sock")
    assert await r.ping()


async def test_unix_socket_only(df_factory, tmp_dir):
    server = df_factory.create(proactor_threads=1, port=0, unixsocket="./df.sock")
    # we call _start because we start() wait for the port to become available and
    # we run here a process without a port.
    server._start()

    await asyncio.sleep(1)

    r = aioredis.Redis(unix_socket_path=tmp_dir / "df.sock")
    assert await r.ping()


"""
Test nested pauses. Executing CLIENT PAUSE should be possible even if another write-pause is active.
It should prolong the pause for all current commands.
"""


@pytest.mark.slow
async def test_nested_client_pause(async_client: aioredis.Redis):
    async def do_pause():
        await async_client.execute_command("CLIENT", "PAUSE", "1000", "WRITE")

    async def do_write():
        await async_client.execute_command("LPUSH", "l", "1")

    p1 = asyncio.create_task(do_pause())
    await asyncio.sleep(0.1)

    p2 = asyncio.create_task(do_write())
    assert not p2.done()

    await asyncio.sleep(0.5)
    p3 = asyncio.create_task(do_pause())

    await p1
    await asyncio.sleep(0.1)
    assert not p2.done()  # blocked by p3 now

    await p2
    await asyncio.sleep(0.0)
    assert p3.done()
    await p3


@dfly_args({"proactor_threads": "4"})
async def test_blocking_command_client_pause(async_client: aioredis.Redis):
    """
    1. Check client pause success when blocking transaction is running
    2. lpush is paused after running client puase
    3. once puased is finished lpush will run and blpop will pop the pushed value
    """

    async def blpop_command():
        res = await async_client.execute_command("blpop dest7 10")
        assert res == ["dest7", "value"]

    async def brpoplpush_command():
        res = await async_client.execute_command("brpoplpush src dest7 2")
        assert res == "value"

    async def lpush_command():
        await async_client.execute_command("lpush src value")

    blpop = asyncio.create_task(blpop_command())
    brpoplpush = asyncio.create_task(brpoplpush_command())
    await asyncio.sleep(0.1)

    res = await async_client.execute_command("client pause 1000")
    assert res == "OK"

    lpush = asyncio.create_task(lpush_command())
    assert not lpush.done()

    await lpush
    await brpoplpush
    await blpop


async def test_multiple_blocking_commands_client_pause(async_client: aioredis.Redis):
    """
    Check running client pause command simultaneously with running multiple blocking command
    from multiple connections
    """

    async def just_blpop():
        key = "".join(random.choices(string.ascii_letters, k=3))
        await async_client.execute_command(f"blpop {key} 2")

    async def client_pause():
        res = await async_client.execute_command("client pause 1000")
        assert res == "OK"

    tasks = [just_blpop() for _ in range(20)]
    tasks.append(client_pause())

    all = asyncio.gather(*tasks)

    assert not all.done()
    await all


async def test_tls_when_read_write_is_interleaved(
    with_ca_tls_server_args, with_ca_tls_client_args, df_factory
):
    """
    This test covers a deadlock bug in helio and TlsSocket when a client connection renegotiated a
    handshake without reading its pending data from the socket.
    This is a weak test case and from our local experiments it deadlocked 30% of the test runs
    """
    server: DflyInstance = df_factory.create(
        port=1211, **with_ca_tls_server_args, proactor_threads=1
    )
    # TODO(kostas): to fix the deadlock in the test
    server.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    ssl_key = with_ca_tls_client_args["ssl_keyfile"]
    ssl_cert = with_ca_tls_client_args["ssl_certfile"]
    ssl_ca_cert = with_ca_tls_client_args["ssl_ca_certs"]
    ssl_sock = ssl.wrap_socket(
        s,
        keyfile=ssl_key,
        certfile=ssl_cert,
        ca_certs=ssl_ca_cert,
        ssl_version=ssl.PROTOCOL_TLSv1_2,
    )
    ssl_sock.connect(("127.0.0.1", server.port))
    ssl_sock.settimeout(0.1)

    tmp = "f" * 1000
    message = f"SET foo {tmp}\r\n".encode()
    ssl_sock.send(message)

    try:
        for i in range(0, 100_000):
            res = random.randint(1, 4)
            message = b""
            for j in range(0, res):
                message = message + b"GET foo\r\n"
            ssl_sock.send(message)
            ssl_sock.do_handshake()
    except:
        # We might have filled the socket buffer, causing further sending will fail
        pass

    # This deadlocks
    client = aioredis.Redis(port=server.port, **with_ca_tls_client_args)
    await client.execute_command("GET foo")


async def test_lib_name_ver(async_client: aioredis.Redis):
    await async_client.execute_command("client setinfo lib-name dragonfly")
    await async_client.execute_command("client setinfo lib-ver 1.2.3.4")

    list = await async_client.execute_command("client list")
    assert len(list) == 1
    assert list[0]["lib-name"] == "dragonfly"
    assert list[0]["lib-ver"] == "1.2.3.4"


async def test_hiredis(df_factory):
    server = df_factory.create(proactor_threads=1)
    server.start()
    client = base_redis.Redis(port=server.port, protocol=3, cache_config=CacheConfig())
    client.ping()


@assert_eventually(times=500)
async def wait_for_conn_drop(async_client):
    clients = await async_client.client_list()
    logging.info("wait_for_conn_drop clients: %s", clients)
    assert len(clients) <= 1


@dfly_args({"timeout": 1})
async def test_timeout(df_server: DflyInstance, async_client: aioredis.Redis):
    another_client = df_server.client()
    await another_client.ping()
    clients = await async_client.client_list()
    assert len(clients) == 2

    await asyncio.sleep(2)

    await wait_for_conn_drop(async_client)
    info = await async_client.info("clients")
    assert int(info["timeout_disconnects"]) >= 1


@dfly_args({"send_timeout": 3})
async def test_send_timeout(df_server, async_client: aioredis.Redis):
    reader, writer = await asyncio.open_connection("127.0.0.1", df_server.port)
    writer.write(f"client setname writer_test\n".encode())
    await writer.drain()
    assert "OK" in (await reader.readline()).decode()
    clients = await async_client.client_list()
    assert len(clients) == 2
    size = 1024 * 1024
    writer.write(f"SET a {'v'*size}\n".encode())
    await writer.drain()

    async def get_task():
        while True:
            writer.write(f"GET a\n".encode())
            await writer.drain()
            await asyncio.sleep(0.1)

    get = asyncio.create_task(get_task())

    @assert_eventually(times=600)
    async def wait_for_stuck_on_send():
        clients = await async_client.client_list()
        logging.info("wait_for_stuck_on_send clients: %s", clients)
        phase = next(
            (client["phase"] for client in clients if client["name"] == "writer_test"), None
        )
        assert phase == "send"

    await wait_for_stuck_on_send()
    await wait_for_conn_drop(async_client)
    info = await async_client.info("clients")
    assert int(info["timeout_disconnects"]) >= 1
    logging.info("finished disconnect")
    get.cancel()


# Test that the cache pipeline does not grow or shrink under constant pipeline load.
@dfly_args({"proactor_threads": 1, "pipeline_squash": 9, "max_busy_read_usec": 50000})
async def test_pipeline_cache_only_async_squashed_dispatches(df_factory):
    server = df_factory.create()
    server.start()

    client = server.client()
    await client.ping()  # Make sure the connection and the protocol were established

    async def push_pipeline(size):
        p = client.pipeline(transaction=True)
        for i in range(size):
            p.info()
        res = await p.execute()
        return res

    # Dispatch only async command/pipelines and force squashing. pipeline_cache_bytes,
    # should be zero because:
    # We always dispatch the items that will be squashed, so when `INFO` gets called
    # the cache is empty because the pipeline consumed it throughout its execution
    # high max_busy_read_usec ensures that the connection fiber has enough time to push
    # all the commands to reach the squashing limit.
    for i in range(0, 10):
        # it's actually 11 commands. 8 INFO + 2 from the MULTI/EXEC block that is injected
        # by the client. The minimum to squash is 9 so it will squash the pipeline
        # and INFO ALL should return zero for all the squashed commands in the pipeline
        res = await push_pipeline(8)
        for r in res:
            assert r["pipeline_cache_bytes"] == 0

    # Non zero because we reclaimed/recycled the messages back to the cache
    info = await client.info()
    assert info["pipeline_cache_bytes"] > 0


# Test that the pipeline cache size shrinks on workloads that storm the datastore with
# pipeline commands and then "back off" by gradually reducing the pipeline load such that
# the cache becomes progressively underutilized. At that stage, the pipeline should slowly
# shrink (because it's underutilized).
@pytest.mark.skip("Flaky")
@dfly_args({"proactor_threads": 1})
async def test_pipeline_cache_size(df_server: DflyInstance):
    # Start 1 client.
    good_client = df_server.client()
    bad_actor_client = df_server.client()

    async def push_pipeline(bad_actor_client, size=1):
        # Fill cache.
        p = bad_actor_client.pipeline(transaction=True)
        for i in range(size):
            p.lpush(str(i), "V")
        await p.execute()

    # Establish a baseline for the cache size. We dispatch async here.
    await push_pipeline(bad_actor_client, 32)
    info = await good_client.info()

    old_pipeline_cache_bytes = info["pipeline_cache_bytes"]
    assert old_pipeline_cache_bytes > 0
    assert info["dispatch_queue_bytes"] == 0

    for i in range(30):
        await push_pipeline(bad_actor_client)
        await good_client.execute_command(f"set foo{i} bar")

    info = await good_client.info()

    # Gradually release pipeline.
    assert old_pipeline_cache_bytes > info["pipeline_cache_bytes"]
    assert info["dispatch_queue_bytes"] == 0

    # Now drain the full cache.
    async with async_timeout.timeout(5):
        while info["pipeline_cache_bytes"] != 0:
            await good_client.execute_command(f"set foo{i} bar")
            info = await good_client.info()

    assert info["dispatch_queue_bytes"] == 0


@dfly_args({"proactor_threads": 4, "pipeline_queue_limit": 10})
async def test_pipeline_overlimit(df_server: DflyInstance):
    client = df_server.client()

    await client.set("x", "a" * 1024 * 5)

    async def pipe_overlimit():
        c = df_server.client()
        pipe = c.pipeline()
        for i in range(1000):
            pipe.get("x")
        logging.debug("Executing...")
        res = await pipe.execute()
        logging.debug(f"Executed.")

    pipeline_tasks = [asyncio.create_task(pipe_overlimit()) for _ in range(20)]

    await asyncio.sleep(2)
    await client.config_set("pipeline_queue_limit", 10000)
    for task in pipeline_tasks:
        await task


async def test_client_unpause(df_server: DflyInstance):
    async_client = df_server.client()
    await async_client.client_pause(3000, all=False)

    async def set_foo():
        client = df_server.client()
        async with async_timeout.timeout(2):
            await client.execute_command("SET", "foo", "bar")

    p1 = asyncio.create_task(set_foo())

    await asyncio.sleep(0.5)
    assert not p1.done()

    async with async_timeout.timeout(0.5):
        await async_client.client_unpause()

    async with async_timeout.timeout(0.5):
        await p1
        assert p1.done()

    await async_client.client_pause(1, all=False)
    await asyncio.sleep(2)


async def test_client_pause_b2b(async_client):
    async with async_timeout.timeout(1):
        await async_client.client_pause(2000, all=False)
        await async_client.client_pause(2000, all=False)


async def test_client_unpause_after_pause_all(async_client):
    await async_client.client_pause(2000, all=True)
    # Blocks and waits
    res = await async_client.client_unpause()
    assert res == "OK"
    await async_client.client_pause(2000, all=False)
    res = await async_client.client_unpause()


async def test_client_detached_crash(df_factory):
    server = df_factory.create(proactor_threads=1)
    server.start()
    async_client = server.client()
    await async_client.client_pause(2, all=False)
    server.stop()


async def test_tls_client_kill_preemption(
    with_ca_tls_server_args, with_ca_tls_client_args, df_factory
):
    server = df_factory.create(proactor_threads=4, port=BASE_PORT, **with_ca_tls_server_args)
    server.start()

    client = aioredis.Redis(port=server.port, **with_ca_tls_client_args)
    assert await client.dbsize() == 0

    # Get the list of clients
    clients_info = await client.client_list()
    assert len(clients_info) == 1

    kill_id = clients_info[0]["id"]

    async def seed():
        with pytest.raises(aioredis.ConnectionError) as roe:
            while True:
                p = client.pipeline(transaction=True)
                expected = []
                for i in range(100):
                    p.lpush(str(i), "V")
                    expected.append(f"LPUSH {i} V")

                await p.execute()

    task = asyncio.create_task(seed())

    await asyncio.sleep(0.1)

    cl = aioredis.Redis(port=server.port, **with_ca_tls_client_args)
    await cl.execute_command(f"CLIENT KILL ID {kill_id}")

    await task
    server.stop()
    lines = server.find_in_logs("Preempting inside of atomic section, fiber")
    assert len(lines) == 0


@dfly_args({"proactor_threads": 4})
async def test_client_migrate(df_server: DflyInstance):
    """
    Test that we can migrate a client with "CLIENT MIGRATE" command.
    """
    client1 = df_server.client()
    await client1.client_setname("test_migrate")
    resp = await client1.execute_command("DFLY THREAD")
    client_id = await client1.client_id()
    assert resp[1] == 4
    current_tid = resp[0]
    client2 = df_server.client()
    resp = await client2.execute_command("CLIENT", "MIGRATE", client_id, current_tid)
    assert resp == 0  # not migrated as it's the same thread
    dest_tid = (current_tid + 1) % 4
    resp = await client2.execute_command("CLIENT", "MIGRATE", client_id + 999, dest_tid)
    assert resp == 0  # Not migrated as the client does not exist
    resp = await client2.execute_command("CLIENT", "MIGRATE", client_id, dest_tid)
    assert resp == 1  # migrated successfully


@dfly_args({})
async def test_issue_5931_malformed_protocol_crash(df_server: DflyInstance):
    """
    Regression test for #5931

    The crash.txt file contains malformed RESP protocol that caused the server to crash
    with: "Check failed: RespExpr::STRING == arg.type" in FromArgs()

    This test sends the exact bytes from crash.txt to verify the server handles it
    gracefully without crashing.
    """
    # Open raw TCP connection to send malformed protocol
    reader, writer = await asyncio.open_connection("127.0.0.1", df_server.port)

    try:
        # Send the exact bytes from crash.txt:
        # *0\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r<0xf4>)1\r\n$4\r\nEXEC\r\n
        crash_data = b"*0\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r"
        crash_data += bytes([0xF4])  # Binary byte instead of \n
        crash_data += b")1\r\n$4\r\nEXEC\r\n"

        writer.write(crash_data)
        await writer.drain()

        try:
            response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
            # If we get a response, it should be an error, not a crash
            # The server is still running if we got here
        except asyncio.TimeoutError:
            # Timeout is acceptable - connection might be closed
            pass
        except ConnectionError:
            # Connection closed is acceptable - server detected bad protocol
            pass

    finally:
        writer.close()
        await writer.wait_closed()

    # Verify server is still running by making a normal request
    client = df_server.client()
    await client.ping()
    assert await client.ping() == True
