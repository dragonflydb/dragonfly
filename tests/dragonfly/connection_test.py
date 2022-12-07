import random
import pytest
import asyncio
import aioredis
import async_timeout

'''
Test the monitor command.
Open connection which is used for monitoring
Then send on other connection commands to dragonfly instance
Make sure that we are getting the commands in the monitor context
'''


@pytest.mark.asyncio
async def test_monitor_command(async_pool):
    def generate(max):
        for i in range(max):
            yield f"key{i}", f"value={i}"

    messages = {a: b for a, b in generate(5)}
    assert await run_monitor(messages, async_pool)


def verify_response(monitor_response: dict, key: str, value: str) -> bool:
    if monitor_response is None:
        return False
    if monitor_response["db"] == 1 and monitor_response["client_type"] == "tcp":
        return key in monitor_response["command"] and value in monitor_response["command"]
    else:
        return False


async def process_cmd(monitor, key, value):
    while True:
        try:
            async with async_timeout.timeout(1):
                response = await monitor.next_command()
                if "select" not in response["command"].lower():
                    success = verify_response(response, key, value)
                    if not success:
                        print(f"failed to verfiy message {response} for {key}/{value}")
                        return False, f"failed on the verification of the message {response} at {key}: {value}"
                        #await asyncio.sleep(0.01)
                    else:
                        return True, None
        except asyncio.TimeoutError:
            pass


async def monitor_cmd(mon: aioredis.client.Monitor, messages: dict):
    success = None
    async with mon as monitor:
        try:
            for key, value in messages.items():
                state, msg = await process_cmd(monitor, key, value)
                if not state:
                    return state, msg
            return True, "monitor is successfully done"
        except Exception as e:
            return False, f"stopping monitor on {e}"


async def run_monitor(messages: dict, pool: aioredis.ConnectionPool):
    cmd1 = aioredis.Redis(connection_pool=pool)
    conn = aioredis.Redis(connection_pool=pool)
    monitor = conn.monitor()
    future = asyncio.create_task(monitor_cmd(monitor, messages))
    success = True

    # make sure that the monitor task starts before we're sending anything else!
    await asyncio.sleep(0.01)
    for key, val in messages.items():
        res = await cmd1.set(key, val)
        if not res:
            success = False
            break
    await asyncio.sleep(0.01)
    await future
    status, message = future.result()
    if status and success:
        return True, "successfully completed all"
    else:
        return False, f"monitor result: {status}: {message}, set command success {success}"


'''
Run test in pipeline mode.
This is mostly how this is done with python - its more like a transaction that
the connections is running all commands in its context
'''


@pytest.mark.asyncio
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


async def run_pipeline_mode(async_client, messages):
    pipe = async_client.pipeline()
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


'''
Test the pipeline command
Open connection to the subscriber and publish on the other end messages
Make sure that we are able to send all of them and that we are getting the
expected results on the subscriber side
'''


@pytest.mark.asyncio
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
        return False, f"subscriber result: {status}: {message},  publisher publish: success {success}"


async def run_multi_pubsub(async_client, messages, channel_name):
    subs = [async_client.pubsub() for i in range(5)]
    for s in subs:
        await s.subscribe(channel_name)

    tasks = [
        asyncio.create_task(reader(s, messages, random.randint(0, len(messages)))) for s in subs]

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
                return False,  f"failed to process {message}"
        return True, "success"
    else:
        return False, "failed to publish"


'''
Test with multiple subscribers for a channel
We want to stress this to see if we have any issue
with the pub sub code since we are "sharing" the message
across multiple connections internally
'''


@pytest.mark.asyncio
async def test_multi_pubsub(async_client):
    def generate(max):
        for i in range(max):
            yield f"this is message number {i} from the publisher on the channel"
    messages = [a for a in generate(500)]
    state, message = await run_multi_pubsub(async_client, messages, "my-channel")

    assert state, message
