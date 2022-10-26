import asyncio
import aioredis
import async_timeout
#from conftest import DATABASE_INDEX


def verify_response(monitor_response: dict, key: str, value: str) -> bool:
    if monitor_response is None:
        return False
    if monitor_response["db"] == 1 and monitor_response["client_type"] == "tcp":
        return key in monitor_response["command"] and value in monitor_response["command"]
    else:
        return False


async def monitor_cmd(mon: aioredis.client.Monitor, messages: dict):
    success = None
    async with mon as monitor:
        try:
            for key, value in messages.items():
                try:
                    async with async_timeout.timeout(1):
                        response = await monitor.next_command()
                        success = verify_response(
                            response, key, value)
                        if not success:
                            return False, f"failed on the verification of the message {response} at {key}: {value}"
                        await asyncio.sleep(0.01)
                except asyncio.TimeoutError:
                    pass
            return True, "monitor is successfully done"
        except Exception as e:
            return False, f"stopping monitor on {e}"


async def run_monitor(messages: dict, pool: aioredis.ConnectionPool):
    cmd1 = aioredis.Redis(connection_pool=pool)
    conn = aioredis.Redis(connection_pool=pool)
    monitor = conn.monitor()
    future = asyncio.create_task(monitor_cmd(monitor, messages))
    success = True
    for key, val in messages.items():
        res = await cmd1.set(key, val)
        if not res:
            success = False
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.01)
    await future
    status, message = future.result()
    if status and success:
        return True, "successfully completed all"
    else:
        return False, f"monitor result: {status}: {message}, set command success {success}"


async def run_monitor_command(connection, messages):
    res = await run_monitor(messages, connection)
    print(f"finish test monitoring returning: {res}")
    return res

'''
Test the monitor command.
Open connection which is used for monitoring
Then send on other connection commands to dragonfly instance
Make sure that we are getting the commands in the monitor context
'''


def test_monitor_command(async_pool, event_loop):
    def generate(max):
        for i in range(max):
            yield f"key{i}", f"value={i}"

    messages = {a: b for a, b in generate(5)}

    success, message = event_loop.run_until_complete(
        run_monitor_command(messages=messages, connection=async_pool))

    assert success == True, message


async def run_pipeline_mode(pool, messages):
    conn = aioredis.Redis(connection_pool=pool)
    pipe = conn.pipeline()
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
Run test in pipeline mode.
This is mostly how this is done with python - its more like a transaction that
the connections is running all commands in its context
'''


def test_pipeline_support(async_pool, event_loop):
    def generate(max):
        for i in range(max):
            yield f"key{i}", f"value={i}"

    messages = {a: b for a, b in generate(5)}
    success, message = event_loop.run_until_complete(
        run_pipeline_mode(async_pool, messages))
    assert success, message


async def reader(channel: aioredis.client.PubSub, messages):

    success = True
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


async def run_pubsub(pool, messages, channel_name):
    conn = aioredis.Redis(connection_pool=pool)
    pubsub = conn.pubsub()
    await pubsub.subscribe(channel_name)

    future = asyncio.create_task(reader(pubsub, messages))
    success = True

    for message in messages:
        res = await conn.publish(channel_name, message)
        if not res:
            success = False
            break

    await future
    status, message = future.result()
    if status and success:
        return True, "successfully completed all"
    else:
        return False, f"subscriber result: {status}: {message},  publisher publish: success {success}"

''' 
Test the pipeline command
Open connection to the subscriber and publish on the other end messages
Make sure that we are able to send all of them and that we are getting the
expected results on the subscriber side
'''


def test_pubsub_command(async_pool, event_loop):
    def generate(max):
        for i in range(max):
            yield f"message number {i}"

    messages = [a for a in generate(5)]
    success, message = event_loop.run_until_complete(
        run_pubsub(async_pool, messages, "channel-1"))
    assert success, message
