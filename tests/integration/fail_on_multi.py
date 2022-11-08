#!/usr/bin/env python3

import asyncio
import aioredis
import async_timeout

DB_INDEX = 1


async def run_pipeline_mode(pool, messages):
    try:
        conn = aioredis.Redis(connection_pool=pool)
        pipe = conn.pipeline()
        for key, val in messages.items():
            pipe.set(key, val)
   #     pipe.get(key)
        pipe.set("foo", "bar", "EX", "oiuqwer") 
        pipe.eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", 2, 'key1', 'key2', 'first', 'second')
        result = await pipe.execute()

        print(f"got result from the pipeline of {result} with len = {len(result)}")
        if len(result) != len(messages):
            return False, f"number of results from pipe {len(result)} != expected {len(messages)}"
        elif False in result:
            return False, "expecting to successfully get all result good, but some failed"
        else:
            return True, "all command processed successfully"
    except Exception as e:
        print(f"failed to run command: error: {e}")
        return False, str(e)


def test_pipeline_support():
    def generate(max):
        for i in range(max):
            yield f"key{i}", f"value={i}"

    messages = {a: b for a, b in generate(1)}
    loop = asyncio.new_event_loop()
    async_pool = aioredis.ConnectionPool(host="localhost", port=6379,
                                         db=DB_INDEX, decode_responses=True, max_connections=16)
    success, message = loop.run_until_complete(
        run_pipeline_mode(async_pool, messages))
    #assert success, message
    return success


if __name__ == "__main__":
    print("starting the test")
    state = test_pipeline_support()
    print(f"finish successfully - {state}")

