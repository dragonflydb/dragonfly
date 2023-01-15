import asyncio
import aioredis
import random
import string
import json


async def main():

    def rand_str(k=3, s=''):
        # Use small k value to reduce mem usage and increase number of ops
        return s.join(random.choices(string.ascii_letters, k=k))

    ints = [{"i": random.randint(0, 100)}
                    for i in range(2)]
    strs = [rand_str() for _ in range(2)]
    obj = json.dumps({"arr": strs, "ints": ints, "i": random.randint(0, 100)})
    jc =  f"JSON.SET doc $ '{obj}'"
    print(jc)

    c = aioredis.Redis()
    await c.execute_command("JSON.SET", "doc", "$", obj)

asyncio.run(main())
