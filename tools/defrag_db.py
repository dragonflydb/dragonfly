import redis.asyncio as aioredis
import argparse
import asyncio

"""
This script iterates over all keys and "recycles" them.
Recycling is done by DUMPing the key first and then re-creating it with EXPIRE.
This will trigger re-allocation of internal data structures in order to reduce
memory fragmentation.
"""

SCRIPT = """
local recycled = 0
for _, key in ipairs(KEYS) do
    local ttl = redis.call('PTTL', key)
    local dumpedData = redis.call('DUMP', key)

    if dumpedData then
        redis.call('RESTORE', key, 0, dumpedData, 'REPLACE')
        if ttl > 0 then
            redis.call('PEXPIRE', key, ttl)
        end
        recycled = recycled + 1
    end
end
return recycled
"""

total_recycled = 0


async def workerfn(client_supplier, sha, queue):
    global total_recycled

    r = client_supplier()
    while True:
        keys = await queue.get()

        try:
            recycled = await r.evalsha(sha, len(keys), *keys)
        except Exception as e:
            raise SystemExit(e)

        if isinstance(recycled, int):
            total_recycled += recycled
        else:
            print("Error recycling", recycled)

        queue.task_done()


async def infofn():
    while True:
        await asyncio.sleep(0.5)
        print("Keys processed:", total_recycled)


async def main(client_supplier, scan_type, num_workers, queue_size, batch_size):
    r = client_supplier()
    sha = await r.script_load(SCRIPT)
    queue = asyncio.Queue(maxsize=queue_size)

    workers = [
        asyncio.create_task(workerfn(client_supplier, sha, queue)) for _ in range(num_workers)
    ]
    info_worker = asyncio.create_task(infofn())

    keys = []
    async for key in r.scan_iter("*", count=batch_size * 2, _type=scan_type):
        keys.append(key)
        if len(keys) >= batch_size:
            await queue.put(keys)
            keys = []

    await queue.put(keys)
    await queue.join()

    info_worker.cancel()
    for w in workers:
        w.cancel()

    await asyncio.gather(*workers, info_worker, return_exceptions=True)
    print("Recycled in total:", total_recycled)


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--workers", type=int, default=8)
arg_parser.add_argument("--batch", type=int, default=20)

arg_parser.add_argument(
    "--type", type=str, default=None, help="Process keys only of specified type"
)

arg_parser.add_argument("--db", type=int)
arg_parser.add_argument("--port", type=int, default=6379)
arg_parser.add_argument("--host", type=str, default="localhost")
args = arg_parser.parse_args()


def client_supplier():
    return aioredis.StrictRedis(db=args.db, port=args.port, host=args.host)


asyncio.run(main(client_supplier, args.type, args.workers, args.workers * 2, args.batch))
