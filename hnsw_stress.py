#!/usr/bin/env python3

import argparse
import asyncio
import json
import random
import signal
import time

import redis.asyncio as aioredis

BATCH_SIZE = 50


async def writer(client: aioredis.Redis, writer_id: int, stop: asyncio.Event, args):
    i = 0
    written = 0
    while not stop.is_set():
        pipe = client.pipeline(transaction=False)
        for _ in range(BATCH_SIZE):
            key = f"d:{writer_id}:{i}"
            doc = json.dumps({"v": [float(i) + writer_id * 0.1], "count": i})
            pipe.execute_command("JSON.SET", key, "$", doc)
            if i % 2 == 0:
                pipe.pexpire(key, random.randint(args.ttl_min, args.ttl_max))
            i += 1
        await pipe.execute()
        written += BATCH_SIZE
        await asyncio.sleep(random.uniform(0.001, 0.01))
    return "writer", writer_id, written


async def overwriter(client: aioredis.Redis, stop: asyncio.Event, args):
    count = 0
    while not stop.is_set():
        pipe = client.pipeline(transaction=False)
        for _ in range(BATCH_SIZE):
            wid = random.randint(0, args.writers - 1)
            kid = random.randint(0, 5000)
            key = f"d:{wid}:{kid}"
            doc = json.dumps({"v": [random.random()], "count": kid})
            pipe.execute_command("JSON.SET", key, "$", doc)
            if random.random() < 0.5:
                pipe.pexpire(key, random.randint(args.ttl_min, args.ttl_max))
        try:
            await pipe.execute()
            count += BATCH_SIZE
        except Exception:
            pass
        await asyncio.sleep(random.uniform(0.001, 0.02))
    return "overwriter", 0, count


async def deleter(client: aioredis.Redis, stop: asyncio.Event, args):
    count = 0
    while not stop.is_set():
        pipe = client.pipeline(transaction=False)
        for _ in range(BATCH_SIZE):
            wid = random.randint(0, args.writers - 1)
            kid = random.randint(0, 5000)
            pipe.delete(f"d:{wid}:{kid}")
        try:
            await pipe.execute()
            count += BATCH_SIZE
        except Exception:
            pass
        await asyncio.sleep(random.uniform(0.001, 0.02))
    return "deleter", 0, count


async def status_printer(client: aioredis.Redis, stop: asyncio.Event):
    start = time.time()
    while not stop.is_set():
        await asyncio.sleep(10)
        try:
            info = await client.execute_command("FT.INFO", "i1")
            info_dict = dict(zip(info[::2], info[1::2]))
            num_docs = info_dict.get(b"num_docs", "?")
            if isinstance(num_docs, bytes):
                num_docs = num_docs.decode()
            elapsed = int(time.time() - start)
            dbsize = await client.dbsize()
            print(f"[{elapsed:>4}s] dbsize={dbsize} indexed_docs={num_docs}")
        except Exception as e:
            print(f"  status error: {e}")
    return "status", 0, 0


async def main():
    parser = argparse.ArgumentParser(description="HNSW expire stress test")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--writers", type=int, default=16)
    parser.add_argument("--overwriters", type=int, default=4)
    parser.add_argument("--deleters", type=int, default=4)
    parser.add_argument("--ttl-min", type=int, default=100, help="Min TTL in ms")
    parser.add_argument("--ttl-max", type=int, default=500, help="Max TTL in ms")
    args = parser.parse_args()

    client = aioredis.Redis(port=args.port, decode_responses=False)
    try:
        await client.execute_command("FT.DROPINDEX", "i1", "DD")
        print("Dropped existing index i1")
        await asyncio.sleep(1)
    except Exception as e:
        print(f"No existing index to drop: {e}")
    await client.flushall()

    print(f"Creating HNSW index on port {args.port}...")
    await client.execute_command(
        "FT.CREATE",
        "i1",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "$.v",
        "AS",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "1",
        "DISTANCE_METRIC",
        "L2",
        "$.count",
        "AS",
        "count",
        "NUMERIC",
    )

    stop = asyncio.Event()

    def signal_handler():
        print("\nStopping...")
        stop.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)

    print(
        f"Starting {args.writers} writers + {args.overwriters} overwriters + {args.deleters} deleters"
    )
    print(f"TTL range: {args.ttl_min}-{args.ttl_max}ms")

    tasks = []
    for wid in range(args.writers):
        tasks.append(asyncio.create_task(writer(client, wid, stop, args)))
    for _ in range(args.overwriters):
        tasks.append(asyncio.create_task(overwriter(client, stop, args)))
    for _ in range(args.deleters):
        tasks.append(asyncio.create_task(deleter(client, stop, args)))
    tasks.append(asyncio.create_task(status_printer(client, stop)))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, tuple):
            role, wid, count = r
            if count:
                print(f"  {role}[{wid}]: {count} ops")

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
