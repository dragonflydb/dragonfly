import asyncio
import json
import random

import redis.asyncio as aioredis

from .instance import DflyInstanceFactory

NUM_WRITERS = 8
KEYS_PER_WRITER = 2000
TTL_MS_MIN = 100
TTL_MS_MAX = 500
DURATION_SEC = 120
BATCH_SIZE = 50


async def _writer(client: aioredis.Redis, writer_id: int, stop_event: asyncio.Event):
    i = 0
    while not stop_event.is_set():
        pipe = client.pipeline(transaction=False)
        for _ in range(BATCH_SIZE):
            key = f"d:{writer_id}:{i}"
            doc = json.dumps({"v": [float(i) + writer_id * 0.1], "count": i})
            pipe.execute_command("JSON.SET", key, "$", doc)
            if i % 2 == 0:
                pipe.pexpire(key, random.randint(TTL_MS_MIN, TTL_MS_MAX))
            i += 1
        await pipe.execute()
        await asyncio.sleep(0.01)


async def _overwriter(client: aioredis.Redis, stop_event: asyncio.Event):
    while not stop_event.is_set():
        pipe = client.pipeline(transaction=False)
        for _ in range(BATCH_SIZE):
            writer_id = random.randint(0, NUM_WRITERS - 1)
            key_id = random.randint(0, KEYS_PER_WRITER - 1)
            key = f"d:{writer_id}:{key_id}"
            doc = json.dumps({"v": [random.random()], "count": key_id})
            pipe.execute_command("JSON.SET", key, "$", doc)
            if random.random() < 0.5:
                pipe.pexpire(key, random.randint(TTL_MS_MIN, TTL_MS_MAX))
        try:
            await pipe.execute()
        except Exception:
            pass
        await asyncio.sleep(0.02)


async def test_hnsw_expire_label_not_found(df_factory: DflyInstanceFactory):
    server = df_factory.create(proactor_threads=16)
    server.start()

    client = aioredis.Redis(port=server.port, decode_responses=False)
    await client.flushall()
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

    stop_event = asyncio.Event()

    tasks = []
    for wid in range(NUM_WRITERS):
        tasks.append(asyncio.create_task(_writer(client, wid, stop_event)))
    tasks.append(asyncio.create_task(_overwriter(client, stop_event)))

    await asyncio.sleep(DURATION_SEC)
    stop_event.set()

    await asyncio.gather(*tasks)

    await asyncio.sleep(2)

    await client.aclose()
    server.stop()

    warnings = server.find_in_logs(r"Label not found")
    if warnings:
        print(f"\n*** Found {len(warnings)} warnings ***")
        for w in warnings[:20]:
            print(f"  {w.strip()}")

    assert len(warnings) == 0, (
        f"Found {len(warnings)} 'Label not found' warnings in logs. "
        "HNSW remove during key expiration is broken."
    )
