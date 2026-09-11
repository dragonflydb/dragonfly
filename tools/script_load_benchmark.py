#!/usr/bin/env python3
r"""Measure SCRIPT LOAD latency while idle and during HGET-heavy Lua contention.

Use fresh local servers for each before/after run (requires redis>=5.2.1):
  build-opt/dragonfly --port=6380 --proactor_threads=4 --num_shards=1 \
      --interpreter_per_thread=50 --default_lua_flags=allow-undeclared-keys --dbfilename=
  python3 tools/script_load_benchmark.py

Adjust the constants below; COLD forces cache misses. Probes arrive at fixed intervals;
background EVAL traffic then stops and requests drain. Latencies include that drain.
"""

import asyncio
import hashlib
import time

from redis import asyncio as redis


PORT = 6380
EVAL_WORKERS = 512
HGETS_PER_EVAL = 2000
PROBES = 40
PROBE_INTERVAL = 0.25
WARMUP = 3
COLD = False
SCRIPT_BODY = b"return 1\n--".ljust(311005, b"x")

WORKLOAD = """
local value
for i = 1, tonumber(ARGV[1]) do
    value = redis.call('HGET', ARGV[2], 'field')
end
return value
"""


async def main():
    key = f"script_load_benchmark:{time.time_ns()}"
    connections = [
        redis.Redis(
            host="127.0.0.1",
            port=PORT,
            decode_responses=True,
            single_connection_client=True,
            socket_timeout=60,
            socket_connect_timeout=5,
        )
        for _ in range(1 + EVAL_WORKERS + PROBES)
    ]
    admin = connections[0]
    stop = asyncio.Event()
    workers, probes = [], []

    async def load(connection, index):
        script = SCRIPT_BODY + f"\n-- {key}:{index}".encode() if COLD else SCRIPT_BODY
        expected = hashlib.sha1(script).hexdigest()
        started = time.monotonic()
        if await connection.script_load(script) != expected:
            raise RuntimeError("Unexpected SCRIPT LOAD SHA")
        return (time.monotonic() - started) * 1000

    async def worker(connection, sha):
        while not stop.is_set():
            if await connection.evalsha(sha, 0, HGETS_PER_EVAL, key) != "value":
                raise RuntimeError("Unexpected HGET workload result")

    try:
        # Establish sockets before timing so connection setup is excluded.
        await asyncio.gather(*(connection.ping() for connection in connections))
        await admin.hset(key, "field", "value")
        sha = await admin.script_load(WORKLOAD)
        if not COLD:
            await admin.script_load(SCRIPT_BODY)
        idle = [await load(admin, i) for i in range(20)]
        blocked_before = (await admin.info("stats"))["lua_blocked_total"]
        workers = [
            asyncio.create_task(worker(connection, sha))
            for connection in connections[1 : 1 + EVAL_WORKERS]
        ]
        await asyncio.sleep(WARMUP)
        if (await admin.info("stats"))["lua_blocked_total"] <= blocked_before:
            raise RuntimeError("No interpreter contention; increase EVAL_WORKERS or HGETS_PER_EVAL")

        started = time.monotonic()
        for i, connection in enumerate(connections[1 + EVAL_WORKERS :]):
            await asyncio.sleep(max(0, started + i * PROBE_INTERVAL - time.monotonic()))
            probes.append(asyncio.create_task(load(connection, i + 20)))
        await asyncio.sleep(max(0, started + PROBES * PROBE_INTERVAL - time.monotonic()))
        stop.set()
        contended = await asyncio.wait_for(asyncio.gather(*probes), 60)
        await asyncio.wait_for(asyncio.gather(*workers), 60)
        await admin.delete(key)
        for name, values in (("idle", idle), ("contended", contended)):
            print(f"{name}: mean {sum(values) / len(values):.2f} ms, max {max(values):.2f} ms")
    finally:
        stop.set()
        for task in workers + probes:
            task.cancel()
        await asyncio.gather(*workers, *probes, return_exceptions=True)
        await asyncio.gather(*(connection.aclose() for connection in connections))


if __name__ == "__main__":
    asyncio.run(main())
