import asyncio
import logging

import aiohttp
import pytest
from redis import asyncio as aioredis

from . import dfly_args
from .instance import DflyInstance
from .seeder import Seeder as SeederV2


async def get_stat(async_client: aioredis.Redis, name: str) -> int:
    info = await async_client.info("stats")
    return info.get(name, 0)


async def set_flag(df_server: DflyInstance, flag: str, value: str):
    async with aiohttp.ClientSession() as session:
        resp = await session.get(
            f"http://localhost:{df_server.port}/flagz?flag={flag}&value={value}"
        )
        assert resp.status == 200


@pytest.mark.opt_only
@dfly_args({"proactor_threads": "1"})
async def test_defrag_flags_are_not_cached(df_server: DflyInstance, async_client: aioredis.Redis):
    key_target = 390_000
    seeder = SeederV2(
        units=8, key_target=key_target, data_size=10_000, collection_size=100, types=["ZSET"]
    )
    await seeder.run(async_client, target_deviation=0.05)
    logging.info(f"[defrag_test] seeded dbsize={await async_client.dbsize()}")

    # Delete 90% of keys
    keys_to_delete = []
    async for key in async_client.scan_iter(match="*", count=1000):
        keys_to_delete.append(key)
    keys_to_delete = [k for i, k in enumerate(keys_to_delete) if i % 10 != 0]
    logging.info(f"[defrag_test] deleting {len(keys_to_delete)} keys")
    for i in range(0, len(keys_to_delete), 5000):
        await async_client.delete(*keys_to_delete[i : i + 5000])

    baseline = await get_stat(async_client, "defrag_task_invocation_total")
    assert baseline == 0
    logging.info(f"[defrag_test] baseline defrag_invocations={baseline} - settling for 4s now")

    await asyncio.sleep(4)

    logging.info("[defrag_test] opening defrag gates via /flagz - watch cpu spike now")

    # Actually start testing defragmentation
    await set_flag(df_server, "mem_defrag_threshold", "0.0")
    await set_flag(df_server, "mem_defrag_check_sec_interval", "0")
    await set_flag(df_server, "mem_defrag_waste_threshold", "0.01")

    # Cap the duty cycle at ~1%: burst of 10ms real time, then a 990ms cooldown -
    # duty_cycle = 10ms / (10ms + 990ms) = 1%.
    await set_flag(df_server, "mem_defrag_max_burst_duration_us", "10000")
    await set_flag(df_server, "mem_defrag_backoff_duration_us", "990000")
    logging.info("[defrag_test] duty-cycle cap armed: target ~1% of one core")

    # Check the impact every 25 seconds, printing the delta between each so the spike is visible
    # as it happens rather than only checked once at the end.
    prev_invocations = 0
    total_invocations = 0
    for checkpoint in (25, 50, 75):
        await asyncio.sleep(25)
        total_invocations = await get_stat(async_client, "defrag_task_invocation_total")
        delta = total_invocations - prev_invocations
        prev_invocations = total_invocations
        logging.info(
            f"[defrag_test] t={checkpoint}s defrag_invocations_total={total_invocations:.0f} "
            f"delta_last_25s={delta:.0f}"
        )

    final_moved = await get_stat(async_client, "defrag_realloc_total")
    logging.info(
        f"[defrag_test] defrag_invocations={total_invocations} defrag_objects_moved={final_moved}"
    )

    # At a 1% duty cycle (~1 burst/sec, each burst fitting however many calls the CPU can do in
    # ~10ms) we expect nowhere near the tens of thousands seen with the cap disabled - but the
    # exact count scales with per-call cost, which varies with CPU speed (faster CI hardware fits
    # more calls into the same time-bounded burst). Generous upper bound to absorb that variance
    # while still clearly distinguishing "capped" from "unbounded spin".
    assert 0 < total_invocations < 12_000, f"{total_invocations} invocations - cap not effective"

    # And confirm real reallocation work actually happened
    assert final_moved > 0
