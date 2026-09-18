"""
SCRATCH verification test for the CI log-demultiplexing fix (dedup-ci-logs branch).
Not meant to stay in the permanent suite -- deliberately fails so the failure-path
log dumping can be inspected by hand. Delete before merging.
"""

import asyncio
import logging

from redis import asyncio as aioredis

from .instance import DflyInstanceFactory


async def _hammer(port, n=4000):
    client = aioredis.Redis(port=port)
    for i in range(n):
        await client.set(f"k{i}", "v" * 50)
    await client.aclose()


async def test_zz_concurrent_instances_clean_logs(df_factory: DflyInstanceFactory):
    """Start two instances, generate heavy concurrent traffic/log output on both at the
    same time, then force a failure so copy_failed_logs() dumps both instances' raw
    console logs. Inspect the pytest failure output by hand: each instance's log should
    appear as one contiguous, readable block (not interleaved line-by-line)."""
    a = df_factory.create(vmodule="dragonfly_connection=2")
    b = df_factory.create(vmodule="dragonfly_connection=2")
    a.start()
    b.start()

    logging.info(f"started instance a on port {a.port}, instance b on port {b.port}")

    await asyncio.gather(_hammer(a.port), _hammer(b.port))

    logging.info("both instances finished hammering, stopping and forcing a failure now")

    a.stop()
    b.stop()

    assert False, "intentional failure to trigger copy_failed_logs()"
