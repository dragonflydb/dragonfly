"""
SCRATCH verification test for CI core dump capture (core-dumps branch). Crashes an
instance via DEBUG VECTOR-OOB (real UB, not DCHECK) so a genuine core is produced, then
forces a failure so the run-regression-tests-helper.sh watcher/collector gets exercised.
Delete before merging.
"""

import contextlib
import logging

import redis
from redis import asyncio as aioredis

from .instance import DflyInstanceFactory


async def test_zz_vector_oob_crash_produces_coredump(df_factory: DflyInstanceFactory):
    a = df_factory.create()
    a.start()
    logging.info(f"started instance pid={a.proc.pid} port={a.port}")

    client = aioredis.Redis(port=a.port)
    with contextlib.suppress(redis.exceptions.ConnectionError, ConnectionResetError):
        await client.execute_command("DEBUG VECTOR-OOB")
    await client.aclose()

    error = None
    try:
        a.stop()
    except Exception as e:
        error = e

    assert error is not None, "expected instance to have crashed via DEBUG VECTOR-OOB"
    assert False, f"intentional failure to exercise core dump collection: {error}"
