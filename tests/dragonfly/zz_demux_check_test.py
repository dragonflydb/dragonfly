"""
SCRATCH verification test for the CI log-demultiplexing fix (dedup-ci-logs branch).
Not meant to stay in the permanent suite -- deliberately crashes both instances so the
failure-path log dumping can be inspected by hand. Delete before merging.

Uses `DEBUG SHARD-CRASH` (src/server/debugcmd.cc, test-only, added for this branch): on
every shard concurrently it logs a few LOG(ERROR) lines then DCHECK-fails, so several
shard/proactor threads in ONE process crash at nearly the same wall-clock moment (the
"single process, multiple threads" scenario), while also exercising the ERROR-level
console path (glog mirrors ERROR+ to stderr by default, no --alsologtostderr needed).
Doing this on two concurrent DflyInstances at once also exercises the "multiple
processes" interleaving scenario in the same run.
"""

import asyncio
import contextlib
import logging

import redis
from redis import asyncio as aioredis

from .instance import DflyInstanceFactory


async def _crash(port):
    client = aioredis.Redis(port=port)
    with contextlib.suppress(redis.exceptions.ConnectionError, ConnectionResetError):
        await client.execute_command("DEBUG SHARD-CRASH")
    await client.aclose()


async def test_zz_concurrent_instances_clean_logs(df_factory: DflyInstanceFactory):
    """Start two instances, crash both concurrently via DEBUG SHARD-CRASH, then force a
    failure so copy_failed_logs() dumps both instances' raw console logs. Inspect the
    pytest failure output by hand: each instance's log should appear as one contiguous,
    readable block (not interleaved with the other instance's), the LOG(ERROR) lines
    from every shard must all be present (not deduped away), and the DCHECK stack trace
    (live-symbolized by the crashing process itself) should show up cleanly."""
    a = df_factory.create()
    b = df_factory.create()
    a.start()
    b.start()

    logging.info(
        f"started instance a pid={a.proc.pid} port={a.port}, "
        f"instance b pid={b.proc.pid} port={b.port}"
    )

    await asyncio.gather(_crash(a.port), _crash(b.port))

    logging.info("both instances asked to crash, stopping now (expect non-zero exit)")

    errors = []
    for inst in (a, b):
        try:
            inst.stop()
        except Exception as e:
            errors.append(e)

    assert not errors, f"instances crashed as expected via DEBUG SHARD-CRASH: {errors}"
