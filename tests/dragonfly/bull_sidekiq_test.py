import json
import logging
import time
import uuid

from redis import asyncio as aioredis

# from bullmq import Queue
# from . import dfly_args


# BULLMQ_QUEUE_NAME = "{test_queue}"

# @pytest.fixture
# async def bullmq_queue(df_server):
#     queue = Queue(BULLMQ_QUEUE_NAME, {"connection": {"host": "localhost", "port": df_server.port}})
#     yield queue
#     await queue.close()


# @dfly_args({"lock_on_hashtags": True})
# async def test_bullmq_push_jobs(async_client: aioredis.Redis, bullmq_queue: Queue):
#     """Push 200 jobs and verify they are stored in Dragonfly."""
#     for i in range(200):
#         await bullmq_queue.add(
#             "process_job",
#             {"job_id": f"job{i}", "payload": f"data for job {i}"},
#         )

#     # BullMQ stores waiting jobs in a list key: bull:<queue_name>:wait
#     wait_key = f"bull:{BULLMQ_QUEUE_NAME}:wait"
#     queue_len = await async_client.llen(wait_key)
#     assert queue_len == 200

#     # Verify a job can be read back
#     raw = await async_client.lindex(wait_key, 0)
#     assert raw is not None
#     mem_usage = await async_client.memory_usage(wait_key)
#     logging.info(f"Queue '{wait_key}' MEMORY USAGE: {mem_usage:,} bytes ({queue_len} jobs)")


def _make_sidekiq_job(i: int) -> str:
    """Generate a job payload matching the Sidekiq wire format.

    Verified against sidekiq/lib/sidekiq/client.rb (atomic_push) and
    sidekiq/lib/sidekiq/job_util.rb (normalize_item).
    """
    jid = uuid.uuid4().hex[:24]  # SecureRandom.hex(12)
    now = time.time()  # Time.now.to_f
    return json.dumps(
        {
            "class": "ProcessJobWorker",
            "args": [
                f"job{i}",
                {"user_id": 100000 + i, "action": "process", "priority": "normal"},
            ],
            "retry": True,
            "queue": "default",
            "jid": jid,
            "created_at": now,
            "enqueued_at": now,
        }
    )


async def test_sidekiq_push_jobs(async_client: aioredis.Redis):
    """Push 2000 Sidekiq jobs and verify they are stored correctly."""
    queue_key = "queue:default"
    num_jobs = 2000

    pipe = async_client.pipeline()
    for i in range(num_jobs):
        pipe.lpush(queue_key, _make_sidekiq_job(i))
    await pipe.execute()

    queue_len = await async_client.llen(queue_key)
    assert queue_len == num_jobs

    # Verify readability
    first = await async_client.lindex(queue_key, 0)
    last = await async_client.lindex(queue_key, -1)
    assert first is not None and last is not None
    parsed = json.loads(first)
    assert parsed["class"] == "ProcessJobWorker"

    mem_usage = await async_client.memory_usage(queue_key)
    logging.info(
        f"Queue '{queue_key}' MEMORY USAGE: {mem_usage:,} bytes ({queue_len} Sidekiq jobs)"
    )
