#!/usr/bin/env python3
"""
Celery (via Kombu's Redis transport) models a task queue as a single Redis list:
  - enqueue  = LPUSH <queue> <json-envelope>   (producer, task.delay())
  - dequeue  = BRPOP <queue> ...               (worker)

Every enqueued element is a Kombu message envelope - a JSON blob whose structure
is nearly identical across tasks (only a few fields like id / argsrepr vary). That
structural repetition is exactly what the per-thread ZSTD dictionary exploits.
"""

import os

from celery import Celery

# Dragonfly speaks the Redis protocol, so point Celery's broker straight at it.
BROKER_URL = os.environ.get("DF_BROKER_URL", "redis://localhost:6379/0")

app = Celery("celery_zstd_experiment", broker=BROKER_URL)

app.conf.update(
    # Producer-only test: we only care about the broker queue, not results.
    task_ignore_result=True,
    result_backend=None,
    # Collapse Celery's emulated priority sub-lists into a single Redis list
    # ("celery"). One list => one Dragonfly shard => the cleanest memory signal.
    broker_transport_options={"priority_steps": [0]},
    task_default_queue="celery",
    task_serializer="json",
    accept_content=["json"],
)


@app.task(name="process_job")
def process_job(job_id, payload):
    """Trivial task body. Only its envelope matters"""
    return job_id
