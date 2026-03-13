import logging
import threading
from redis import asyncio as aioredis

import pytest
from celery import Celery
from celery.contrib.testing.worker import (
    setup_app_for_worker,
    TestWorkController,
    _set_task_join_will_block,
)


def _process_job(job_id):
    return f"Worker successfully processed job {job_id}"


@pytest.fixture
def celery_app(df_server):
    broker_url = f"redis://localhost:{df_server.port}/0"
    app = Celery("dragonfly_test", broker=broker_url, backend=broker_url)
    app.conf.task_default_queue = "my_queue"

    app.task(name="process_job")(_process_job)
    yield app

    # Prevent AsyncResult.__del__ on leftover task objects from pinging
    # the Redis backend after the server has already been shut down.
    if hasattr(app, "backend"):
        app.backend.remove_pending_result = lambda *args, **kwargs: None

    app.close()


@pytest.fixture
def celery_worker(celery_app):
    """Teardown order: celery_worker -> celery_app -> df_server,
    so the worker stops while Dragonfly is still running."""
    setup_app_for_worker(celery_app, loglevel="INFO", logfile=None)
    worker = TestWorkController(
        app=celery_app,
        concurrency=1,
        pool="solo",
        loglevel="INFO",
        without_heartbeat=True,
        without_mingle=True,
        without_gossip=True,
    )
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    worker.ensure_started()
    # Explicitly allow tests to call .get() on tasks. By default, Celery's eager
    # test worker will block and raise an error if you try to get results from
    # within what it perceives to be a worker context to prevent deadlocks.
    _set_task_join_will_block(False)
    yield worker

    # Must explicitly stop the daemon to prevent it from entering a tight
    # reconnection spin loop when the test abruptly destroys the Redis socket.
    worker.stop()
    t.join()


async def test_celery_push_jobs(async_client: aioredis.Redis, celery_app):
    process_job = celery_app.tasks["process_job"]

    results = []
    for i in range(0, 200):
        results.append(process_job.delay(f"job{i}"))

    queue_len = await async_client.llen("my_queue")
    assert queue_len == 200


def test_celery_inspect(celery_app, celery_worker):
    process_job = celery_app.tasks["process_job"]
    inspector = celery_app.control.inspect()

    # Worker should be alive
    ping = inspector.ping()
    logging.info(f"Ping response: {ping}")
    assert len(ping) == 1

    # Our task should be registered
    registered = inspector.registered()
    worker_name = list(registered.keys())[0]
    task_names = registered[worker_name]
    assert "process_job" in task_names

    # Check active queues
    queues = inspector.active_queues()
    assert queues is not None
    queue_names = [q["name"] for q in queues[worker_name]]
    assert "my_queue" in queue_names

    # Check stats
    stats = inspector.stats()
    logging.info(f"Stats response: {stats}")
    assert worker_name in stats
