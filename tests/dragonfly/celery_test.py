import logging
import time
import threading
from redis import asyncio as aioredis

import pytest
from celery import Celery, group
from celery.result import allow_join_result
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
    # Use a timeout because the worker thread may be blocked on socket.recv()
    # in the kombu event loop and never notice the stop flag.
    # The thread is a daemon, so it will be cleaned up on process exit.
    t.join(timeout=10)


async def test_celery_push_jobs(async_client: aioredis.Redis, celery_app):
    process_job = celery_app.tasks["process_job"]

    results = []
    for i in range(0, 200):
        results.append(process_job.delay(f"job{i}"))

    queue_len = await async_client.llen("my_queue")
    assert queue_len == 200
    mem_usage = await async_client.memory_usage("my_queue")
    logging.info(f"Queue 'my_queue' MEMORY USAGE: {mem_usage:,} bytes ({queue_len} jobs)")


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


# Reproducer for #7056: PUBLISH lost under concurrent Celery group().get().


def _noop_task(i):
    time.sleep(0.5)
    return i


def _group_get_task(batch_size, get_timeout=5):
    """Dispatch a group of subtasks and call .get() — the pattern that triggers the bug."""
    from celery import current_app

    with allow_join_result():
        g = group(current_app.tasks["noop"].s(i) for i in range(batch_size))()
        try:
            g.get(timeout=get_timeout)
            return 0
        except Exception:
            import redis

            conn = redis.Redis.from_url(current_app.conf.result_backend)
            keys = [f"celery-task-meta-{r.id}" for r in g.results]
            stored = sum(1 for v in conn.mget(keys) if v is not None)
            conn.close()
            return stored  # tasks completed but PUBLISH was lost
        finally:
            g.revoke()
            g.forget()


@pytest.fixture
def pubsub_celery_app(df_server):
    broker = f"redis://localhost:{df_server.port}/0"
    backend = f"redis://localhost:{df_server.port}/1"
    app = Celery("pubsub_test", broker=broker, backend=backend)
    app.conf.update(task_serializer="json", result_serializer="json", accept_content=["json"])
    app.task(name="noop")(_noop_task)
    app.task(name="group_get", bind=False)(_group_get_task)
    yield app

    if hasattr(app, "backend"):
        app.backend.remove_pending_result = lambda *args, **kwargs: None
    app.close()


@pytest.fixture
def pubsub_worker(pubsub_celery_app):
    # Python 3 raises EOFError (not IOError) on broken pipes, but
    # _help_stuff_finish only catches IOError, producing a spurious traceback
    # during pool cleanup. Assign directly so the patch outlives fixture teardown.
    try:
        from celery.concurrency.asynpool import AsynPool

        orig = AsynPool._help_stuff_finish.__func__

        @classmethod
        def _silent(cls, *args, **kwargs):
            try:
                orig(cls, *args, **kwargs)
            except EOFError:
                pass

        AsynPool._help_stuff_finish = _silent
    except Exception:
        pass

    setup_app_for_worker(pubsub_celery_app, loglevel="WARNING", logfile=None)
    w = TestWorkController(
        app=pubsub_celery_app,
        concurrency=32,
        pool="prefork",
        loglevel="WARNING",
        without_heartbeat=True,
        without_mingle=True,
        without_gossip=True,
    )
    t = threading.Thread(target=w.start, daemon=True)
    t.start()
    w.ensure_started()
    _set_task_join_will_block(False)
    yield w

    pubsub_celery_app.control.purge()
    w.stop()
    t.join(timeout=10)


@pytest.mark.large
@pytest.mark.opt_only
def test_pubsub_publish_not_lost_in_celery(pubsub_celery_app, pubsub_worker):
    """#7056: PUBLISH notifications must not be silently lost under Celery group().get()."""
    dispatch = pubsub_celery_app.tasks["group_get"]
    BATCH, CONCURRENT, ROUNDS, TIMEOUT = 10, 8, 5, 5

    total_lost = 0
    for rnd in range(1, ROUNDS + 1):
        results = [dispatch.delay(BATCH, get_timeout=TIMEOUT) for _ in range(CONCURRENT)]
        round_lost = 0
        for ar in results:
            try:
                round_lost += ar.get(timeout=TIMEOUT + 10)
            except Exception:
                round_lost += BATCH
        assert round_lost == 0, f"Round {rnd}/{ROUNDS}: {round_lost} PUBLISH messages lost"
