#!/usr/bin/env python3
"""
  * producer (async task): calls the real `process_job.delay()` continuously
  * consumer (async task): drives a `celery worker` subprocess. The worker
    is paused with SIGSTOP so the queue builds, then every `--drain-every`
    seconds it is resumed (SIGCONT) for a `--drain-window` burst during which it
    actually BRPOPs + executes tasks (decompressing nodes on read), then paused
    again

Usage:
    python churn.py --produce-rate 1500 --drain-every 20 --drain-window 8
"""

import argparse
import asyncio
import os
import signal
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as aioredis

from celery_app import process_job

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER_LOG = "/tmp/celery_zstd_worker.log"


_WAREHOUSES = ["us-east-1", "us-west-2", "eu-central-1", "ap-south-1"]
_STATUSES = ["pending", "processing", "shipped", "backordered"]


def make_payload(i, n_items):
    """Structured task payload. With n_items>0 it carries a list of records with a
    fixed schema but realistically varying values (incl. a random per-record id),
    like a real batch task - compressible via the shared structure, not
    artificially so."""
    p = {"user_id": i % 10000, "action": "process", "retries": 0, "queue": "my_queue", "attempt": 1}
    if n_items:
        p["items"] = [
            {
                "sku": f"SKU-{(i * 7 + k * 13) % 1000000:07d}",
                "qty": (k % 9) + 1,
                "price": f"{((i + k) % 5000) / 100:.2f}",
                "warehouse": _WAREHOUSES[(i + k) % 4],
                "status": _STATUSES[(i + k) % 4],
                "line_id": str(uuid.uuid4()),
            }
            for k in range(n_items)
        ]
    return p


def publish_batch(start_i, n, n_items):
    """Synchronously publish n real Celery tasks (runs in a worker thread)."""
    for j in range(n):
        i = start_i + j
        process_job.delay(
            job_id=f"b3e4b923-8a77-4053-aff0-{100000 + i}",
            payload=make_payload(i, n_items),
        )
    return start_i + n


async def _sleep_or_stop(stop, secs):
    try:
        await asyncio.wait_for(stop.wait(), timeout=secs)
    except asyncio.TimeoutError:
        pass
    return stop.is_set()


def _signal_worker(worker, sig):
    """Send a signal to the worker, tolerating an already-dead process."""
    if worker.returncode is None:
        try:
            worker.send_signal(sig)
        except ProcessLookupError:
            pass


async def producer(stop, args, pool):
    """Bursty producer: publish a spike of tasks, then pause, repeat. Note: real
    Celery .delay() is the speed ceiling (~1-2k/s single-thread), so a large spike
    is smeared over a few seconds rather than instantaneous - use sawtooth.py for
    sharp spikes."""
    loop = asyncio.get_running_loop()
    i = 0
    while not stop.is_set():
        produced = 0
        while produced < args.produce_burst and not stop.is_set():
            chunk = min(500, args.produce_burst - produced)
            i = await loop.run_in_executor(pool, publish_batch, i, chunk, args.payload_items)
            produced += chunk
        print(f"[produce] spike of {produced:,}")
        if await _sleep_or_stop(stop, args.produce_pause):  # pause between spikes
            break


async def start_worker(args):
    logf = open(WORKER_LOG, "w")
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "celery",
        "-A",
        "celery_app",
        "worker",
        "--pool=solo",
        "-Q",
        args.queue,
        "--loglevel=INFO",
        "--without-gossip",
        "--without-mingle",
        "--without-heartbeat",
        "--prefetch-multiplier=1",  # keep the backlog in Dragonfly, not the worker
        cwd=HERE,
        start_new_session=True,  # shield it from terminal Ctrl-C; we drive it
        stdout=logf,
        stderr=logf,
    )


async def consumer(stop, args, worker):
    await asyncio.sleep(args.worker_boot)  # let the worker connect first
    if worker.returncode is not None:
        print(f"[worker] exited early (rc={worker.returncode}); see {WORKER_LOG}")
        return
    _signal_worker(worker, signal.SIGSTOP)
    print("[worker] paused; queue will build")
    while not stop.is_set():
        if await _sleep_or_stop(stop, args.drain_every):  # build-up phase
            break
        _signal_worker(worker, signal.SIGCONT)
        print("[worker] resumed - draining")
        if await _sleep_or_stop(stop, args.drain_window):
            break
        _signal_worker(worker, signal.SIGSTOP)
        print("[worker] paused")


async def monitor(stop, r, args):
    while not stop.is_set():
        llen = await r.llen(args.queue)
        used = (await r.info("memory")).get("used_memory", 0)
        print(f"[mon]  LLEN({args.queue})={llen:>9,}  used_memory={used / 1024 / 1024:6.1f} MiB")
        if await _sleep_or_stop(stop, args.monitor_every):
            break


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--queue", default="celery")
    ap.add_argument(
        "--produce-burst",
        type=int,
        default=20000,
        help="tasks per producer spike (real .delay() is the speed ceiling)",
    )
    ap.add_argument(
        "--produce-pause",
        type=float,
        default=5.0,
        help="seconds the producer pauses between spikes (0 = continuous)",
    )
    ap.add_argument(
        "--payload-items",
        type=int,
        default=0,
        help="structured records per task: 0=~1KB, 50=~12KB, 100=~22KB each",
    )
    ap.add_argument(
        "--drain-every", type=float, default=20.0, help="build-up seconds between worker wake-ups"
    )
    ap.add_argument(
        "--drain-window", type=float, default=8.0, help="seconds the worker drains on each wake-up"
    )
    ap.add_argument(
        "--worker-boot",
        type=float,
        default=5.0,
        help="seconds to let the worker boot before first pause",
    )
    ap.add_argument("--monitor-every", type=float, default=2.0)
    args = ap.parse_args()

    r = aioredis.Redis(host=args.host, port=args.port, db=0)
    pool = ThreadPoolExecutor(max_workers=1)  # serialize real .delay() calls
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    print(
        f"real Celery: producer spikes of {args.produce_burst:,} (pause {args.produce_pause}s); "
        f"worker drains {args.drain_window}s every {args.drain_every}s. "
        f"Worker log: {WORKER_LOG}. Ctrl-C to stop."
    )
    worker = await start_worker(args)
    try:
        await asyncio.gather(
            producer(stop, args, pool), consumer(stop, args, worker), monitor(stop, r, args)
        )
    finally:
        # Resume (so it can act on terminate), then shut the worker down cleanly.
        _signal_worker(worker, signal.SIGCONT)
        if worker.returncode is None:
            worker.terminate()
            try:
                await asyncio.wait_for(worker.wait(), timeout=10)
            except asyncio.TimeoutError:
                worker.kill()
        pool.shutdown(wait=False)
        try:
            await r.aclose()
        except AttributeError:
            await r.close()
    print("stopped.")


if __name__ == "__main__":
    asyncio.run(main())
