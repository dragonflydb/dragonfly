import argparse
import time
import threading
import random
import redis
import celery.result

from celery import Celery, group
from celery.result import allow_join_result


# Disable Celery's block guard so group_get can call .get() inside a worker.
def _no_block_check(*args, **kwargs):
    return


celery.result.assert_will_not_block = _no_block_check


# ------------------------
# CONFIG
# ------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Celery pub/sub test with worker warmup and metrics"
    )
    parser.add_argument("--broker", default="redis://localhost:6379/0", help="Celery broker URL")
    parser.add_argument(
        "--backend", default="redis://localhost:6379/1", help="Celery result backend URL"
    )
    parser.add_argument("--batch-size", type=int, default=50, help="Number of subtasks per group")
    parser.add_argument("--rounds", type=int, default=5, help="Number of rounds to run")
    parser.add_argument(
        "--get-timeout", type=int, default=5, help="Timeout for group.get() inside the task"
    )
    parser.add_argument(
        "--concurrent", type=int, default=16, help="Concurrent dispatch_group tasks per round"
    )
    parser.add_argument(
        "--extra-poll", type=int, default=30, help="Seconds to keep polling instance after timeout"
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducible test ordering"
    )
    return parser.parse_args()


args = parse_args()

if args.seed is not None:
    random.seed(args.seed)

REDIS_BROKER = args.broker
REDIS_BACKEND = args.backend

BATCH = args.batch_size
CONCURRENT = args.concurrent
ROUNDS = args.rounds
TIMEOUT = args.get_timeout
EXTRA_POLL = args.extra_poll


# ------------------------
# CELERY APP
# ------------------------

app = Celery("pubsub_test", broker=REDIS_BROKER, backend=REDIS_BACKEND)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    result_expires=300,
    worker_prefetch_multiplier=4,
)


# ------------------------
# TASK
# ------------------------


@app.task(name="noop")
def noop(i):
    time.sleep(0.02)
    return {"i": i, "published_at": time.time()}


# ------------------------
# GROUP TASK
# ------------------------


@app.task(name="group_get", bind=True, soft_time_limit=120, time_limit=130)
def group_get(self, batch_size, get_timeout=5):
    with allow_join_result():
        dispatched_at = time.time()
        g = group(noop.s(i) for i in range(batch_size))()

        try:
            results = g.get(timeout=get_timeout)
            # Wall-clock time when g.get() unblocked — i.e. the last PUBLISH arrived.
            received_at = time.time()

            published_times = [res["published_at"] for res in results]
            last_published = max(published_times)

            return {
                "lost": 0,
                "not_completed": 0,
                # Total time from group dispatch to all results received.
                "group_rtt_ms": (received_at - dispatched_at) * 1000,
                # Time from when the last noop task stored its result (sent PUBLISH)
                # to when group_get received the notification. This is the true
                # PUBLISH round-trip latency for the bottleneck task.
                "tail_latency_ms": (received_at - last_published) * 1000,
            }

        except Exception:
            timed_out_at = time.time()
            conn = redis.Redis.from_url(REDIS_BACKEND)
            keys = [f"celery-task-meta-{r.id}" for r in g.results]

            # Snapshot Redis right at timeout — tasks already here finished before the
            # timeout but their PUBLISH was silently dropped (the actual bug).
            snapshot = conn.mget(keys)
            publish_lost = sum(1 for v in snapshot if v is not None)

            # Poll for the remaining tasks to distinguish slow tasks from truly lost ones.
            deadline = timed_out_at + EXTRA_POLL
            while time.time() < deadline:
                values = conn.mget(keys)
                if all(v is not None for v in values):
                    break
                time.sleep(0.05)
            else:
                values = conn.mget(keys)

            polled_at = time.time()
            conn.close()

            # Tasks not in Redis at timeout but present after polling — just slow workers.
            slow_tasks = sum(1 for s, v in zip(snapshot, values) if s is None and v is not None)
            # Still missing even after EXTRA_POLL seconds — truly stuck or lost.
            truly_missing = sum(1 for v in values if v is None)
            # How much longer after the timeout it took for remaining tasks to appear.
            extra_wait_ms = (polled_at - timed_out_at) * 1000 if truly_missing == 0 else None

            return {
                "lost": publish_lost,
                "slow_tasks": slow_tasks,
                "not_completed": truly_missing,
                # If not None: tasks did finish, timeout was just too short by this many ms.
                "extra_wait_ms": extra_wait_ms,
                "group_rtt_ms": None,
                "tail_latency_ms": None,
            }

        finally:
            g.revoke()
            g.forget()


# ------------------------
# WORKER
# ------------------------


def start_worker():
    app.worker_main(
        [
            "worker",
            "--loglevel=WARNING",
            "--concurrency=32",
            "--pool=prefork",
            "--without-gossip",
            "--without-mingle",
            "--without-heartbeat",
        ]
    )


# ------------------------
# WARMUP
# ------------------------


def warmup_workers(num_workers):
    """
    Two-phase warmup to initialize worker child processes before measuring.

    Phase 1: Send sequential dispatch_group tasks (small batches) so each
    child process lazily initializes its ResultConsumer pubsub connection.

    Phase 2: Ramp up to full concurrent load and wait until stable. This
    ensures all children are warm before we start measuring.
    """
    total_children = 32 * num_workers  # 32 children per worker

    # Phase 1: sequential tasks to initialize individual children
    num_seq = total_children * 2
    print(f"  Phase 1: {num_seq} sequential group_get tasks...")
    for _ in range(num_seq):
        ar = app.tasks["group_get"].apply_async(args=[5], kwargs={"get_timeout": 60})
        try:
            ar.get(timeout=70)
        except Exception:
            pass
    print(f"    done")

    # Phase 2: ramp up to full concurrency, wait until stable (using lighter load)
    print(f"  Phase 2: ramping to full concurrency, waiting for stability...")
    consecutive_ok = 0
    attempts = 0
    warmup_concurrent = max(1, CONCURRENT // 4)  # Use 1/4 load for faster warmup
    warmup_batch = max(10, BATCH // 5)  # Smaller batches for faster iteration

    while consecutive_ok < 3:
        attempts += 1
        # Run warmup with lighter load: fewer concurrent tasks, smaller batches
        ars = [
            app.tasks["group_get"].delay(warmup_batch, get_timeout=TIMEOUT)
            for _ in range(warmup_concurrent)
        ]

        round_fail = 0
        for ar in ars:
            try:
                res = ar.get(timeout=TIMEOUT + EXTRA_POLL + 10)
                if res["lost"] > 0 or res.get("not_completed", 0) > 0:
                    round_fail += 1
            except Exception:
                round_fail += 1

        if round_fail == 0:
            consecutive_ok += 1
        else:
            consecutive_ok = 0
        if attempts > 20:
            print("    WARN: could not stabilize after 20 rounds")
            break
    print(f"    stable after {attempts} round(s)")


# ------------------------
# TEST RUNNER
# ------------------------


def run():
    print("Starting pub/sub publish-not-lost test...\n")

    # Detect backend type
    backend_url = REDIS_BACKEND.replace("redis://", "")
    host_port, _ = backend_url.rsplit("/", 1)
    host, port = host_port.split(":")
    r = redis.Redis(host=host, port=int(port))
    info = r.info("server")
    server_type = "Dragonfly" if "dragonfly_version" in info else "Redis"
    version = info.get("dragonfly_version", info.get("redis_version", "unknown"))
    r.close()

    print(f"Backend: {server_type} {version}")
    seed_str = f", seed={args.seed}" if args.seed is not None else ""
    print(
        f"Config: batch_size={BATCH}, rounds={ROUNDS}, get_timeout={TIMEOUT}s, concurrent={CONCURRENT}{seed_str}"
    )
    print()

    # Wait for workers
    print("Waiting for workers...", end=" ", flush=True)
    active_workers = None
    for _ in range(30):
        inspector = app.control.inspect()
        active_workers = inspector.ping()
        if active_workers:
            break
        time.sleep(2)
        print(".", end="", flush=True)
    if not active_workers:
        print(" NONE FOUND")
        print("Start workers first")
        return
    num_workers = len(active_workers)
    print(f" {num_workers} worker(s)")
    print()

    outer_timeout = TIMEOUT + EXTRA_POLL + 10
    dispatch = app.tasks["group_get"]

    def run_round():
        """Run one round and return (round_ok, round_fail, round_lost, round_elapsed)."""
        ars = [dispatch.delay(BATCH, get_timeout=TIMEOUT) for _ in range(CONCURRENT)]

        round_ok = 0
        round_fail = 0
        round_lost = 0
        round_max_elapsed = 0.0

        for ar in ars:
            try:
                res = ar.get(timeout=outer_timeout)
                if res["lost"] == 0 and res.get("not_completed", 0) == 0:
                    round_ok += 1
                else:
                    round_fail += 1
                    round_lost += res["lost"]
                round_max_elapsed = max(round_max_elapsed, outer_timeout)
            except Exception:
                round_fail += 1
                round_lost += BATCH
                round_max_elapsed = outer_timeout

        return round_ok, round_fail, round_lost, round_max_elapsed

    print("Warming up workers (initializing connections)...")
    warmup_workers(num_workers)
    print()

    total_ok = 0
    total_fail = 0
    total_lost = 0

    for rnd in range(1, ROUNDS + 1):
        print(f"================ ROUND {rnd}/{ROUNDS} ================")

        ars = [dispatch.delay(BATCH, get_timeout=TIMEOUT) for _ in range(CONCURRENT)]

        round_lost = 0
        round_max_tail = 0.0
        round_max_rtt = 0.0
        round_ok = 0
        round_fail = 0

        for worker_idx, ar in enumerate(ars):
            try:
                res = ar.get(timeout=outer_timeout)
                lost = res["lost"]
                slow_tasks = res.get("slow_tasks", 0)
                not_completed = res.get("not_completed", 0)
                extra_wait_ms = res.get("extra_wait_ms")
                round_lost += lost

                if res["tail_latency_ms"] is not None:
                    round_max_tail = max(round_max_tail, res["tail_latency_ms"])
                if res["group_rtt_ms"] is not None:
                    round_max_rtt = max(round_max_rtt, res["group_rtt_ms"])

                if lost or slow_tasks or not_completed:
                    round_fail += 1
                    parts = []
                    if lost:
                        msg = f"publish_lost={lost}"
                        if extra_wait_ms is not None:
                            msg += f" (finished {extra_wait_ms:.0f} ms after timeout)"
                        parts.append(msg)
                    if slow_tasks:
                        if extra_wait_ms is not None:
                            parts.append(
                                f"slow_tasks={slow_tasks} (finished {extra_wait_ms:.0f} ms after timeout)"
                            )
                        else:
                            parts.append(f"slow_tasks={slow_tasks}")
                    if not_completed:
                        parts.append(
                            f"not_completed={not_completed} (still missing after {EXTRA_POLL}s)"
                        )
                    print(f"  [worker {worker_idx:02d}] FAIL  {'  '.join(parts)}")
                else:
                    round_ok += 1
                    print(
                        f"  [worker {worker_idx:02d}] ok"
                        f"  rtt={res['group_rtt_ms']:.1f} ms"
                        f"  tail={res['tail_latency_ms']:.1f} ms"
                    )

            except Exception as e:
                round_fail += 1
                task_state = ar.state
                task_info = f"state={task_state}"

                # If task succeeded but ar.get() timed out, use the result directly
                if (
                    task_state == "SUCCESS"
                    and ar.result is not None
                    and isinstance(ar.result, dict)
                ):
                    res = ar.result
                    task_info += f", result={res}"
                    lost = res["lost"]
                    slow_tasks = res.get("slow_tasks", 0)
                    not_completed = res.get("not_completed", 0)
                    round_lost += lost

                    if lost == 0 and not_completed == 0:
                        round_ok += 1
                    else:
                        # Fall through to print failure details
                        round_fail += 1
                        parts = []
                        if lost:
                            msg = f"publish_lost={lost}"
                            if res.get("extra_wait_ms") is not None:
                                msg += (
                                    f" (finished {res.get('extra_wait_ms'):.0f} ms after timeout)"
                                )
                            parts.append(msg)
                        if slow_tasks:
                            if res.get("extra_wait_ms") is not None:
                                parts.append(
                                    f"slow_tasks={slow_tasks} (finished {res.get('extra_wait_ms'):.0f} ms after timeout)"
                                )
                            else:
                                parts.append(f"slow_tasks={slow_tasks}")
                        if not_completed:
                            parts.append(
                                f"not_completed={not_completed} (still missing after {EXTRA_POLL}s)"
                            )
                        print(
                            f"  [worker {worker_idx:02d}] FAIL (ar.get timeout but result available)  {'  '.join(parts)}"
                        )
                    continue

                # Otherwise, actual timeout or failure
                if ar.result is not None and isinstance(ar.result, dict):
                    task_info += f", result={ar.result}"
                round_lost += BATCH
                print(f"  [worker {worker_idx:02d}] TIMEOUT — {task_info}, error={e}")

        total_ok += round_ok
        total_fail += round_fail
        total_lost += round_lost
        status = "PASS" if round_fail == 0 else "FAIL"
        detail = ""
        if round_lost > 0:
            detail = f" (pubsub_lost={round_lost})"
        print(
            f"  => {round_ok}/{CONCURRENT} ok  max_rtt={round_max_rtt:.1f} ms"
            f"  max_tail_latency={round_max_tail:.1f} ms [{status}]{detail}\n"
        )

    print()
    total = total_ok + total_fail
    print(f"================= RESULT ==================")
    print(f"Total: {total_ok}/{total} passed, {total_fail} failed")
    if total_lost > 0:
        print(f"  PUB/SUB BUG: {total_lost} tasks completed but group.get() never received PUBLISH")
    if total_fail > 0:
        print("FAILED: Some group.get() calls timed out")
    else:
        print("PASSED: All group.get() calls completed successfully")


# ------------------------
# ENTRYPOINT
# ------------------------

if __name__ == "__main__":
    t = threading.Thread(target=start_worker, daemon=True)
    t.start()

    time.sleep(1)

    run()
