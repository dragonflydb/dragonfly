#!/usr/bin/env python3

"""
Pub/sub stress test.

Phase 1 (--subscribe): ramp up --sub-max-channels additional subscriptions at --sub-rate/sec
                       (on top of any --pre-subscribe baseline).
                       Subscriptions remain open as background load during the publish phase.
Phase 2 (--publish):  create --pub-sub-connections dedicated subscriber connections,
                      publish at --pub-rate pipelines/sec across --pub-workers publisher
                      connections, and measure both publish RTT and end-to-end delivery latency
                      per second.

Quick tutorial:
    # Start Dragonfly/Redis separately, then run against localhost:6379.
    python3 tools/pubsub/stress.py --publish

    # Publish 20k messages/sec for 60 seconds to 1k one-channel subscribers.
    python3 tools/pubsub/stress.py --publish --pub-rate 20000 --pub-duration 60 \
        --pub-workers 8 --pub-sub-connections 1000

    # Send three 200k-command PUBLISH pipeline per second to K one-subscriber channels.
    python3 tools/pubsub/stress.py --publish --pub-rate 3 --pub-duration 5 \
        --pub-sub-connections K --pub-pipeline-depth 200000

    # First add 100k long-lived background subscriptions, then run the publish phase.
    python3 tools/pubsub/stress.py --subscribe --sub-max-channels 100000 --sub-channels 1000 \
        --sub-rate 10 --publish --pub-rate 5000 --pub-duration 30

    # Use a URI, including rediss:// and password support.
    python3 tools/pubsub/stress.py --uri redis://localhost:6379 --publish

What this tool can and cannot measure:
    - It can ramp subscription count and report per-second publish RTT and delivery latency.
    - --pub-rate is pipelines/second. Overall PUBLISH commands/second is
        --pub-rate * --pub-pipeline-depth.
    - With --pub-pipeline-depth > 1, publish latency is measured after the pipeline bytes are
        flushed to the socket until all PUBLISH replies are read.
    - --pub-sub-connections controls the number of publish-phase channels and subscriber
    connections.
"""

import argparse
import sys
import time
import threading
import queue
import random
from urllib.parse import urlparse

import redis


def gen_channels(count: int, start: int = 0) -> list[str]:
    return [f"subscribe/channel/{start + i}" for i in range(count)]


def parse_uri(uri: str) -> tuple[str, int, str | None, bool]:
    parsed = urlparse(uri)
    host = parsed.hostname or "localhost"
    port = parsed.port or (6380 if parsed.scheme == "rediss" else 6379)
    password = parsed.password or None
    ssl = parsed.scheme == "rediss"
    return host, port, password, ssl


def worker(pubsub: redis.client.PubSub, work_queue: queue.Queue, total_ref: list):
    """Dedicated subscribe thread — the only thread that calls pubsub methods.

    Serialising all subscribe calls here prevents concurrent use of the pubsub
    socket and makes acknowledgement counting unambiguous.

    Queue item shapes:
      (channels, t_start)           — ramp-phase batch; prints a progress line when done
      (channels, t_start, event)    — pre-subscribe batch; sets event when done, no output
      None                          — poison pill, causes the thread to exit
    """
    while True:
        item = work_queue.get()
        if item is None:
            break

        if len(item) == 3:
            channels, t_start, done_event = item
        else:
            channels, t_start, done_event = item[0], item[1], None

        pubsub.subscribe(*channels)

        # Drain subscribe confirmations so the caller knows all channels are active.
        # redis-py sends one "subscribe" message per channel.
        received = 0
        while received < len(channels):
            msg = pubsub.get_message(ignore_subscribe_messages=False, timeout=5.0)
            if msg and msg["type"] == "subscribe":
                received += 1

        ms = (time.perf_counter() - t_start) * 1000

        if done_event:
            done_event.set()
        else:
            total_ref[0] += len(channels)
            print(f"{total_ref[0]:>12,}  {work_queue.qsize():>8}  {ms:>10.3f}", flush=True)

        work_queue.task_done()


def publish_phase(
    redis_kwargs: dict,
    pub_rate: int,
    pub_duration: int,
    pub_workers: int = 1,
    pub_sub_connections: int = 128,
    pub_pipeline_depth: int = 1,
):
    """Publish-latency benchmark with dedicated subscriber connections.

    Creates pub_sub_connections subscriber connections (one channel each) that are separate from
    the ramp-up subscriptions, so the two phases do not share a socket.
    pub_workers publisher connections share the total pub_rate pipelines/second evenly.

    Two latency columns are reported each second:
      Publish   — time for r.publish() to return, or for pipelined publishes: time after the
                  pipeline bytes are flushed until all PUBLISH replies are read.
      Delivery  — time from publish() call to message receipt in the listener thread
    """
    # Each subscriber connection owns exactly one channel so listener threads can call
    # get_message() independently without sharing a socket.
    channels = [f"publish/channel/{i}" for i in range(pub_sub_connections)]

    latencies: list[float] = []  # delivery latencies collected by listener threads
    pub_latencies: list[float] = []  # publish RTT collected by publisher threads
    total_received = [0]
    lock = threading.Lock()  # guards latencies, pub_latencies, total_received
    stop_event = threading.Event()
    errors: queue.Queue[str] = queue.Queue()

    def report_error(context: str, err: Exception):
        if not stop_event.is_set():
            errors.put(f"{context}: {err}")
        stop_event.set()

    def get_error() -> str | None:
        try:
            return errors.get_nowait()
        except queue.Empty:
            return None

    def make_listener(ps: redis.client.PubSub):
        """Return a listener function bound to a single pubsub handle."""

        def _listener():
            try:
                while not stop_event.is_set():
                    msg = ps.get_message(ignore_subscribe_messages=True, timeout=0.1)
                    if msg and msg["type"] == "message":
                        try:
                            data = msg["data"]
                            if isinstance(data, (bytes, bytearray)):
                                data = data.decode()
                            # Payload is the perf_counter timestamp set by the publisher.
                            with lock:
                                latencies.append((time.perf_counter() - float(data)) * 1000)
                                total_received[0] += 1
                        except (ValueError, TypeError):
                            pass
            except redis.exceptions.RedisError as err:
                report_error("subscriber connection error", err)

        return _listener

    # Subscribe each connection to its own channel and drain the ack before starting.
    sub_connections = [redis.Redis(**redis_kwargs) for _ in range(pub_sub_connections)]
    sub_pubsubs = [r.pubsub() for r in sub_connections]

    print(f"\n=== Publish ===\n", flush=True)
    print(
        f"Subscribing {pub_sub_connections} connections to"
        f" publish/channel/0..{pub_sub_connections - 1}...",
        flush=True,
    )
    try:
        for i, ps in enumerate(sub_pubsubs):
            ps.subscribe(channels[i])
            while True:
                msg = ps.get_message(ignore_subscribe_messages=False, timeout=5.0)
                if msg and msg["type"] == "subscribe":
                    break
    except redis.exceptions.RedisError as err:
        for ps in sub_pubsubs:
            ps.close()
        for r in sub_connections:
            r.close()
        print(f"[ERROR] Cannot subscribe publish clients — {err}")
        sys.exit(1)

    listener_threads = [
        threading.Thread(target=make_listener(ps), daemon=True) for ps in sub_pubsubs
    ]
    for t in listener_threads:
        t.start()

    # Distribute pub_rate evenly across workers; actual rate may differ slightly
    # from the requested rate when pub_rate is not divisible by pub_workers.
    rate_per_worker = max(1, pub_rate // pub_workers)
    actual_pipeline_rate = rate_per_worker * pub_workers
    actual_publish_rate = actual_pipeline_rate * pub_pipeline_depth

    print(
        f"Publishing to {pub_sub_connections} channels at {actual_pipeline_rate} "
        f"pipeline(s)/sec ({pub_workers} worker(s) × {rate_per_worker}/sec) "
        f"for {pub_duration}s, pipeline depth {pub_pipeline_depth}, "
        f"PUBLISH rate {actual_publish_rate}/sec\n"
    )
    pub_hdr = "--- Publish ---".rjust(23).ljust(26)
    del_hdr = "--- Delivery ---".rjust(23).ljust(26)
    print(f"{'':7}  {'':10}  {'':10}  {pub_hdr}  {del_hdr}")
    print(
        f"{'Second':>7}  {'Published':>10}  {'Received':>10}"
        f"  {'Avg(ms)':>12}  {'Max(ms)':>12}"
        f"  {'Avg(ms)':>12}  {'Max(ms)':>12}"
    )
    print("-" * 87)

    # Per-worker counters avoid a shared atomic; the reporter sums them each tick.
    counters = [0] * pub_workers

    def publisher(worker_id: int, r: redis.Redis):
        """Publish rate_per_worker pipelines per second for pub_duration seconds.
        Embeds a perf_counter timestamp as the payload so listeners can compute
        end-to-end delivery latency.
        """
        interval = 1.0 / rate_per_worker

        conn = None
        try:
            if pub_pipeline_depth <= 1:
                for _ in range(pub_duration):
                    second_start = time.perf_counter()
                    for i in range(rate_per_worker):
                        target_t = second_start + i * interval
                        delay = target_t - time.perf_counter()
                        if delay > 0:
                            time.sleep(delay)
                        t0 = time.perf_counter()
                        r.publish(random.choice(channels), str(t0))
                        with lock:
                            pub_latencies.append((time.perf_counter() - t0) * 1000)
                        counters[worker_id] += 1
                    # Sleep out the remainder of the second so the next iteration starts cleanly.
                    elapsed = time.perf_counter() - second_start
                    if elapsed < 1.0:
                        time.sleep(1.0 - elapsed)
                return

            pool = r.connection_pool
            conn = pool.get_connection()

            conn.connect()
            for _ in range(pub_duration):
                second_start = time.perf_counter()
                pipelines_this_second = 0
                while pipelines_this_second < rate_per_worker:
                    target_t = second_start + pipelines_this_second * interval
                    delay = target_t - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)

                    batch_size = pub_pipeline_depth
                    t0 = time.perf_counter()
                    commands = [
                        ("PUBLISH", random.choice(channels), str(t0)) for _ in range(batch_size)
                    ]

                    conn.send_packed_command(conn.pack_commands(commands))
                    flushed_at = time.perf_counter()
                    for _ in range(batch_size):
                        conn.read_response()

                    with lock:
                        pub_latencies.append((time.perf_counter() - flushed_at) * 1000)
                    counters[worker_id] += batch_size
                    pipelines_this_second += 1

                # Sleep out the remainder of the second so the next iteration starts cleanly.
                elapsed = time.perf_counter() - second_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
        except redis.exceptions.RedisError as err:
            report_error(f"publisher {worker_id} connection error", err)
        finally:
            if conn is not None:
                r.connection_pool.release(conn)

    pub_connections = [redis.Redis(**redis_kwargs) for _ in range(pub_workers)]
    worker_threads = [
        threading.Thread(target=publisher, args=(i, pub_connections[i]), daemon=True)
        for i in range(pub_workers)
    ]
    for t in worker_threads:
        t.start()

    # Reporting loop: wake once per second and snapshot the shared counters.
    prev_received = 0
    prev_published = 0

    def print_snapshot(label: str):
        nonlocal prev_received, prev_published
        total_published = sum(counters)
        published_delta = total_published - prev_published
        prev_published = total_published

        with lock:
            del_snap = latencies[:]
            latencies.clear()
            pub_snap = pub_latencies[:]
            pub_latencies.clear()
            received_delta = total_received[0] - prev_received
            prev_received = total_received[0]

        pub_avg = sum(pub_snap) / len(pub_snap) if pub_snap else 0.0
        pub_max = max(pub_snap) if pub_snap else 0.0
        del_avg = sum(del_snap) / len(del_snap) if del_snap else 0.0
        del_max = max(del_snap) if del_snap else 0.0
        print(
            f"{label:>7}  {published_delta:>10,}  {received_delta:>10,}"
            f"  {pub_avg:>12.3f}  {pub_max:>12.3f}"
            f"  {del_avg:>12.3f}  {del_max:>12.3f}",
            flush=True,
        )

    next_tick = time.perf_counter() + 1.0
    first_error = None
    for sec in range(pub_duration):
        delay = next_tick - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        next_tick += 1.0
        print_snapshot(str(sec + 1))
        first_error = get_error()
        if first_error is not None:
            break

    for t in worker_threads:
        t.join()

    # A large pipeline may complete after the last per-second tick. Give subscriber listener
    # threads a short chance to drain their sockets, then print the remaining measurements.
    delivery_deadline = time.perf_counter() + 5.0
    while time.perf_counter() < delivery_deadline:
        first_error = first_error or get_error()
        if first_error is not None:
            break
        total_published = sum(counters)
        with lock:
            done = total_received[0] >= total_published
        if done:
            break
        time.sleep(0.05)

    if sum(counters) != prev_published or total_received[0] != prev_received:
        print_snapshot("final")

    for r in pub_connections:
        r.close()

    stop_event.set()
    for t in listener_threads:
        t.join()
    for ps in sub_pubsubs:
        ps.close()
    for r in sub_connections:
        r.close()

    if first_error is not None:
        print(f"[ERROR] {first_error}")
        sys.exit(1)


def run(
    host: str,
    port: int,
    channels_per_batch: int,
    rate_per_sec: int,
    max_channels: int,
    pre_subscribe: int,
    subscribe: bool,
    publish: bool,
    pub_rate: int,
    pub_duration: int,
    pub_workers: int = 1,
    pub_sub_connections: int = 128,
    pub_pipeline_depth: int = 1,
    password: str | None = None,
    ssl: bool = False,
):
    r = redis.Redis(host=host, port=port, password=password, ssl=ssl)

    try:
        r.ping()
    except Exception as e:
        print(f"[ERROR] Cannot connect to {host}:{port} — {e}")
        sys.exit(1)

    redis_kwargs = dict(host=host, port=port, password=password, ssl=ssl)

    worker_thread = None
    pubsub = None

    if subscribe:
        pubsub = r.pubsub()
        work_queue: queue.Queue = queue.Queue()
        total_ref = [0]

        # Dedicated thread owns all subscribe calls to avoid concurrent socket use.
        worker_thread = threading.Thread(
            target=worker, args=(pubsub, work_queue, total_ref), daemon=True
        )
        worker_thread.start()

        total = 0
        channel_cursor = 0

        # Pre-subscribe phase: subscribe silently up to --pre-subscribe channels
        # before the visible ramp starts (useful for testing with a large baseline).
        if pre_subscribe > 0:
            print(f"\n=== Pre-subscribe ===\n", flush=True)
            print(f"Subscribing {pre_subscribe:,} channels silently...", flush=True)
            while total < pre_subscribe:
                size = min(1000, pre_subscribe - total)
                channels = gen_channels(size, start=channel_cursor)
                channel_cursor += size
                done_event = threading.Event()
                work_queue.put((channels, time.perf_counter(), done_event))
                done_event.wait()
                total += size
                if total % 10_000 == 0:
                    print(f"  {total:,} / {pre_subscribe:,}", flush=True)
            total_ref[0] = total
            print(f"Done. {total:,} channels active.\n", flush=True)

        # Ramp phase: subscribe channels_per_batch channels rate_per_sec times per second
        # until max_channels total subscriptions have been added.
        print(f"\n=== Subscribe ramp-up ===\n", flush=True)
        print(f"{'Total subs':>12}  {'Queue':>8}  {'ms':>10}")
        print("-" * 34)

        ramp_target = total + max_channels
        interval = 1.0 / rate_per_sec

        while total < ramp_target:
            second_start = time.perf_counter()

            for i in range(rate_per_sec):
                if total >= ramp_target:
                    break
                size = min(channels_per_batch, ramp_target - total)
                channels = gen_channels(size, start=channel_cursor)
                channel_cursor += size
                total += size

                target_t = second_start + i * interval
                delay = target_t - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)

                work_queue.put((channels, time.perf_counter()))

            elapsed = time.perf_counter() - second_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

        # Block until the worker has processed every subscribe batch.
        # The worker thread stays alive so the pubsub socket remains open,
        # keeping all subscriptions active as background load during publish phase.
        work_queue.join()

    if publish:
        publish_phase(
            redis_kwargs=redis_kwargs,
            pub_rate=pub_rate,
            pub_duration=pub_duration,
            pub_workers=pub_workers,
            pub_sub_connections=pub_sub_connections,
            pub_pipeline_depth=pub_pipeline_depth,
        )

    if worker_thread is not None:
        work_queue.put(None)
        worker_thread.join()
        pubsub.close()

    r.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--uri",
        default=None,
        help="URI",
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=6379, type=int)
    # Subscribe phase
    parser.add_argument("--subscribe", action="store_true", help="Run subscribe ramp-up phase")
    parser.add_argument("--pre-subscribe", default=0, type=int)
    parser.add_argument("--sub-channels", default=5, type=int, help="Channels per batch")
    parser.add_argument("--sub-rate", default=50, type=int, help="Subscribe rate per second")
    parser.add_argument("--sub-max-channels", default=150_000, type=int)
    # Publish phase
    parser.add_argument("--publish", action="store_true", help="Run publish phase")
    parser.add_argument(
        "--pub-rate",
        default=100,
        type=int,
        help="Total publish pipelines per second",
    )
    parser.add_argument(
        "--pub-duration", default=60, type=int, help="Publish phase duration in seconds"
    )
    parser.add_argument(
        "--pub-workers", default=1, type=int, help="Number of parallel publish workers"
    )
    parser.add_argument(
        "--pub-sub-connections",
        default=128,
        type=int,
        help="Number of channels in publish phase",
    )
    parser.add_argument(
        "--pub-pipeline-depth",
        default=1,
        type=int,
        help="Number of PUBLISH commands to send per pipeline batch",
    )
    args = parser.parse_args()

    errors = []
    if args.sub_rate < 1:
        errors.append("--sub-rate must be >= 1")
    if args.sub_channels < 1:
        errors.append("--sub-channels must be >= 1")
    if args.pub_workers < 1:
        errors.append("--pub-workers must be >= 1")
    if args.pub_sub_connections < 1:
        errors.append("--pub-sub-connections must be >= 1")
    if args.pub_pipeline_depth < 1:
        errors.append("--pub-pipeline-depth must be >= 1")
    if errors:
        parser.error("\n  ".join(errors))

    if args.uri:
        host, port, password, ssl = parse_uri(args.uri)
    else:
        host, port, password, ssl = args.host, args.port, None, False

    run(
        host=host,
        port=port,
        channels_per_batch=args.sub_channels,
        rate_per_sec=args.sub_rate,
        max_channels=args.sub_max_channels,
        pre_subscribe=args.pre_subscribe,
        subscribe=args.subscribe,
        publish=args.publish,
        pub_rate=args.pub_rate,
        pub_duration=args.pub_duration,
        pub_workers=args.pub_workers,
        pub_sub_connections=args.pub_sub_connections,
        pub_pipeline_depth=args.pub_pipeline_depth,
        password=password,
        ssl=ssl,
    )
