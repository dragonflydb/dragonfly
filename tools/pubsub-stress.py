"""
Pub/sub stress test.

Phase 1 (--subscribe): ramp up to --sub-max-channels subscriptions at --sub-rate/sec.
                       Subscriptions remain open as background load during the publish phase.
Phase 2 (--publish):  create --pub-sub-connections dedicated subscriber connections,
                      publish at --pub-rate msgs/sec across --pub-workers publisher connections,
                      and measure both publish RTT and end-to-end delivery latency per second.
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
):
    """Publish-latency benchmark with dedicated subscriber connections.

    Creates pub_sub_connections subscriber connections (one channel each) that are
    separate from the ramp-up subscriptions, so the two phases do not share a socket.
    pub_workers publisher connections share the total pub_rate evenly.

    Two latency columns are reported each second:
      Publish   — time for r.publish() to return (round-trip to server)
      Delivery  — time from publish() call to message receipt in the listener thread
    """
    # Each subscriber connection owns exactly one channel so listener threads
    # can call get_message() independently without sharing a socket.
    channels = [f"publish/channel/{i}" for i in range(pub_sub_connections)]

    latencies: list[float] = []  # delivery latencies collected by listener threads
    pub_latencies: list[float] = []  # publish RTT collected by publisher threads
    total_received = [0]
    lock = threading.Lock()  # guards latencies, pub_latencies, total_received
    stop_event = threading.Event()

    def make_listener(ps: redis.client.PubSub):
        """Return a listener function bound to a single pubsub handle."""

        def _listener():
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
    for i, ps in enumerate(sub_pubsubs):
        ps.subscribe(channels[i])
        while True:
            msg = ps.get_message(ignore_subscribe_messages=False, timeout=5.0)
            if msg and msg["type"] == "subscribe":
                break

    listener_threads = [
        threading.Thread(target=make_listener(ps), daemon=True) for ps in sub_pubsubs
    ]
    for t in listener_threads:
        t.start()

    # Distribute pub_rate evenly across workers; actual rate may differ slightly
    # from the requested rate when pub_rate is not divisible by pub_workers.
    rate_per_worker = max(1, pub_rate // pub_workers)
    actual_rate = rate_per_worker * pub_workers

    print(
        f"Publishing to {pub_sub_connections} channels at {actual_rate}/sec "
        f"({pub_workers} worker(s) × {rate_per_worker}/sec) for {pub_duration}s\n"
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
        """Publish rate_per_worker messages per second for pub_duration seconds.
        Embeds a perf_counter timestamp as the payload so listeners can compute
        end-to-end delivery latency.
        """
        interval = 1.0 / rate_per_worker
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
    next_tick = time.perf_counter() + 1.0
    for sec in range(pub_duration):
        delay = next_tick - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        next_tick += 1.0

        total_published = sum(counters)
        published_this_sec = total_published - prev_published
        prev_published = total_published

        with lock:
            del_snap = latencies[:]
            latencies.clear()
            pub_snap = pub_latencies[:]
            pub_latencies.clear()
            received_this_sec = total_received[0] - prev_received
            prev_received = total_received[0]

        pub_avg = sum(pub_snap) / len(pub_snap) if pub_snap else 0.0
        pub_max = max(pub_snap) if pub_snap else 0.0
        del_avg = sum(del_snap) / len(del_snap) if del_snap else 0.0
        del_max = max(del_snap) if del_snap else 0.0
        print(
            f"{sec + 1:>7}  {published_this_sec:>10,}  {received_this_sec:>10,}"
            f"  {pub_avg:>12.3f}  {pub_max:>12.3f}"
            f"  {del_avg:>12.3f}  {del_max:>12.3f}",
            flush=True,
        )

    for t in worker_threads:
        t.join()
    for r in pub_connections:
        r.close()

    stop_event.set()
    for t in listener_threads:
        t.join()
    for ps in sub_pubsubs:
        ps.close()
    for r in sub_connections:
        r.close()


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
    parser.add_argument("--pub-rate", default=100, type=int, help="Total publishes per second")
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
        help="Number of subscriber connections in publish phase",
    )
    args = parser.parse_args()

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
        password=password,
        ssl=ssl,
    )
