#!/usr/bin/env python3
"""
Test pub/sub latency under concurrent load.
Measures: publish latency, subscribe notification latency, and how they scale with concurrency.
"""
import redis
import threading
import time
import statistics
import argparse
from concurrent.futures import ThreadPoolExecutor

DRAGONFLY_HOST = "127.0.0.1"
DRAGONFLY_PORT = 6379


def test_pubsub_latency(num_channels=16, publishes_per_channel=100):
    """
    Test pub/sub latency:
    - Subscribe to N channels
    - Publish to each channel M times
    - Measure time from publish -> notification received
    """
    r = redis.Redis(host=DRAGONFLY_HOST, port=DRAGONFLY_PORT, decode_responses=True)
    pubsub = r.pubsub()

    latencies = []

    # Subscribe to all channels
    channels = [f"channel-{i}" for i in range(num_channels)]
    for ch in channels:
        pubsub.subscribe(ch)

    # Consume subscription confirmations
    for _ in range(num_channels):
        pubsub.get_message(timeout=1)

    # Publish and measure
    for ch in channels:
        for msg_id in range(publishes_per_channel):
            publish_time = time.time()
            r.publish(ch, f"msg-{msg_id}")

            # Wait for notification
            msg = pubsub.get_message(timeout=1)
            if msg and msg["type"] == "message":
                receive_time = time.time()
                latency_us = (receive_time - publish_time) * 1e6
                latencies.append(latency_us)

    pubsub.close()
    r.close()

    return latencies


def test_concurrent_publish(num_concurrent=16, publishes_per_client=100):
    """
    Test concurrent publish throughput and latency.
    Multiple threads publishing simultaneously.
    """
    r = redis.Redis(host=DRAGONFLY_HOST, port=DRAGONFLY_PORT, decode_responses=True)
    pubsub = r.pubsub()

    # Subscribe to all channels
    channels = [f"chan-{i}" for i in range(num_concurrent)]
    for ch in channels:
        pubsub.subscribe(ch)

    # Consume subscription confirmations
    for _ in range(num_concurrent):
        pubsub.get_message(timeout=1)

    latencies = []
    lock = threading.Lock()

    def publish_and_measure(channel_id):
        r_thread = redis.Redis(host=DRAGONFLY_HOST, port=DRAGONFLY_PORT, decode_responses=True)
        ch = channels[channel_id]

        for msg_id in range(publishes_per_client):
            publish_time = time.time()
            r_thread.publish(ch, f"msg-{msg_id}")
            # Note: we're measuring publish latency, not end-to-end notification
            publish_latency_us = (time.time() - publish_time) * 1e6

            with lock:
                latencies.append(publish_latency_us)

        r_thread.close()

    # Run concurrent publishers and measure wall-clock time
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
        futures = [executor.submit(publish_and_measure, i) for i in range(num_concurrent)]
        for f in futures:
            f.result()
    elapsed_time = time.time() - start_time

    pubsub.close()
    r.close()

    return latencies, elapsed_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test Dragonfly pub/sub latency under concurrent load"
    )
    parser.add_argument(
        "--num-channels", type=int, default=16, help="Number of channels (default: 16)"
    )
    parser.add_argument(
        "--publishes-per-channel", type=int, default=100, help="Messages per channel (default: 100)"
    )
    parser.add_argument(
        "--num-concurrent", type=int, default=16, help="Number of concurrent clients (default: 16)"
    )
    parser.add_argument(
        "--publishes-per-client", type=int, default=100, help="Publishes per client (default: 100)"
    )
    args = parser.parse_args()

    print("Testing Dragonfly pub/sub latency...\n")

    # Test 1: Sequential pub/sub (baseline)
    print("=" * 60)
    print(
        f"Test 1: Sequential pub/sub ({args.num_channels} channels, {args.publishes_per_channel} messages each)"
    )
    print("=" * 60)
    latencies = test_pubsub_latency(
        num_channels=args.num_channels, publishes_per_channel=args.publishes_per_channel
    )
    if latencies:
        print(f"Messages measured: {len(latencies)}")
        print(f"Min latency:    {min(latencies):.2f} µs")
        print(f"p50 latency:    {statistics.median(latencies):.2f} µs")
        print(f"p99 latency:    {sorted(latencies)[int(len(latencies)*0.99)]:.2f} µs")
        print(f"Max latency:    {max(latencies):.2f} µs")
        print(f"Avg latency:    {statistics.mean(latencies):.2f} µs")
    else:
        print("No measurements collected")
    print()

    # Test 2: Concurrent publish (16 clients, 100 publishes each)
    print("=" * 60)
    print(
        f"Test 2: Concurrent publish ({args.num_concurrent} clients, {args.publishes_per_client} publishes each)"
    )
    print("=" * 60)
    latencies, elapsed_time = test_concurrent_publish(
        num_concurrent=args.num_concurrent, publishes_per_client=args.publishes_per_client
    )
    if latencies:
        print(f"Publishes measured: {len(latencies)}")
        print(f"Min latency:    {min(latencies):.2f} µs")
        print(f"p50 latency:    {statistics.median(latencies):.2f} µs")
        print(f"p99 latency:    {sorted(latencies)[int(len(latencies)*0.99)]:.2f} µs")
        print(f"Max latency:    {max(latencies):.2f} µs")
        print(f"Avg latency:    {statistics.mean(latencies):.2f} µs")
        if elapsed_time > 0:
            throughput = len(latencies) / elapsed_time
            print(f"Throughput:     {throughput:.0f} ops/sec")
        else:
            print("Throughput:     N/A (elapsed time too short)")
    else:
        print("No measurements collected")
