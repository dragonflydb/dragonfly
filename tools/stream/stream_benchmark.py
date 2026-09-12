#!/usr/bin/env python3
"""
Comprehensive Redis Stream Benchmarking Suite
Benchmarks: XADD, XREAD, XRANGE, consumer groups
Measures: Throughput, latency, memory usage
Scenarios: ops (XADD,XREAD), consumer groups, mixed workloads

Written with Claude Code (https://claude.com/claude-code)
"""

import redis
import time
import csv
import statistics
import argparse
import random
import string
from datetime import datetime
from typing import Dict, List, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from urllib.parse import urlparse

_FIELD_NAMES = ["user_id", "session_id", "action", "resource", "status", "region", "version"]
_ACTIONS = ["read", "write", "update", "delete", "list", "create", "process"]
_STATUSES = ["ok", "ok", "ok", "error", "pending"]  # weighted toward ok


def _make_payload(i: int) -> dict:
    """Generic variable-size payload simulating a realistic stream entry."""
    payload = {
        "seq": str(i),
        "ts": str(time.time()),
        "user_id": str(random.randint(1, 100_000)),
        "action": random.choice(_ACTIONS),
        "status": random.choice(_STATUSES),
        "region": random.choice(["us-east", "us-west", "eu-west", "ap-south"]),
    }
    # Variable-size random data blob (8-64 bytes, mean 16)
    size = int(16 + random.expovariate(1 / 16))
    size = min(max(size, 8), 64)
    payload["data"] = "".join(random.choices(string.ascii_letters + string.digits, k=size))
    return payload


@dataclass
class BenchmarkResult:
    """Stores benchmark metrics"""

    scenario: str
    command: str
    num_operations: int
    duration_seconds: float
    throughput_ops_sec: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    memory_before_mb: float
    memory_after_mb: float
    memory_delta_mb: float
    timestamp: str


class RedisStreamBenchmark:
    def __init__(
        self, host="localhost", port=6379, db=0, password=None, uri=None, stream_key="bench_stream"
    ):
        if uri:
            # Parse connection URI (redis://[:password@]host[:port][/db])
            parsed = urlparse(uri)
            if parsed.scheme not in ("redis", "rediss"):
                raise ValueError("URI must start with redis:// or rediss://")

            host = parsed.hostname or "localhost"
            port = parsed.port or 6379
            password = parsed.password
            if parsed.path:
                try:
                    db = int(parsed.path.lstrip("/"))
                except (ValueError, IndexError):
                    db = 0

        self.r = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)
        self.stream_key = stream_key
        self.results: List[BenchmarkResult] = []

        # Verify connection
        try:
            self.r.ping()
            print(f"Connected to Redis at {host}:{port}")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            raise

    def cleanup_stream(self):
        """Delete the stream to ensure clean state"""
        self.r.delete(self.stream_key)

    def get_memory_usage(self) -> float:
        """Get server used_memory in MB from Redis INFO"""
        return self.r.info("memory")["used_memory"] / 1024 / 1024

    def benchmark_xadd(self, num_ops: int = 10000, num_threads: int = 1) -> BenchmarkResult:
        """Benchmark XADD command (adding entries to stream)"""
        print(f"\nBenchmarking XADD ({num_ops} ops, {num_threads} threads)...")

        self.cleanup_stream()
        mem_before = self.get_memory_usage()
        latencies = []

        def add_entry(entry_num):
            start = time.perf_counter()
            self.r.xadd(self.stream_key, _make_payload(entry_num))
            elapsed = (time.perf_counter() - start) * 1000  # Convert to ms
            return elapsed

        start_time = time.perf_counter()

        if num_threads == 1:
            latencies = [add_entry(i) for i in range(num_ops)]
        else:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                latencies = list(executor.map(add_entry, range(num_ops)))

        duration = time.perf_counter() - start_time
        mem_after = self.get_memory_usage()

        return self._create_result(
            "producer", "XADD", num_ops, duration, latencies, mem_before, mem_after
        )

    def benchmark_xread(self, num_ops: int = 10000, num_entries: int = 5000) -> BenchmarkResult:
        """Benchmark XREAD command (reading entries)"""
        print(f"\nBenchmarking XREAD ({num_ops} ops on {num_entries} entries)...")

        # Pre-populate stream
        print(f"  Populating stream with {num_entries} entries...")
        for i in range(num_entries):
            self.r.xadd(
                self.stream_key,
                _make_payload(i),
            )

        mem_before = self.get_memory_usage()
        latencies = []

        # Read from various positions
        start_time = time.perf_counter()
        for i in range(num_ops):
            start = time.perf_counter()
            self.r.xread({self.stream_key: "0"}, count=10)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)

        duration = time.perf_counter() - start_time
        mem_after = self.get_memory_usage()

        return self._create_result(
            "consumer", "XREAD", num_ops, duration, latencies, mem_before, mem_after
        )

    def benchmark_xrange(self, num_ops: int = 10000, num_entries: int = 5000) -> BenchmarkResult:
        """Benchmark XRANGE command (range queries)"""
        print(f"\nBenchmarking XRANGE ({num_ops} ops on {num_entries} entries)...")

        # Pre-populate stream
        print(f"  Populating stream with {num_entries} entries...")
        for i in range(num_entries):
            self.r.xadd(
                self.stream_key,
                _make_payload(i),
            )

        mem_before = self.get_memory_usage()
        latencies = []

        start_time = time.perf_counter()
        for i in range(num_ops):
            start = time.perf_counter()
            self.r.xrange(self.stream_key, "-", "+", count=100)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)

        duration = time.perf_counter() - start_time
        mem_after = self.get_memory_usage()

        return self._create_result(
            "range_query", "XRANGE", num_ops, duration, latencies, mem_before, mem_after
        )

    def benchmark_consumer_group(
        self, num_ops: int = 10000, num_consumers: int = 3, num_entries: int = 5000
    ) -> BenchmarkResult:
        """Benchmark consumer group operations (create group, read, ack)"""
        print(f"\nBenchmarking Consumer Groups ({num_ops} ops, {num_consumers} consumers)...")

        group_name = "bench_group"

        # Pre-populate stream
        print(f"  Populating stream with {num_entries} entries...")
        for i in range(num_entries):
            self.r.xadd(
                self.stream_key,
                _make_payload(i),
            )

        # Create consumer group
        try:
            self.r.xgroup_create(self.stream_key, group_name, id="0", mkstream=False)
        except redis.ResponseError:
            self.r.xgroup_destroy(self.stream_key, group_name)
            self.r.xgroup_create(self.stream_key, group_name, id="0", mkstream=False)

        mem_before = self.get_memory_usage()
        latencies = []

        def process_messages(consumer_id):
            consumer_latencies = []
            ops_per_consumer = num_ops // num_consumers

            for _ in range(ops_per_consumer):
                # Read pending messages
                start = time.perf_counter()
                messages = self.r.xreadgroup(
                    group_name, consumer_id, {self.stream_key: ">"}, count=1
                )

                if messages and messages[0][1]:
                    msg_id = messages[0][1][0][0]
                    self.r.xack(self.stream_key, group_name, msg_id)

                elapsed = (time.perf_counter() - start) * 1000
                consumer_latencies.append(elapsed)

            return consumer_latencies

        start_time = time.perf_counter()

        with ThreadPoolExecutor(max_workers=num_consumers) as executor:
            futures = [
                executor.submit(process_messages, f"consumer_{i}") for i in range(num_consumers)
            ]
            for future in as_completed(futures):
                latencies.extend(future.result())

        duration = time.perf_counter() - start_time
        mem_after = self.get_memory_usage()

        self.r.xgroup_destroy(self.stream_key, group_name)

        return self._create_result(
            "consumer_group", "XREADGROUP+XACK", num_ops, duration, latencies, mem_before, mem_after
        )

    def benchmark_mixed_workload(
        self, duration_seconds: int = 30, num_threads: int = 5
    ) -> List[BenchmarkResult]:
        """Benchmark realistic mixed workload (producers + consumers)"""
        print(f"\nBenchmarking Mixed Workload ({duration_seconds}s, {num_threads} threads)...")

        self.cleanup_stream()
        group_name = "mixed_group"

        try:
            self.r.xgroup_create(self.stream_key, group_name, id="0", mkstream=True)
        except redis.ResponseError:
            pass

        mem_before = self.get_memory_usage()
        stop_flag = False
        stats = {"producers": 0, "consumers": 0, "acks": 0}
        latencies = {"xadd": [], "xread": [], "xack": []}

        def producer():
            count = 0
            while not stop_flag:
                start = time.perf_counter()
                self.r.xadd(self.stream_key, _make_payload(count))
                latencies["xadd"].append((time.perf_counter() - start) * 1000)
                count += 1
            stats["producers"] = count

        def consumer(consumer_id):
            count = 0
            while not stop_flag:
                start = time.perf_counter()
                messages = self.r.xreadgroup(
                    group_name,
                    f"consumer_{consumer_id}",
                    {self.stream_key: ">"},
                    count=5,
                    block=100,
                )
                latencies["xread"].append((time.perf_counter() - start) * 1000)

                if messages:
                    for msg_id in [m[0] for m in messages[0][1]]:
                        ack_start = time.perf_counter()
                        self.r.xack(self.stream_key, group_name, msg_id)
                        latencies["xack"].append((time.perf_counter() - ack_start) * 1000)
                        stats["acks"] += 1
                count += 1
            stats["consumers"] = count

        start_time = time.perf_counter()
        threads = []

        # Start producers and consumers
        num_producers = max(1, num_threads // 3)
        num_consumers = num_threads - num_producers

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for i in range(num_producers):
                executor.submit(producer)
            for i in range(num_consumers):
                executor.submit(consumer, i)

            time.sleep(duration_seconds)
            stop_flag = True

        actual_duration = time.perf_counter() - start_time
        mem_after = self.get_memory_usage()

        results = []
        for cmd, lats in latencies.items():
            if lats:
                results.append(
                    self._create_result(
                        "mixed_workload",
                        cmd.upper(),
                        len(lats),
                        actual_duration,
                        lats,
                        mem_before,
                        mem_after,
                    )
                )

        self.r.xgroup_destroy(self.stream_key, group_name)

        return results

    def _create_result(
        self,
        scenario: str,
        command: str,
        num_ops: int,
        duration: float,
        latencies: List[float],
        mem_before: float,
        mem_after: float,
    ) -> BenchmarkResult:
        """Create a BenchmarkResult from metrics"""
        sorted_latencies = sorted(latencies)

        return BenchmarkResult(
            scenario=scenario,
            command=command,
            num_operations=num_ops,
            duration_seconds=round(duration, 3),
            throughput_ops_sec=round(num_ops / duration, 2),
            min_latency_ms=round(min(latencies), 3),
            max_latency_ms=round(max(latencies), 3),
            avg_latency_ms=round(statistics.mean(latencies), 3),
            p50_latency_ms=round(sorted_latencies[int((len(sorted_latencies) - 1) * 0.50)], 3),
            p95_latency_ms=round(sorted_latencies[int((len(sorted_latencies) - 1) * 0.95)], 3),
            p99_latency_ms=round(sorted_latencies[int((len(sorted_latencies) - 1) * 0.99)], 3),
            memory_before_mb=round(mem_before, 2),
            memory_after_mb=round(mem_after, 2),
            memory_delta_mb=round(mem_after - mem_before, 2),
            timestamp=datetime.now().isoformat(),
        )

    def save_results_csv(self, filename: str = "redis_benchmark_results.csv"):
        """Save results to CSV"""
        if not self.results:
            print("No results to save")
            return

        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=asdict(self.results[0]).keys())
            writer.writeheader()
            for result in self.results:
                writer.writerow(asdict(result))

        print(f"\nResults saved to {filename}")

    def print_results_summary(self):
        """Print summary of all results"""
        print("\n" + "=" * 120)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 120)

        for result in self.results:
            print(f"\n{result.scenario.upper()} - {result.command}")
            print(f"  Operations: {result.num_operations:,}")
            print(f"  Duration: {result.duration_seconds:.3f}s")
            print(f"  Throughput: {result.throughput_ops_sec:,.0f} ops/sec")
            print(
                f"  Latency - Min: {result.min_latency_ms:.3f}ms, "
                f"Avg: {result.avg_latency_ms:.3f}ms, "
                f"P95: {result.p95_latency_ms:.3f}ms, "
                f"P99: {result.p99_latency_ms:.3f}ms, "
                f"Max: {result.max_latency_ms:.3f}ms"
            )
            print(
                f"  Memory - Before: {result.memory_before_mb:.2f}MB, "
                f"After: {result.memory_after_mb:.2f}MB, "
                f"Delta: {result.memory_delta_mb:+.2f}MB"
            )

        print("\n" + "=" * 120)

    def run_full_benchmark_suite(
        self,
        xadd_num_ops=10000,
        xread_num_ops=5000,
        xread_num_entries=5000,
        xrange_num_ops=5000,
        xrange_num_entries=5000,
        consumer_group_num_ops=5000,
        mixed_duration_seconds=30,
        threads=4,
    ):
        """Run all benchmarks"""
        print("\nStarting Redis Stream Benchmark Suite...")
        print(f"Stream Key: {self.stream_key}")

        # Operation benchmarks
        self.results.append(self.benchmark_xadd(num_ops=xadd_num_ops, num_threads=threads))
        self.results.append(
            self.benchmark_xread(num_ops=xread_num_ops, num_entries=xread_num_entries)
        )
        self.results.append(
            self.benchmark_xrange(num_ops=xrange_num_ops, num_entries=xrange_num_entries)
        )
        self.results.append(
            self.benchmark_consumer_group(num_ops=consumer_group_num_ops, num_consumers=threads)
        )

        # Mixed workload
        mixed_results = self.benchmark_mixed_workload(
            duration_seconds=mixed_duration_seconds, num_threads=threads
        )
        self.results.extend(mixed_results)

        self.print_results_summary()
        self.cleanup_stream()


def positive_int(value):
    """Validator for positive integers"""
    num = int(value)
    if num <= 0:
        raise argparse.ArgumentTypeError(f"{value} must be greater than 0")
    return num


def main():
    parser = argparse.ArgumentParser(description="Redis Stream Benchmark Suite")
    parser.add_argument("--uri", help="Redis connection URI (redis://[:password@]host[:port][/db])")
    parser.add_argument("--hostname", default="localhost", help="Redis hostname")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--db", type=int, default=0, help="Redis database number")
    parser.add_argument("--password", help="Redis password")
    parser.add_argument("--output", default="redis_benchmark_results.csv", help="Output CSV file")
    parser.add_argument("--full", action="store_true", help="Run full benchmark suite")
    parser.add_argument("--xadd", action="store_true", help="Benchmark XADD only")
    parser.add_argument("--xread", action="store_true", help="Benchmark XREAD only")
    parser.add_argument("--xrange", action="store_true", help="Benchmark XRANGE only")
    parser.add_argument("--consumer-group", action="store_true", help="Benchmark consumer groups")
    parser.add_argument("--mixed", action="store_true", help="Benchmark mixed workload")

    # Benchmark parameters
    parser.add_argument(
        "--xadd-num-ops",
        type=positive_int,
        default=10000,
        help="Number of XADD operations (default: 10000)",
    )
    parser.add_argument(
        "--xread-num-ops",
        type=positive_int,
        default=5000,
        help="Number of XREAD operations (default: 5000)",
    )
    parser.add_argument(
        "--xread-num-entries",
        type=positive_int,
        default=5000,
        help="Number of entries to pre-populate for XREAD (default: 5000)",
    )
    parser.add_argument(
        "--xrange-num-ops",
        type=positive_int,
        default=5000,
        help="Number of XRANGE operations (default: 5000)",
    )
    parser.add_argument(
        "--xrange-num-entries",
        type=positive_int,
        default=5000,
        help="Number of entries to pre-populate for XRANGE (default: 5000)",
    )
    parser.add_argument(
        "--consumer-group-num-ops",
        type=positive_int,
        default=5000,
        help="Number of consumer group operations (default: 5000)",
    )
    parser.add_argument(
        "--mixed-duration-seconds",
        type=positive_int,
        default=30,
        help="Duration of mixed workload in seconds (default: 30)",
    )
    parser.add_argument(
        "--threads",
        type=positive_int,
        default=4,
        help="Number of threads for XADD, consumer group, and mixed workload (default: 4)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Random seed for reproducible payload generation (default: None, non-deterministic)",
    )

    args = parser.parse_args()

    random.seed(args.seed)

    bench = RedisStreamBenchmark(
        host=args.hostname,
        port=args.port,
        db=args.db,
        password=args.password,
        uri=args.uri,
    )

    try:
        if args.full or not any(
            [args.xadd, args.xread, args.xrange, args.consumer_group, args.mixed]
        ):
            bench.run_full_benchmark_suite(
                xadd_num_ops=args.xadd_num_ops,
                xread_num_ops=args.xread_num_ops,
                xread_num_entries=args.xread_num_entries,
                xrange_num_ops=args.xrange_num_ops,
                xrange_num_entries=args.xrange_num_entries,
                consumer_group_num_ops=args.consumer_group_num_ops,
                mixed_duration_seconds=args.mixed_duration_seconds,
                threads=args.threads,
            )
        else:
            if args.xadd:
                bench.results.append(
                    bench.benchmark_xadd(num_ops=args.xadd_num_ops, num_threads=args.threads)
                )
            if args.xread:
                bench.results.append(
                    bench.benchmark_xread(
                        num_ops=args.xread_num_ops, num_entries=args.xread_num_entries
                    )
                )
            if args.xrange:
                bench.results.append(
                    bench.benchmark_xrange(
                        num_ops=args.xrange_num_ops, num_entries=args.xrange_num_entries
                    )
                )
            if args.consumer_group:
                bench.results.append(
                    bench.benchmark_consumer_group(
                        num_ops=args.consumer_group_num_ops, num_consumers=args.threads
                    )
                )
            if args.mixed:
                bench.results.extend(
                    bench.benchmark_mixed_workload(
                        duration_seconds=args.mixed_duration_seconds, num_threads=args.threads
                    )
                )

            bench.print_results_summary()

        bench.save_results_csv(args.output)

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted")
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()
