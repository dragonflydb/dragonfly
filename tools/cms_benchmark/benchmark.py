#!/usr/bin/env python3 -u
"""
CMS (Count-Min Sketch) Benchmark: Dragonfly vs Redis Stack

This benchmark compares the performance of CMS operations between
Dragonfly's native implementation and Redis Stack's RedisBloom module.

Usage:
    python benchmark.py [--dragonfly-host HOST] [--redis-host HOST] [--iterations N]
"""

import argparse
import random
import statistics
import string
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, List, Tuple

import redis


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""
    operation: str
    server: str
    iterations: int
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    p50_time_ms: float
    p95_time_ms: float
    p99_time_ms: float
    ops_per_sec: float


def generate_random_items(count: int, length: int = 10) -> List[str]:
    """Generate random string items for testing."""
    return [
        "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
        for _ in range(count)
    ]


def measure_latencies(func: Callable, iterations: int) -> List[float]:
    """Execute a function multiple times and measure latencies in ms."""
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        latencies.append((end - start) * 1000)  # Convert to ms
    return latencies


def compute_stats(
    operation: str, server: str, latencies: List[float]
) -> BenchmarkResult:
    """Compute statistics from latency measurements."""
    sorted_latencies = sorted(latencies)
    total = sum(latencies)
    count = len(latencies)

    return BenchmarkResult(
        operation=operation,
        server=server,
        iterations=count,
        total_time_ms=total,
        avg_time_ms=statistics.mean(latencies),
        min_time_ms=min(latencies),
        max_time_ms=max(latencies),
        p50_time_ms=sorted_latencies[int(count * 0.50)],
        p95_time_ms=sorted_latencies[int(count * 0.95)],
        p99_time_ms=sorted_latencies[int(count * 0.99)],
        ops_per_sec=(count / total) * 1000 if total > 0 else 0,
    )


def print_result(result: BenchmarkResult):
    """Print a formatted benchmark result."""
    print(f"  {result.server}:")
    print(f"    Total time:    {result.total_time_ms:>10.2f} ms")
    print(f"    Avg latency:   {result.avg_time_ms:>10.4f} ms")
    print(f"    Min latency:   {result.min_time_ms:>10.4f} ms")
    print(f"    Max latency:   {result.max_time_ms:>10.4f} ms")
    print(f"    P50 latency:   {result.p50_time_ms:>10.4f} ms")
    print(f"    P95 latency:   {result.p95_time_ms:>10.4f} ms")
    print(f"    P99 latency:   {result.p99_time_ms:>10.4f} ms")
    print(f"    Throughput:    {result.ops_per_sec:>10.0f} ops/sec")


def print_comparison(df_result: BenchmarkResult, redis_result: BenchmarkResult):
    """Print comparison between Dragonfly and Redis results."""
    speedup = redis_result.avg_time_ms / df_result.avg_time_ms if df_result.avg_time_ms > 0 else 0
    print(f"  Speedup (Dragonfly vs Redis): {speedup:.2f}x")
    if speedup > 1:
        print(f"  → Dragonfly is {speedup:.2f}x faster")
    elif speedup < 1:
        print(f"  → Redis is {1/speedup:.2f}x faster")
    else:
        print("  → Performance is similar")


class CMSBenchmark:
    """CMS Benchmark suite."""

    def __init__(
        self,
        dragonfly_client: redis.Redis,
        redis_client: redis.Redis,
        iterations: int = 1000,
    ):
        self.df = dragonfly_client
        self.redis = redis_client
        self.iterations = iterations
        self.results: List[Tuple[BenchmarkResult, BenchmarkResult]] = []

    def setup(self):
        """Clean up any existing keys."""
        for client in [self.df, self.redis]:
            try:
                client.flushall()
            except Exception as e:
                print(f"Warning: Could not flush: {e}")

    def benchmark_initbydim(self, width: int = 1000, depth: int = 5):
        """Benchmark CMS.INITBYDIM operation."""
        print(f"\n{'='*60}")
        print(f"Benchmark: CMS.INITBYDIM (width={width}, depth={depth})")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        # Dragonfly
        key_counter = [0]

        def df_init():
            key = f"cms_df_{key_counter[0]}"
            key_counter[0] += 1
            self.df.execute_command("CMS.INITBYDIM", key, width, depth)

        df_latencies = measure_latencies(df_init, self.iterations)
        df_result = compute_stats("INITBYDIM", "Dragonfly", df_latencies)

        # Redis
        key_counter[0] = 0

        def redis_init():
            key = f"cms_redis_{key_counter[0]}"
            key_counter[0] += 1
            self.redis.execute_command("CMS.INITBYDIM", key, width, depth)

        redis_latencies = measure_latencies(redis_init, self.iterations)
        redis_result = compute_stats("INITBYDIM", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))
        return df_result, redis_result

    def benchmark_incrby(self, num_items: int = 100, increments_per_call: int = 10):
        """Benchmark CMS.INCRBY operation."""
        print(f"\n{'='*60}")
        print(f"Benchmark: CMS.INCRBY ({increments_per_call} items per call)")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        # Setup CMS structures
        self.df.execute_command("CMS.INITBYDIM", "bench_df", 2000, 7)
        self.redis.execute_command("CMS.INITBYDIM", "bench_redis", 2000, 7)

        items = generate_random_items(num_items)

        # Dragonfly
        def df_incrby():
            batch_items = random.sample(items, increments_per_call)
            args = []
            for item in batch_items:
                args.extend([item, random.randint(1, 100)])
            self.df.execute_command("CMS.INCRBY", "bench_df", *args)

        df_latencies = measure_latencies(df_incrby, self.iterations)
        df_result = compute_stats("INCRBY", "Dragonfly", df_latencies)

        # Redis
        def redis_incrby():
            batch_items = random.sample(items, increments_per_call)
            args = []
            for item in batch_items:
                args.extend([item, random.randint(1, 100)])
            self.redis.execute_command("CMS.INCRBY", "bench_redis", *args)

        redis_latencies = measure_latencies(redis_incrby, self.iterations)
        redis_result = compute_stats("INCRBY", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))
        return df_result, redis_result

    def benchmark_query(self, num_items: int = 100, queries_per_call: int = 10):
        """Benchmark CMS.QUERY operation."""
        print(f"\n{'='*60}")
        print(f"Benchmark: CMS.QUERY ({queries_per_call} items per call)")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        # Setup: Create and populate CMS structures
        self.df.execute_command("CMS.INITBYDIM", "query_df", 2000, 7)
        self.redis.execute_command("CMS.INITBYDIM", "query_redis", 2000, 7)

        items = generate_random_items(num_items)

        # Populate with data
        for i in range(0, len(items), 10):
            batch = items[i : i + 10]
            args = []
            for item in batch:
                args.extend([item, random.randint(1, 1000)])
            self.df.execute_command("CMS.INCRBY", "query_df", *args)
            self.redis.execute_command("CMS.INCRBY", "query_redis", *args)

        # Dragonfly
        def df_query():
            batch_items = random.sample(items, queries_per_call)
            self.df.execute_command("CMS.QUERY", "query_df", *batch_items)

        df_latencies = measure_latencies(df_query, self.iterations)
        df_result = compute_stats("QUERY", "Dragonfly", df_latencies)

        # Redis
        def redis_query():
            batch_items = random.sample(items, queries_per_call)
            self.redis.execute_command("CMS.QUERY", "query_redis", *batch_items)

        redis_latencies = measure_latencies(redis_query, self.iterations)
        redis_result = compute_stats("QUERY", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))
        return df_result, redis_result

    def benchmark_info(self):
        """Benchmark CMS.INFO operation."""
        print(f"\n{'='*60}")
        print("Benchmark: CMS.INFO")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        # Setup CMS structures
        self.df.execute_command("CMS.INITBYDIM", "info_df", 2000, 7)
        self.redis.execute_command("CMS.INITBYDIM", "info_redis", 2000, 7)

        # Dragonfly
        def df_info():
            self.df.execute_command("CMS.INFO", "info_df")

        df_latencies = measure_latencies(df_info, self.iterations)
        df_result = compute_stats("INFO", "Dragonfly", df_latencies)

        # Redis
        def redis_info():
            self.redis.execute_command("CMS.INFO", "info_redis")

        redis_latencies = measure_latencies(redis_info, self.iterations)
        redis_result = compute_stats("INFO", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))
        return df_result, redis_result

    def benchmark_merge(self, num_sources: int = 3):
        """Benchmark CMS.MERGE operation."""
        print(f"\n{'='*60}")
        print(f"Benchmark: CMS.MERGE ({num_sources} sources)")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        items = generate_random_items(50)
        merge_iterations = min(self.iterations, 500)  # Merge is heavier

        # Setup for Dragonfly
        for i in range(num_sources):
            self.df.execute_command("CMS.INITBYDIM", f"merge_src_df_{i}", 1000, 5)
            args = []
            for item in random.sample(items, 10):
                args.extend([item, random.randint(1, 100)])
            self.df.execute_command("CMS.INCRBY", f"merge_src_df_{i}", *args)

        # Setup for Redis
        for i in range(num_sources):
            self.redis.execute_command("CMS.INITBYDIM", f"merge_src_redis_{i}", 1000, 5)
            args = []
            for item in random.sample(items, 10):
                args.extend([item, random.randint(1, 100)])
            self.redis.execute_command("CMS.INCRBY", f"merge_src_redis_{i}", *args)

        # Create destination keys
        dest_counter = [0]

        # Dragonfly
        def df_merge():
            dest = f"merge_dest_df_{dest_counter[0]}"
            dest_counter[0] += 1
            self.df.execute_command("CMS.INITBYDIM", dest, 1000, 5)
            sources = [f"merge_src_df_{i}" for i in range(num_sources)]
            self.df.execute_command("CMS.MERGE", dest, num_sources, *sources)

        df_latencies = measure_latencies(df_merge, merge_iterations)
        df_result = compute_stats("MERGE", "Dragonfly", df_latencies)

        # Redis
        dest_counter[0] = 0

        def redis_merge():
            dest = f"merge_dest_redis_{dest_counter[0]}"
            dest_counter[0] += 1
            self.redis.execute_command("CMS.INITBYDIM", dest, 1000, 5)
            sources = [f"merge_src_redis_{i}" for i in range(num_sources)]
            self.redis.execute_command("CMS.MERGE", dest, num_sources, *sources)

        redis_latencies = measure_latencies(redis_merge, merge_iterations)
        redis_result = compute_stats("MERGE", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))
        return df_result, redis_result

    def benchmark_mixed_workload(self, read_ratio: float = 0.7):
        """Benchmark mixed read/write workload."""
        print(f"\n{'='*60}")
        print(f"Benchmark: Mixed Workload ({int(read_ratio*100)}% reads)")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        # Setup
        self.df.execute_command("CMS.INITBYDIM", "mixed_df", 2000, 7)
        self.redis.execute_command("CMS.INITBYDIM", "mixed_redis", 2000, 7)

        items = generate_random_items(100)

        # Pre-populate
        for item in items[:50]:
            self.df.execute_command("CMS.INCRBY", "mixed_df", item, 100)
            self.redis.execute_command("CMS.INCRBY", "mixed_redis", item, 100)

        # Dragonfly mixed workload
        def df_mixed():
            if random.random() < read_ratio:
                # Read operation
                batch = random.sample(items, 5)
                self.df.execute_command("CMS.QUERY", "mixed_df", *batch)
            else:
                # Write operation
                batch = random.sample(items, 3)
                args = []
                for item in batch:
                    args.extend([item, random.randint(1, 10)])
                self.df.execute_command("CMS.INCRBY", "mixed_df", *args)

        df_latencies = measure_latencies(df_mixed, self.iterations)
        df_result = compute_stats("MIXED", "Dragonfly", df_latencies)

        # Redis mixed workload
        def redis_mixed():
            if random.random() < read_ratio:
                batch = random.sample(items, 5)
                self.redis.execute_command("CMS.QUERY", "mixed_redis", *batch)
            else:
                batch = random.sample(items, 3)
                args = []
                for item in batch:
                    args.extend([item, random.randint(1, 10)])
                self.redis.execute_command("CMS.INCRBY", "mixed_redis", *args)

        redis_latencies = measure_latencies(redis_mixed, self.iterations)
        redis_result = compute_stats("MIXED", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))
        return df_result, redis_result

    def benchmark_baseline_get_set(self):
        """Benchmark basic GET/SET to compare framework overhead."""
        print(f"\n{'='*60}")
        print("Benchmark: BASELINE GET/SET (framework overhead comparison)")
        print(f"Iterations: {self.iterations}")
        print("=" * 60)

        # Setup: Create keys with values
        for i in range(100):
            self.df.set(f"bench_key_{i}", f"value_{i}" * 10)
            self.redis.set(f"bench_key_{i}", f"value_{i}" * 10)

        # Dragonfly GET
        def df_get():
            key = f"bench_key_{random.randint(0, 99)}"
            self.df.get(key)

        df_latencies = measure_latencies(df_get, self.iterations)
        df_result = compute_stats("GET", "Dragonfly", df_latencies)

        # Redis GET
        def redis_get():
            key = f"bench_key_{random.randint(0, 99)}"
            self.redis.get(key)

        redis_latencies = measure_latencies(redis_get, self.iterations)
        redis_result = compute_stats("GET", "Redis Stack", redis_latencies)

        print_result(df_result)
        print_result(redis_result)
        print_comparison(df_result, redis_result)

        self.results.append((df_result, redis_result))

        # Now test SET
        print(f"\n{'-'*60}")
        print("SET operation:")
        print("-" * 60)

        counter = [0]

        # Dragonfly SET
        def df_set():
            key = f"bench_set_{counter[0]}"
            counter[0] += 1
            self.df.set(key, "x" * 100)

        df_latencies = measure_latencies(df_set, self.iterations)
        df_result_set = compute_stats("SET", "Dragonfly", df_latencies)

        counter[0] = 0

        # Redis SET
        def redis_set():
            key = f"bench_set_{counter[0]}"
            counter[0] += 1
            self.redis.set(key, "x" * 100)

        redis_latencies = measure_latencies(redis_set, self.iterations)
        redis_result_set = compute_stats("SET", "Redis Stack", redis_latencies)

        print_result(df_result_set)
        print_result(redis_result_set)
        print_comparison(df_result_set, redis_result_set)

        self.results.append((df_result_set, redis_result_set))

        return df_result, redis_result

    def print_summary(self):
        """Print a summary table of all results."""
        print(f"\n{'='*60}")
        print("SUMMARY")
        print("=" * 60)
        print(
            f"{'Operation':<15} {'Dragonfly (ops/s)':<20} {'Redis (ops/s)':<20} {'Speedup':<10}"
        )
        print("-" * 65)

        for df_result, redis_result in self.results:
            speedup = (
                redis_result.avg_time_ms / df_result.avg_time_ms
                if df_result.avg_time_ms > 0
                else 0
            )
            speedup_str = f"{speedup:.2f}x"
            if speedup > 1:
                speedup_str = f"\033[92m{speedup_str}\033[0m"  # Green
            elif speedup < 1:
                speedup_str = f"\033[91m{1/speedup:.2f}x slower\033[0m"  # Red

            print(
                f"{df_result.operation:<15} {df_result.ops_per_sec:<20.0f} "
                f"{redis_result.ops_per_sec:<20.0f} {speedup_str:<10}"
            )

    def run_all(self):
        """Run all benchmarks."""
        self.setup()

        # First: baseline GET/SET to measure framework overhead
        self.benchmark_baseline_get_set()
        self.setup()

        self.benchmark_initbydim()
        self.setup()

        self.benchmark_incrby()
        self.setup()

        self.benchmark_query()
        self.setup()

        self.benchmark_info()
        self.setup()

        self.benchmark_merge()
        self.setup()

        self.benchmark_mixed_workload()

        self.print_summary()


def benchmark_concurrent(host: str, port: int, operation: Callable,
                         num_threads: int, ops_per_thread: int, name: str) -> dict:
    """Run concurrent benchmark with multiple threads."""

    def worker(thread_id: int) -> int:
        """Each worker creates its own connection and runs operations."""
        client = redis.Redis(host=host, port=port, decode_responses=True)
        count = 0
        for _ in range(ops_per_thread):
            operation(client, thread_id)
            count += 1
        client.close()
        return count

    start = time.perf_counter()
    total_ops = 0

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        for future in as_completed(futures):
            total_ops += future.result()

    elapsed = time.perf_counter() - start
    ops_per_sec = total_ops / elapsed

    return {
        "name": name,
        "total_ops": total_ops,
        "elapsed_sec": elapsed,
        "ops_per_sec": ops_per_sec,
        "threads": num_threads
    }


def wait_for_server(client: redis.Redis, name: str, max_retries: int = 30):
    """Wait for a server to be ready."""
    for i in range(max_retries):
        try:
            client.ping()
            print(f"✓ {name} is ready")
            return True
        except redis.ConnectionError:
            if i < max_retries - 1:
                print(f"  Waiting for {name}... ({i+1}/{max_retries})")
                time.sleep(1)
    print(f"✗ Failed to connect to {name}")
    return False


def run_concurrent_cms_benchmark(df_host: str, df_port: int,
                                  redis_host: str, redis_port: int,
                                  num_threads: int, ops_per_thread: int):
    """Run concurrent CMS benchmark."""
    print(f"\n{'='*70}")
    print(f"CONCURRENT CMS BENCHMARK ({num_threads} threads, {ops_per_thread} ops/thread)")
    print("=" * 70)

    items = generate_random_items(1000)

    # Setup CMS on both servers - CREATE MULTIPLE KEYS FOR SHARDING
    num_keys = num_threads  # One CMS per thread for max parallelism
    df_setup = redis.Redis(host=df_host, port=df_port, decode_responses=True)
    redis_setup = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    df_setup.flushall()
    redis_setup.flushall()

    for i in range(num_keys):
        df_setup.execute_command("CMS.INITBYDIM", f"bench_cms_{i}", 2000, 7)
        redis_setup.execute_command("CMS.INITBYDIM", f"bench_cms_{i}", 2000, 7)

    df_setup.close()
    redis_setup.close()

    # CMS.INCRBY operation - each thread uses its own key
    def cms_incrby(client, thread_id):
        key = f"bench_cms_{thread_id % num_keys}"
        batch_items = random.sample(items, 5)
        args = []
        for item in batch_items:
            args.extend([item, random.randint(1, 100)])
        client.execute_command("CMS.INCRBY", key, *args)

    # CMS.QUERY operation - each thread uses its own key
    def cms_query(client, thread_id):
        key = f"bench_cms_{thread_id % num_keys}"
        batch_items = random.sample(items, 5)
        client.execute_command("CMS.QUERY", key, *batch_items)

    # Test INCRBY
    print(f"\n--- CMS.INCRBY (5 items/call) ---")
    df_result = benchmark_concurrent(df_host, df_port, cms_incrby,
                                      num_threads, ops_per_thread, "Dragonfly")
    redis_result = benchmark_concurrent(redis_host, redis_port, cms_incrby,
                                         num_threads, ops_per_thread, "Redis")

    print(f"  Dragonfly: {df_result['ops_per_sec']:>12,.0f} ops/sec")
    print(f"  Redis:     {redis_result['ops_per_sec']:>12,.0f} ops/sec")
    speedup = df_result['ops_per_sec'] / redis_result['ops_per_sec']
    if speedup > 1:
        print(f"  → Dragonfly is {speedup:.2f}x FASTER ✅")
    else:
        print(f"  → Redis is {1/speedup:.2f}x faster")

    # Test QUERY
    print(f"\n--- CMS.QUERY (5 items/call) ---")
    df_result = benchmark_concurrent(df_host, df_port, cms_query,
                                      num_threads, ops_per_thread, "Dragonfly")
    redis_result = benchmark_concurrent(redis_host, redis_port, cms_query,
                                         num_threads, ops_per_thread, "Redis")

    print(f"  Dragonfly: {df_result['ops_per_sec']:>12,.0f} ops/sec")
    print(f"  Redis:     {redis_result['ops_per_sec']:>12,.0f} ops/sec")
    speedup = df_result['ops_per_sec'] / redis_result['ops_per_sec']
    if speedup > 1:
        print(f"  → Dragonfly is {speedup:.2f}x FASTER ✅")
    else:
        print(f"  → Redis is {1/speedup:.2f}x faster")


def main():
    parser = argparse.ArgumentParser(
        description="CMS Benchmark: Dragonfly vs Redis Stack"
    )
    parser.add_argument(
        "--dragonfly-host", default="localhost", help="Dragonfly host (default: localhost)"
    )
    parser.add_argument(
        "--dragonfly-port", type=int, default=6380, help="Dragonfly port (default: 6380)"
    )
    parser.add_argument(
        "--redis-host", default="localhost", help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-port", type=int, default=6381, help="Redis port (default: 6381)"
    )
    parser.add_argument(
        "--iterations", type=int, default=1000, help="Number of iterations (default: 1000)"
    )
    parser.add_argument(
        "--concurrent", action="store_true", help="Run concurrent benchmark only"
    )
    parser.add_argument(
        "--threads", type=int, default=50, help="Number of threads for concurrent test"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("CMS Benchmark: Dragonfly vs Redis Stack")
    print("=" * 60)

    # Connect to servers
    print("\nConnecting to servers...")

    df_client = redis.Redis(
        host=args.dragonfly_host, port=args.dragonfly_port, decode_responses=True
    )
    redis_client = redis.Redis(
        host=args.redis_host, port=args.redis_port, decode_responses=True
    )

    if not wait_for_server(df_client, "Dragonfly"):
        sys.exit(1)
    if not wait_for_server(redis_client, "Redis Stack"):
        sys.exit(1)

    if args.concurrent:
        # Run concurrent benchmark only
        run_concurrent_cms_benchmark(
            args.dragonfly_host, args.dragonfly_port,
            args.redis_host, args.redis_port,
            num_threads=args.threads,
            ops_per_thread=args.iterations
        )
    else:
        # Run sequential benchmarks
        benchmark = CMSBenchmark(df_client, redis_client, iterations=args.iterations)
        benchmark.run_all()

    # Cleanup
    df_client.close()
    redis_client.close()


if __name__ == "__main__":
    main()
