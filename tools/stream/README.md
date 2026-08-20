# Redis Stream Benchmarking Suite - Complete Guide

> **Note:** This tool was written with [Claude Code](https://claude.com/claude-code)

## Overview

This benchmarking suite provides comprehensive performance testing for Redis stream commands in real-world scenarios. It measures throughput, latency, and memory usage across different workload patterns.

## Features

### Measured Metrics
- **Throughput**: Operations per second (ops/sec)
- **Latency**: Min, max, average, and percentiles (P50, P95, P99)
- **Memory**: Before/after memory usage and delta
- **Efficiency**: Throughput per unit of memory overhead
- **Memory Analysis**: Track memory consumption changes across benchmark runs

### Covered Commands
- `XADD` - Adding entries to streams
- `XREAD` - Reading entries from streams
- `XRANGE` - Range queries on streams
- `XREADGROUP` + `XACK` - Consumer group operations

### Payload
Each stream entry uses a generic variable-size payload with realistic fields: `seq`, `ts`, `user_id`, `action`, `status`, `region`, and `data` blob. All entries carry a random alphanumeric `data` field (8–64 bytes) to simulate traffic with variable message sizes.

Use `--seed` for reproducible payload sequences across runs. Default: 0 (non-deterministic).

### Workload Scenarios
1. **Producer** - XADD performance with configurable thread count
2. **Consumer** - XREAD performance
3. **Range Query** - XRANGE performance
4. **Consumer Groups** - Complete consumer group workflow (XREADGROUP + XACK)
5. **Mixed Workload** - Concurrent producers and consumers; reports XADD, XREAD, and XACK latency separately

## Installation

### Prerequisites
```bash
pip install redis pandas
```

### Files
- `stream_benchmark.py` - Main benchmarking script
- `stream_benchmark_analyzer.py` - Analysis and comparison tool
- This guide

## Quick Start

### 1. Run Full Benchmark Suite

```bash
python3 stream_benchmark.py --full
```

This runs all benchmarks and saves results to:
- `redis_benchmark_results.csv` - Comma-separated values

### 2. Run Specific Benchmarks

```bash
# XADD only
python3 stream_benchmark.py --xadd

# XREAD only
python3 stream_benchmark.py --xread

# Consumer groups only
python3 stream_benchmark.py --consumer-group

# Mixed workload
python3 stream_benchmark.py --mixed
```

### 3. Analyze Results

```bash
# Comprehensive analysis
python3 stream_benchmark_analyzer.py redis_benchmark_results.csv --all

# Compare two runs
python3 stream_benchmark_analyzer.py run_1.csv run_2.csv --all

# Export comparison
python3 stream_benchmark_analyzer.py run_1.csv run_2.csv --export comparison.csv
```

## Configuration Options

### Benchmark Script

```bash
python3 stream_benchmark.py [OPTIONS]

Connection Options:
  --uri URI                Redis connection URI (redis://[:password@]host[:port][/db])
  --hostname HOSTNAME      Redis hostname (default: localhost)
  --port PORT              Redis port (default: 6379)
  --db DB                  Redis database (default: 0)
  --password PASSWORD      Redis password
  --output FILE            Output CSV file (default: redis_benchmark_results.csv)

Benchmark Selection:
  --full                   Run full benchmark suite
  --xadd                   Benchmark XADD only
  --xread                  Benchmark XREAD only
  --xrange                 Benchmark XRANGE only
  --consumer-group         Benchmark consumer groups
  --mixed                  Benchmark mixed workload

Benchmark Parameters (must be > 0):
  --xadd-num-ops NUM             Number of XADD operations (default: 10000)
  --xread-num-ops NUM            Number of XREAD operations (default: 5000)
  --xread-num-entries NUM        Pre-populate entries for XREAD (default: 5000)
  --xrange-num-ops NUM           Number of XRANGE operations (default: 5000)
  --xrange-num-entries NUM       Pre-populate entries for XRANGE (default: 5000)
  --consumer-group-num-ops NUM   Number of consumer group ops (default: 5000)
  --mixed-duration-seconds NUM   Duration of mixed workload (default: 30)
  --threads NUM                  Threads for XADD, consumer group, and mixed workload (default: 4)
  --seed NUM                     Random seed for reproducible payloads (default: 0, non-deterministic)
```

#### Connection Examples

```bash
# Default (localhost:6379)
python3 stream_benchmark.py --full

# Specific host and port
python3 stream_benchmark.py --hostname redis.example.com --port 6380 --full

# With password
python3 stream_benchmark.py --hostname redis.example.com --password secret --full

# Using URI
python3 stream_benchmark.py --uri "redis://:password@redis.example.com:6380/0" --full

# URI with authentication
python3 stream_benchmark.py --uri "redis://:mypass@localhost:6379/1" --full
```

#### Custom Workload Parameters

```bash
# Stress test XADD with more threads
python3 stream_benchmark.py --xadd --xadd-num-ops 50000 --threads 8

# Run XREAD with more pre-populated entries
python3 stream_benchmark.py --xread --xread-num-ops 10000 --xread-num-entries 10000

# Quick test with fewer operations
python3 stream_benchmark.py --full --xadd-num-ops 1000 --xread-num-ops 1000 --xrange-num-ops 1000

# Extended mixed workload test
python3 stream_benchmark.py --mixed --mixed-duration-seconds 120 --threads 8

# Reproducible run (fixed seed)
python3 stream_benchmark.py --full --seed 42

# Full suite with custom parameters
python3 stream_benchmark.py --full \
  --xadd-num-ops 20000 \
  --xread-num-ops 10000 \
  --xrange-num-ops 10000 \
  --consumer-group-num-ops 10000 \
  --mixed-duration-seconds 60 \
  --threads 8 \
  --seed 42

# Parameter validation (must be > 0)
python3 stream_benchmark.py --xadd --xadd-num-ops 0  # Error: must be greater than 0
```

### Analyzer Script

```bash
python3 stream_benchmark_analyzer.py FILE [FILE...] [OPTIONS]

Options:
  --throughput            Compare throughput
  --latency               Compare latency
  --memory                Compare memory usage
  --efficiency            Analyze efficiency metrics
  --bottlenecks           Identify performance bottlenecks
  --regression            Detect performance regressions (includes memory analysis)
  --percentiles           Show latency percentile distribution
  --scenarios             Analyze by scenario (includes memory metrics)
  --cross                 Cross-run comparison (includes memory changes)
  --all                   Run all analyses
  --export FILE           Export comparison to CSV (includes memory delta changes)
```

## Real-World Scenarios

### Scenario 1: High-Throughput Message Queue (E-commerce Orders)

```bash
# Run with optimized settings for high throughput
python3 stream_benchmark.py --xadd --output orders_baseline.csv
python3 stream_benchmark_analyzer.py orders_baseline.csv --throughput --latency
```

### Scenario 2: Distributed Task Processing (Microservices)

```bash
# Simulate multiple services processing tasks
python3 stream_benchmark.py --consumer-group --output tasks_baseline.csv
python3 stream_benchmark_analyzer.py tasks_baseline.csv --scenarios --efficiency
```

### Scenario 3: Analytics/Event Streaming (Multiple producers & consumers)

```bash
# Run mixed workload to simulate real-world analytics
python3 stream_benchmark.py --mixed --output analytics_baseline.csv
python3 stream_benchmark_analyzer.py analytics_baseline.csv --all
```

### Scenario 4: Comparing Two Benchmark Runs

**Run 1:**
```bash
python3 stream_benchmark.py --full --output run1.csv
```

**Run 2 (with different settings):**
```bash
python3 stream_benchmark.py --full --output run2.csv
```

**Compare results (includes memory changes):**
```bash
python3 stream_benchmark_analyzer.py run1.csv run2.csv --all --regression
```

## Understanding the Results

### CSV Output Columns

| Column | Description |
|--------|-------------|
| scenario | Test scenario (single_producer, consumer_group, etc.) |
| command | Redis command tested (XADD, XREAD, etc.) |
| num_operations | Total operations executed |
| duration_seconds | Time taken (in seconds) |
| throughput_ops_sec | Operations per second |
| min_latency_ms | Minimum latency |
| max_latency_ms | Maximum latency |
| avg_latency_ms | Average latency |
| p50_latency_ms | Median latency (50th percentile) |
| p95_latency_ms | 95th percentile latency |
| p99_latency_ms | 99th percentile latency |
| memory_before_mb | Server `used_memory` before test (MB) |
| memory_after_mb | Server `used_memory` after test (MB) |
| memory_delta_mb | Server memory growth during test |
| timestamp | Test timestamp |

## Advanced Usage

### Custom Workload Testing

Modify `stream_benchmark.py` to add custom scenarios:

```python
def benchmark_custom_scenario(self, num_ops: int = 10000) -> BenchmarkResult:
    """Your custom benchmark scenario"""
    print(f"\nBenchmarking Custom Scenario...")

    mem_before = self.get_memory_usage()
    latencies = []

    start_time = time.perf_counter()
    for i in range(num_ops):
        start = time.perf_counter()
        # Your custom Redis operations here
        self.r.xadd(self.stream_key, {'data': f'custom_{i}'})
        elapsed = (time.perf_counter() - start) * 1000
        latencies.append(elapsed)

    duration = time.perf_counter() - start_time
    mem_after = self.get_memory_usage()

    return self._create_result(
        'custom_scenario',
        'CUSTOM_COMMAND',
        num_ops,
        duration,
        latencies,
        mem_before,
        mem_after
    )
```

### Comparing Different Redis Instances

```bash
# Benchmark against instance 1
python3 stream_benchmark.py --hostname redis1.example.com --output redis1.csv

# Benchmark against instance 2
python3 stream_benchmark.py --hostname redis2.example.com --output redis2.csv

# Compare with memory analysis
python3 stream_benchmark_analyzer.py redis1.csv redis2.csv --all --regression
```

### Remote Redis with Authentication

```bash
# Benchmark remote secured instance
python3 stream_benchmark.py --uri "redis://:mypass@prod-redis.internal:6379/0" --full --output prod_baseline.csv
```

### Long-Running Stability Tests

```bash
# Run multiple times and compare for memory leaks/performance degradation
python3 stream_benchmark.py --mixed --output run1.csv
python3 stream_benchmark.py --mixed --output run2.csv
python3 stream_benchmark_analyzer.py run1.csv run2.csv --regression
```

## Memory Analysis Features

Memory metrics reflect **server-side** `used_memory` from `INFO memory`, not client process RSS. This accurately shows how much memory Dragonfly/Redis consumed storing stream data before and after each benchmark.

The analyzer includes comprehensive memory tracking:

### Cross-Run Memory Comparison
```bash
python3 stream_benchmark_analyzer.py baseline.csv optimized.csv --cross
```

Shows memory delta changes between runs for each scenario.

### Regression Detection with Memory
```bash
python3 stream_benchmark_analyzer.py before.csv after.csv --regression
```

Reports:
- Memory delta changes (baseline → current)
- Identifies memory regressions or improvements

### Exported Comparison Data
```bash
python3 stream_benchmark_analyzer.py before.csv after.csv --export comparison.csv
```

Includes:
- `baseline_memory_delta` - Memory usage in baseline
- `memory_delta` - Memory usage in current run
- `memory_delta_change_MB` - Absolute memory change

## Best Practices

### 1. Baseline Before Optimization
```bash
# Always create a baseline first
python3 stream_benchmark.py --full --output baseline.csv
```

### 2. Isolate Variables
- Test one change at a time
- Run multiple times to reduce noise
- Ensure consistent Redis state

### 3. Realistic Data Sizes
- Use realistic payload sizes (match your production data)
- Test with production-like entry counts
- Consider memory constraints

### 4. Monitor System Resources
- Run benchmarks on isolated systems
- Monitor CPU, memory, and disk usage
- Use `redis-cli --stat` to observe Redis metrics

### 5. Statistical Significance
- Always look at percentiles (P95, P99) not just averages
- Run benchmarks multiple times
- Use analyzer to detect regressions across runs

## Example Workflow

```bash
# 1. Run first benchmark
python3 stream_benchmark.py --full --output benchmark_1.csv

# 2. Run second benchmark with different settings
python3 stream_benchmark.py --full --output benchmark_2.csv

# 3. Analyze results
python3 stream_benchmark_analyzer.py benchmark_1.csv benchmark_2.csv --all

# 4. Check for regressions (including memory)
python3 stream_benchmark_analyzer.py benchmark_1.csv benchmark_2.csv --regression

# 5. Export comparison
python3 stream_benchmark_analyzer.py benchmark_1.csv benchmark_2.csv --export comparison.csv

# 6. Review CSV in spreadsheet application
```

## Troubleshooting

### Connection Issues
- Check Redis is running: `redis-cli ping`
- Verify host/port using `--hostname` and `--port` options
- Test URI format with `redis-cli -u "your-uri"`

### Memory Issues
- Reduce operation counts using parameters: `--xadd-num-ops`, `--xread-num-ops`, etc.
- Use shorter mixed workload: `--mixed-duration-seconds 10` instead of 30
- Clean up Redis with `redis-cli FLUSHDB`
- Example: `python3 stream_benchmark.py --full --xadd-num-ops 1000 --xread-num-ops 1000`

### Inconsistent Results
- Run benchmarks on isolated systems with minimal background load
- Run multiple times to get consistent data

### Slow Benchmarks
- Reduce operation counts: `--xadd-num-ops 1000 --xread-num-ops 1000`
- Shorten mixed workload: `--mixed-duration-seconds 10`
- Test individual commands instead of `--full`
- Check network latency for remote Redis instances

## Support

For issues or questions:
1. Check Redis is running: `redis-cli ping`
2. Verify network connectivity with `--hostname` and `--port` options
3. Test URI format with `redis-cli -u "your-uri"`
4. Review logs for specific error messages
5. Test with reduced workload size
