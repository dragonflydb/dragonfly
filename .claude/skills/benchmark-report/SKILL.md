---
name: benchmark-report
description: Generate a technical benchmark report in markdown from raw memtier/redis-benchmark output. Use this whenever the user provides benchmark results, log files, memtier JSON output, or a directory of benchmark data and wants a written report or comparison document. Trigger even when the user says "write up these results", "make a report from this data", "turn this into a blog post", or just drops a directory path. Always use this skill when the user provides benchmark files and wants a human-readable output. Reports compare Dragonfly against another system (ElastiCache, Redis, Valkey) but can handle single-system data too.
version: 1.0.0
---

# Benchmark Report Generator

Generate a technical markdown report from raw memtier/redis-benchmark data. The report is aimed at engineers evaluating Dragonfly for production use.

## Step 0: Interview before writing

Before touching any files, ask the user 3–4 targeted questions to fill in context that cannot be extracted from the raw data. Read the data directory first so your questions are specific, not generic.

Questions to ask (adapt based on what's missing after scanning the files):

1. **Scenario**: What is the purpose of this benchmark? (e.g., "SSD tiering comparison", "evaluating ElastiCache replacement", "testing read-heavy workload at scale") — this becomes the intro paragraph and title.
2. **Instance types**: What instance type / tier was each system running on? Include memory size.
3. **Storage configuration**: For Dragonfly — is SSD tiering enabled? What is the memory limit vs total dataset size?
4. **Anything else relevant**: Pricing comparison, region, network setup, anything not in the scripts/logs.

Wait for the user's answers before proceeding to Step 1. If the user says "just use TODOs" or similar, proceed without waiting.

## What the user provides

- A directory path with benchmark data files
- Context answers from the interview above

Typical directory layout:
```
data_dir/
  *.sh        — benchmark script (parameters, key distribution, test phases)
  *.json      — memtier JSON output per system per phase
  *.log       — memtier log output per system
```

Files may be flat in one directory or split across subdirectories. Read them all.

## Step 1: Scan and parse

List all files in the directory. Then read them:

**Scripts (`*.sh`)** — extract:
- `KEY_MAXIMUM`, `DATA_SIZE`, `PIPELINE`, `CLIENTS`, `THREADS`, `TEST_TIME`
- `FILL_RATIO`, `ACCESS_RATIO`, `FILL_KEY_PATTERN`, `ACCESS_KEY_PATTERN`
- Key distribution flags: `--key-stddev`, `--key-zipf-exp`, etc.
- Which phases ran (`RUN_FILL=true`, `RUN_ACCESS_READ_002=true`, etc.)
- Hostnames — used to identify systems under test

**JSON files (`*.json`)** — from memtier `--json-out-file` output:
- `configuration.server` → hostname (used to identify the system)
- `configuration.clients`, `threads`, `pipeline`, `ratio`, `key_maximum`, `data_size`, `key_stddev`
- `ALL STATS.Gets` or `ALL STATS.Sets` → `Ops/sec`, `Average Latency`, `Percentile Latencies` (p50/p99/p99.9)
- `ALL STATS.Runtime.Total duration` → test duration in ms

**Log files (`*.log`)** — from memtier stdout:
- Look for the `ALL STATS` table block. Take the last occurrence (final aggregate):
  ```
  Type         Ops/sec   ...  Avg. Latency  p50 Latency  p99 Latency  p99.9 Latency
  Sets      220503.72    ...     4.32106       2.95900      9.72700      53.24700
  Gets      138188.17    ...     6.94510       5.53500     36.09500      54.27100
  ```
- Look for `real Xm Y.Zs` timing lines to capture wall-clock duration per phase

## Step 2: Identify systems under test

Name each system from filename signals and hostnames:

| Signal | System name |
|--------|------------|
| `bench_df*.log`, hostname contains `dragonflydb.cloud` | Dragonfly |
| `bench_ec*.log`, hostname contains `cache.amazonaws.com` | ElastiCache |
| `bench_valkey*.log` | Valkey |
| `bench_redis*.log` | Redis |
| `bench_local*.log`, `127.0.0.1` | Local (note version if available) |

Use the actual product name, not the hostname. Do not include hostnames or endpoints in the report — they are not relevant for reproducing the results and may change.

## Step 3: Generate charts

If JSON files are present, generate latency charts with `tools/plot_memtier_latency.py`:

```bash
mkdir -p <data_dir>/charts
python3 <repo_root>/tools/plot_memtier_latency.py <json_file> <data_dir>/charts/<name>.html
```

Run once per JSON file. If the script is not found or fails, leave a `[TODO: chart]` stub. The script skips operations with no traffic (zero-latency entries), so it handles write-only fill JSONs and read-only access JSONs without errors.

To find `tools/plot_memtier_latency.py`, search from the current working directory: `find . -name "plot_memtier_latency.py" -not -path "*/build*"`.

## Step 4: Write the report

Save to `<data_dir>/report.md`. Use the template below. Populate every value you found; use `[TODO]` for anything missing (instance type, memory, pricing, scenario context the user didn't provide).

---

```markdown
# <Descriptive title: what was tested, e.g., "Dragonfly SSD Tiering vs ElastiCache: 50 GB Read Workload">

<1–3 sentence intro: what was measured and why. No adjectives. Example: "This benchmark measures read throughput and latency for a 50 GB key-value dataset under a Gaussian access pattern. Dragonfly was configured with SSD tiering; ElastiCache r7g.4xlarge was used with all data in memory.">

## Test Environment

| | Dragonfly | ElastiCache |
|---|---|---|
| Version / tier | [TODO] | [TODO] |
| Instance type | [TODO] | [TODO] |
| Memory | [TODO] | [TODO] |
| Storage | [TODO: SSD tiering?] | [TODO] |
| Region | [TODO] | [TODO] |

<!-- Do NOT include hostnames, endpoints, or internal URLs — they are irrelevant to reproducing the test. -->

## Dataset

| Parameter | Value |
|---|---|
| Key count | <from KEY_MAXIMUM, formatted with commas> |
| Value size | <DATA_SIZE> bytes |
| Approximate dataset size | <KEY_MAXIMUM × DATA_SIZE, in GB> |
| Key distribution | <Gaussian σ=X% of keys / uniform / Zipf exp=X> |

## Benchmark Configuration

| Parameter | Value |
|---|---|
| Tool | memtier_benchmark |
| Threads | <THREADS> |
| Connections per thread | <CLIENTS> |
| Pipeline depth | <PIPELINE> |
| Test duration (access phase) | <TEST_TIME>s |
| Access ratio (SET:GET) | <ratio> |

## Results

### Fill Phase

| System | Throughput (ops/sec) | Avg Latency (ms) | p99 Latency (ms) | Duration |
|---|---|---|---|---|
| Dragonfly | | | | |
| ElastiCache | | | | |

### Read Access Phase

| System | Throughput (ops/sec) | Avg Latency (ms) | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---|---|---|---|---|
| Dragonfly | | | | | |
| ElastiCache | | | | | |

## Latency Over Time

<For each JSON file, embed the chart link or stub>

**Dragonfly — read access (σ=2%)**
![Dragonfly latency over time](./charts/df_access.html)

**ElastiCache — read access (σ=2%)**
[TODO: ElastiCache JSON not available; chart cannot be generated]

## Observations

<Factual, numbered. One observation per line. State what the data shows; do not say whether it is good or bad.>

1. Throughput: Dragonfly achieved X ops/sec vs Y for ElastiCache — a Z% difference.
2. Average latency: X ms (Dragonfly) vs Y ms (ElastiCache).
3. p99 latency: X ms vs Y ms. <If there's a large gap relative to p50, note it.>
4. Fill phase: <note fill throughput and duration for each system>.
5. <Note any caveats: e.g., "This run covers only 100% read traffic. Mixed read/write was not executed.">
6. <Note anything unusual in the time-series if visible from the data.>
```

---

## Writing rules

- Use exact numbers from the data. No rounding unless the value is very large (round to 3 significant figures max).
- Percentages for comparisons: compute `(A - B) / B * 100`; write as "X% higher" or "X% lower".
- Prohibited phrases: "blazing fast", "industry-leading", "dramatically", "significantly", "next-level", "game-changing", "unparalleled", "impressive"
- No narrative hooks: no "but wait", "what happens next", "the results speak for themselves", "as you can see"
- Observations may include inferences (e.g., "the Gaussian σ=2% means the hot working set is ~3.5M keys, likely fitting in memory") — label them clearly as inferences if they go beyond what the data directly shows
- Bold labels in observations (e.g., `**Read throughput**:`) — keep them for scannability
- Do not include hostnames, endpoints, or internal URLs anywhere in the report
- Missing context → `[TODO: ...]` not a guess
- Single-system data: omit comparison columns; keep observations factual about absolute numbers

## Output

Print the report path when done: `Report written to: <path>/report.md`

If charts were generated, list them too.
