# Dragonfly Benchmark Orchestrator

This is an automated, configuration-driven benchmarking tool designed to run high-precision, isolated load tests against Dragonfly, Valkey, Redis, and Memcached using `memtier_benchmark`.

It automatically iterates through a defined matrix of target engines, pipeline depths, and read/write ratios, generating a unified CSV report of the results.

**Important Limitation:** This script is designed strictly for **single-session, sequential runs**. It does not support parallel execution of benchmarks. Running multiple instances of this script simultaneously will cause port conflicts and overwrite streamed logs.

---

## Execution Architecture & Log Streaming

The suite acts as a central **Orchestrator**.

1. **The Brain:** You always execute the script from your local machine (e.g., your laptop or a jumpbox).
2. **The Execution:** Based on your `bench.conf`, the script will either run commands locally or SSH into remote machines to start the databases and the load generators.
3. **The Data (Important):** **All results are saved locally.** Even when running a benchmark on a remote machine, the output of the database and `memtier_benchmark` is piped in real-time over SSH directly back to the `results/` folder on the machine where you typed `./df_bench_runner.sh run`. You will not find result files on the remote servers.



### Supported Topologies
By configuring the `SERVER_CONNECTION` and `CLIENT_CONNECTION` variables, the suite can operate in three distinct modes:

* **1. Fully Local:** Both the database and the load generator run on the local machine orchestrating the test.
  *(Setup: Set both variables to `"local"`).*
* **2. Remote Single-Node:** Both the database and the load generator run on the *same* remote machine. The script orchestrates them via SSH, and automatically routes the load generator through `127.0.0.1` on that remote box to bypass public cloud firewalls and measure raw engine performance.
  *(Setup: Set both variables to the exact same `"user@ip"`).*
* **3. Remote Multi-Node:** The database runs on one remote machine, and the load generator runs on another. This simulates real-world network latency.
  *(Setup: Set `SERVER_CONNECTION` and `CLIENT_CONNECTION` to different `"user@ip"` addresses).*

---

## 📋 Pre-requisites & Environment Setup

Before running the orchestrator, ensure your infrastructure meets the following requirements so the script can run completely unattended.

### 1. SSH Access & Host Keys (Crucial for Remote Runs)
The orchestrator relies heavily on SSH to trigger remote databases and clients.
* **Passwordless Authentication:** You must configure key-based SSH access (e.g., `~/.ssh/id_rsa` or `id_ed25519`) from your local orchestrator machine to your remote Server and Client machines. The script cannot handle interactive password prompts.
* **First-Time Connection (Known Hosts):** Even with SSH keys, Linux will prompt you to accept the host key fingerprint the *very first time* you connect to a new IP. **You must manually SSH into the Server and the Client at least once** from your terminal and type `yes` to add them to your `~/.ssh/known_hosts` file. If you skip this, the script will hang indefinitely waiting for user input.

### 2. Binary Placement (`raw` environments)
The script **does not** compile, upload, or transfer database binaries for you.
* If you define a target using the `raw` environment, the compiled executable (e.g., `valkey-server`, `dragonfly`) must already exist on the target machine at the exact path you specify in the `bench.conf` file.

### 3. Docker Images (`docker` environments)
* **Official/Public Images:** If you specify a standard image (e.g., `redis:7.2`), the Docker daemon on the target machine will automatically download it if it is not already present.
* **Custom/Private Images:** If you are benchmarking a custom build or a private image (e.g., `my-custom-dragonfly:latest`), you must ensure the image is already pulled or built on the target machine before running the suite.

## Configuration (`bench.conf`)

The behavior of the suite is completely controlled by the `bench.conf` file. *Note: The script performs an automatic syntax check on this file before running to prevent mid-test crashes.*

### Infrastructure Settings
* `SERVER_CONNECTION`: Where the database engine runs (e.g., `local` or `ubuntu@54.172.245.149`).
* `CLIENT_CONNECTION`: Where `memtier_benchmark` runs (e.g., `local` or `ubuntu@35.172.215.241`).

### Benchmark Settings
* `TEST_TIME`: Duration of each benchmark run in seconds.
* `CLIENT_THREADS`: Number of threads `memtier` uses (`-t`).
* `CLIENT_CONNS`: Number of connections per thread (`-c`).
* `KEY_MAX`: The key-space range for the load generator.
* `CLIENT_TASKSET`: CPU core pinning for the load generator (e.g., `"3"`). *Only applies when `CLIENT_CONNECTION="local"` to prevent resource contention.*

### Workload Matrix
* `RATIOS`: An array of read:write ratios (e.g., `("1:0" "1:1")`).
* `PIPELINES`: An array of pipeline depths (e.g., `("1" "30")`).
*The script multiplies your Targets × Ratios × Pipelines to determine the total number of runs.*

---

## Target Engines & Environments

The suite supports 4 combinations of execution by mixing **Location** (`local` vs `remote`) and **Environment** (`raw` vs `docker`).

The `TARGETS` array uses a strict pipe-separated (`|`) format:
`"Alias | Protocol | Port | Location | Env | Path_or_Image | Server_Args"`

| Column | Explanation |
| :--- | :--- |
| **Alias** | A unique name for the run (e.g., `valkey-remote`). |
| **Protocol** | `redis` or `memcache_text`. |
| **Port** | The port to bind to (e.g., `6379` or `11211`). |
| **Location** | `local` (runs on orchestrator) or `remote` (SSH). |
| **Env** | `raw` (executes a binary) or `docker` (runs a container). |
| **Path_or_Image** | For `raw`, the absolute file path to the binary. For `docker`, the image tag (e.g., `redis:7.2`). |
| **Server_Args** | Flags passed to the database (e.g., `--port 6379 --protected-mode no`). |

---

## Networking & Cloud Security Groups

When running in **Remote Multi-Node** mode, the orchestrator handles the SSH logic, but you must manually configure the cloud firewall (e.g., AWS Security Groups) to allow the database traffic.



### 1. Required Inbound Rules
On your **Server Instance**, you must add an Inbound Rule to its Security Group allowing the client to connect:

| Type | Protocol | Port Range | Source |
| :--- | :--- | :--- | :--- |
| Custom TCP | TCP | `6379` (or your DB port) | `<Client_Public_IP>/32` |

### 2. Automated Pre-Flight Check
The orchestrator includes a built-in `nc` (netcat) health check. Before launching `memtier_benchmark`, the Client will attempt to ping the Server's database port. If the connection is blocked, the script will instantly abort and clean up the server, saving you from waiting for a full test cycle just to get a "Connection timed out" error.

### 3. Server Bind Requirements
If your Client is external, ensure your target server arguments (`t_args` in `bench.conf`) allow external connections. For Redis/Valkey, this often means ensuring `--protected-mode no` is set.

---

## Usage Commands

### 1. Running the Benchmark Matrix
Executes the matrix defined in `bench.conf`.

```bash
# Standard execution (saves to default results/ folder)
./df_bench_runner.sh run

# Execute with a custom config file and a custom output directory
./df_bench_runner.sh run -c custom.conf -o /mnt/data/benchmarks
```
*(Note: During execution, your terminal will only show the live progress bar to keep the screen clean. The massive "ALL STATS" table from memtier is hidden from the terminal but safely saved to `memtier.log`).*

### 2. Generating the Report
Parses the local output logs and generates a clean `summary.csv` containing Ops/sec, Throughput, and Latency percentiles.

```bash
# Auto-Discovery (No Options):
# Automatically finds the most recently created directory in the results/ folder and parses it.
./df_bench_runner.sh report

# Manual Override:
# Parse a specific historical run directory
./df_bench_runner.sh report -d results/benchmark_20260301_135053
```

---

## Output Directory Structure

For every execution, a timestamped master directory is created locally. Inside, isolated subfolders are created for every individual permutation.

```text
results/benchmark_20260301_135053/
├── valkey-remote_ratio1_0_pipe1/
│   ├── memtier.log      # Raw performance stats from the client (Full Table)
│   ├── meta.env         # Internal metadata for the report parser
│   ├── resources.log    # top output (CPU/Mem) for raw local runs
│   └── server.log       # Real-time stdout/stderr from the database
├── redis-remote-docker_ratio1_0_pipe1/
│   └── ...
└── summary.csv          # Generated only after running the 'report' command
```

---

## Real-World Example: The "Overnight" Matrix

To understand the true power of this orchestrator, consider a real-world enterprise benchmarking scenario.

Imagine you need to evaluate if upgrading from **Dragonfly v1** to **Dragonfly v2** is worth it, while also comparing them against **Valkey** and **Memcached**. You need to test how each engine handles heavy reads, heavy writes, and balanced workloads, all while scaling pipeline depths to see where the network chokes.

Doing this manually would require sitting at a terminal for 12 hours typing commands and copy-pasting terminal output into Excel. With this orchestrator, it's a single command before you go to bed.

### The Math:
* **4 Target Engines** (DF v1, DF v2, Valkey, Memcached)
* **6 Read/Write Ratios** (from 100% Writes to 100% Reads)
* **6 Pipeline Depths** (from 1 to 200)
* **= 144 isolated, high-precision benchmark runs.**

At 5 minutes (`300` seconds) per run, **this single configuration will run completely unattended for exactly 12 hours**, generating a flawless 144-row `summary.csv` by the time you wake up.

### The `bench.conf`

```bash
# =================================================================
# THE 12-HOUR OVERNIGHT BENCHMARK (144 Runs)
# =================================================================

# --- Infrastructure (Remote Multi-Node) ---
SERVER_CONNECTION="ubuntu@10.0.1.50"
CLIENT_CONNECTION="ubuntu@10.0.1.51"

# --- Benchmark Settings ---
TEST_TIME=300         # 5 minutes per run
CLIENT_THREADS=8      # 8 threads per memtier instance
CLIENT_CONNS=64       # 64 connections per thread
KEY_MAX=10000000      # 10 Million key-space to prevent caching entirely

# --- Workload Matrix ---
# 100% Write | Heavy Write | Balanced | Heavy Read | Read-Only | 2:1 Custom
RATIOS=("1:0" "1:4" "1:1" "10:1" "0:1" "2:1")

# Network pressure scaling
PIPELINES=("1" "10" "30" "50" "100" "200")

# --- Target Engines ---
TARGETS=(
    # 1. Valkey (Compiled from source, using 4 I/O threads)
    "valkey-7.2 | redis | 6379 | remote | raw | /opt/valkey/src/valkey-server | --port 6379 --bind 0.0.0.0 --protected-mode no --io-threads 4"

    # 2. Dragonfly v1 (Legacy version via Docker)
    "dragonfly-v1 | redis | 6380 | remote | docker | docker.dragonflydb.io/dragonflydb/dragonfly:v1.15.0 | --port 6380 --proactor_threads=4"

    # 3. Dragonfly v2 (Latest version via Docker)
    "dragonfly-v2 | redis | 6381 | remote | docker | docker.dragonflydb.io/dragonflydb/dragonfly:latest | --port 6381 --proactor_threads=4"

    # 4. Memcached (Raw binary, using 4 threads)
    "memcached-1.6 | memcache_text | 11211 | remote | raw | /usr/bin/memcached | -p 11211 -u memcache -t 4 -m 16384 -c 4096"
)
```

**Execution:**
```bash
$ ./df_bench_runner.sh run
Starting Benchmark Matrix (144 total runs)
Saving artifacts to: results/benchmark_20260301_220000

--- [Run 1/144] valkey-7.2 | Loc: remote | Env: raw | Ratio: 1:0 | Pipe: 1 ---
...
```

### The Morning After: Generating the Report

When you wake up 12 hours later, you don't have to dig through 144 text files. You just run one command, and the orchestrator parses every single `memtier.log`, extracts the exact percentiles, and generates a unified CSV.

**Execution:**
```bash
$ ./df_bench_runner.sh report
```

**Simulated Output:**
```text
=========================================== BENCHMARK RESULTS ===========================================
Protocol   Engine         Ratio  Pipeline  Ops_sec     KB_sec     Avg_Latency  p50_Latency  p99_Latency  p99.9_Latency
RESP       valkey-7.2     1:0    1         124500.21   10450.45   0.510        0.480        1.200        2.100
RESP       dragonfly-v1   1:0    1         285400.10   24010.88   0.220        0.200        0.850        1.400
RESP       dragonfly-v2   1:0    1         410200.55   34500.12   0.150        0.140        0.600        1.100
Memcached  memcached-1.6  1:0    1         115300.00   9800.00    0.550        0.510        1.300        2.400
...
RESP       valkey-7.2     1:0    30        850400.00   71000.20   2.200        2.100        4.500        8.100
RESP       dragonfly-v1   1:0    30        1950000.00  162000.00  0.980        0.900        2.100        4.000
RESP       dragonfly-v2   1:0    30        3200500.00  268000.00  0.590        0.550        1.400        2.800
...
=========================================================================================================

Report saved: results/benchmark_20260301_220000/summary.csv (144 runs)
```

Now you have a clean `summary.csv` ready to be imported directly into Excel, Google Sheets, or Jupyter Notebooks to generate your graphs. No copy-pasting required.
