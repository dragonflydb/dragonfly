#!/bin/bash
set -eo pipefail

usage() {
    cat <<EOF
Usage: ./bench_v2.sh [binary] [mode] [runs] [tag] [ver] [threads] [server_log_dir]
  binary:       Path to the server binary to launch. Works for all SERVER_TYPE values.
                Must be an existing regular file, symlink (hard or soft), or a bare
                name resolvable via 'which'. The resolved path must exist as a file.
                Accepted forms:
                  ./build-opt/dragonfly          (relative path)
                  /usr/local/bin/valkey-server   (absolute path)
                  ~/bin/my-server                (tilde-expanded path)
                  valkey-server                  (bare name: looked up via 'which')
                The script exits immediately if the binary cannot be found or is not
                a file/symlink. This is intentional: benchmarks must be reproducible
                and you must know exactly what binary you are running.
                Default: ./build-opt/dragonfly
  mode:         multi_conn | single_conn | pubsub | all (default: all)
  runs:         how many times to repeat the full benchmark (default: 3).
                Must be odd (1, 3, 5, ...) for reliable averaging.
                If runs > 1, a final average report is printed at the end.
  tag:          optional label for the log file.
                If given, log file is: {tag}_{HHMMSS}.log
                If omitted:             bench_v2_{mode}_{sha}_{YYYYMMDD_HHMMSS}.log
  ver:          which version(s) to run: v1 | v2 | both (default: both)
  threads:      number of server threads (proactor threads for dragonfly/ok_backend).
                For SERVER_TYPE=valkey, use VALKEY_IO_THREADS instead; this arg is ignored.
                Default: nproc-1 in local mode (leaves 1 core for memtier),
                         nproc   in remote mode (CLIENT_HOST is set, client is elsewhere).
  server_log_dir: optional directory to save the server log.
                If given, the log is copied there on exit as server_{tag_or_sha}_{TIMESTAMP}.log.
                If omitted, the log is discarded.

Environment variables (for cross-machine benchmarking):
  CLIENT_HOST:  SSH host of the client machine that runs memtier/redis-benchmark.
                If unset, client tools run locally (single-machine mode).
  SERVER_HOST:  IP the client uses to reach this server (default: 127.0.0.1).
                Set to the server's private IP when CLIENT_HOST is set.
  SSH_USER:     SSH username for CLIENT_HOST (default: current user).
  CLIENT_DELAY_US: Artificial one-way delay in microseconds on loopback (local mode only).
                Simulates real-network RTT gaps. Requires 'sudo' and 'tc'.
                Example: CLIENT_DELAY_US=100 simulates 0.2ms RTT.
                Default: 0 (no delay). Set to 50-200 to mimic cross-machine.
  PIPELINE:     Comma-separated list of pipeline depths to run. Default: all (1,10,100).
                Example: PIPELINE=10 or PIPELINE=1,100
  CMD:          Command to benchmark: set | zadd (default: set).
                SET uses the async dispatch path; ZADD uses the synchronous path
                (the connection blocks until the shard completes each command).
                Both multi_conn and single_conn modes respect this variable.
  EXTRA_SERVER_FLAGS: Additional flags passed verbatim to the server binary on every
                invocation. Works for all SERVER_TYPE values.
                For dragonfly/ok_backend: appended after all script-managed flags.
                For valkey: appended after all script-managed flags.
                Example (dragonfly): EXTRA_SERVER_FLAGS="--uring_recv_buffer_cnt=16000"
                Example (valkey):    EXTRA_SERVER_FLAGS="--maxmemory 4gb"
  MEMTIER_ARGS: Arguments for memtier_benchmark (multi_conn and single_conn modes).
                When set, completely replaces the default arguments. Do NOT include the
                binary name 'memtier_benchmark'. The script always prepends:
                  memtier_benchmark -s <host> -p <port> --pipeline=<depth>
                Must NOT include: -s/--server, -p/--port, --pipeline  (script exits if found).
                Batch density is computed only if -n <N> is present in the args.
                Example: MEMTIER_ARGS="-t 1 -c 1 -n 2000000 --ratio=1:0 -d 2 --hide-histogram"
  REDIS_CLI_ARGS: Extra arguments for redis-benchmark in pubsub mode (publisher only).
                Replaces the default '-c 5 -q' options. The script always injects:
                  redis-benchmark -p <port> -n <count> -P <pipeline>
                Must NOT include: -h/--host, -p/--port, -P/--pipeline, -n/--requests  (script exits if found).
                Example: REDIS_CLI_ARGS="-c 10 --tls"
  CLIENT_TIMEOUT: Hard timeout in seconds for each client invocation (memtier/redis-benchmark).
                When set, wraps the client call with 'timeout <N>'. Protects against hangs
                when the server crashes mid-benchmark. Unset = no timeout (default).
                Example: CLIENT_TIMEOUT=300
  BENCH_DURATION: Duration in seconds for each memtier run (default: 15).
                Applied as --test-time=N. Use longer values for more stable averages;
                shorter values for faster iteration. Time-based runs give consistent
                wall-clock duration regardless of pipeline depth or server speed.
                Has no effect when MEMTIER_ARGS is set (include --test-time in MEMTIER_ARGS).
                Example: BENCH_DURATION=30
  NUM_RUNS:     Number of times to repeat the full benchmark (default: 3).
                Overrides the [runs] positional argument when set. Useful when driving
                bench_v2.sh from a wrapper script (e.g. run_stage1.sh) so all rows
                can be adjusted from a single env var without touching positional args.
                Example: NUM_RUNS=1 BENCH_DURATION=2 ./bench_v2.sh ...  (quick smoke test)
  SERVER_TYPE:  Which server to launch. Default: dragonfly.
                Supported values:
                  dragonfly  - launch [binary] with all Dragonfly flags
                               (--proactor_threads, --enable_resp_io_loop_v2, --admin_port,
                               --dbfilename). /metrics scraping is enabled.
                               ver=v1/v2/both controls which loop version(s) are benchmarked.
                  valkey     - launch [binary] as valkey-server.
                               Dragonfly-specific flags are not passed.
                               /metrics scraping is disabled; SEND_SYSCALLS and
                               BATCH_DENSITY will show 0.
                               ver argument is accepted but ignored (only one run per mode).
                               Pubsub mode is supported (standard Redis pub/sub protocol).
                Additional env vars for SERVER_TYPE=valkey:
                  VALKEY_IO_THREADS: io-threads to pass to valkey-server (default: 1).
                Example: SERVER_TYPE=valkey VALKEY_IO_THREADS=4 \
                           ./bench_v2.sh /usr/local/bin/valkey-server multi_conn 3 vk_4t
                Example: SERVER_TYPE=valkey ./bench_v2.sh valkey-server multi_conn 3 vk_1t

Modes:
  multi_conn     - 50 clients, heavy saturation (command set by CMD env var).
  single_conn    - 1 client, isolates per-connection behavior (command set by CMD).
  pubsub         - 10 subscribers, PUBLISH fan-out.
  all            - Run all modes sequentially.

Examples:
  # Local (single machine):
  ./bench_v2.sh ./build-opt/dragonfly pubsub 5 my_tag v2

  # Cross-machine (run this ON THE SERVER):
  SERVER_HOST=172.31.30.209 CLIENT_HOST=172.31.20.3 ./bench_v2.sh ./build-opt/dragonfly all 3

  # Run only pipeline=10 cross-machine, V2 only:
  PIPELINE=10 SERVER_HOST=172.31.30.209 CLIENT_HOST=172.31.20.3 ./bench_v2.sh ./build-opt/dragonfly multi_conn 1 mytag v2

  # Benchmark ZADD (sync command path) with 50 connections:
  CMD=zadd ./bench_v2.sh ./build-opt/dragonfly multi_conn 1 mytag both

  # Run pipeline=10 and pipeline=100 only:
  PIPELINE=10,100 ./bench_v2.sh ./build-opt/dragonfly multi_conn 1 mytag both

  # Benchmark Valkey (pass valkey-server binary as first arg):
  SERVER_TYPE=valkey ./bench_v2.sh ./valkey-server single_conn 3 valkey_1t
  SERVER_TYPE=valkey ./bench_v2.sh ~/bin/valkey-server single_conn 3 valkey_1t
  SERVER_HOST=172.31.30.209 CLIENT_HOST=172.31.20.3 SERVER_TYPE=valkey \\
    ./bench_v2.sh /usr/local/bin/valkey-server multi_conn 3 valkey_cross

  # Benchmark with multishot recv enabled (V2 only):
  EXTRA_SERVER_FLAGS="--uring_recv_buffer_cnt=16000" \\
    ./bench_v2.sh ./build-opt/dragonfly multi_conn 3 dfly_multishot v2

  # Benchmark ok_backend (same flags as dragonfly/ok_backend):
  ./bench_v2.sh ./build-opt/ok_backend multi_conn 3 ok_backend_4t both

  # Quick smoke test — 1 run, 2 seconds per pipeline depth:
  NUM_RUNS=1 BENCH_DURATION=2 ./bench_v2.sh ./build-opt/dragonfly multi_conn _ my_tag both 4
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" || $# -eq 0 ]]; then
    usage
    exit 0
fi

SERVER_BIN=${1:-"./build-opt/dragonfly"}
MODE=${2:-"all"}
RUNS=${NUM_RUNS:-${3:-3}}
TAG=${4:-""}
VER=${5:-"both"}

# Validate positional arguments.
# Binary validation rules:
#   1. Tilde-expand the path.
#   2. If it contains a / (relative or absolute path): must be an existing file or symlink.
#   3. If it is a bare name (no /): resolve via 'which', then verify the result is a file.
# We never silently fall back to a global PATH binary for a path-like argument.
_resolve_binary() {
    local bin="$1"
    # Tilde expansion (bash doesn't expand ~ inside variables automatically)
    if [[ "$bin" == "~"* ]]; then
        bin="${HOME}${bin:1}"
    fi
    if [[ "$bin" == */* ]]; then
        # Path-like (contains /): must exist as a regular file or symlink right now.
        if [[ -f "$bin" || -L "$bin" ]]; then
            echo "$bin"; return 0
        fi
        return 1
    else
        # Bare name: resolve via 'which', then verify it's actually a file.
        local resolved
        resolved=$(command -v "$bin" 2>/dev/null) || return 1
        if [[ -f "$resolved" || -L "$resolved" ]]; then
            echo "$resolved"; return 0
        fi
        return 1
    fi
}
if ! _resolved=$(_resolve_binary "$SERVER_BIN"); then
    echo "[!] Error: binary '$SERVER_BIN' not found or not a regular file/symlink."
    echo "    Use a relative path (./build-opt/dragonfly), absolute path (/usr/bin/valkey-server),"
    echo "    tilde path (~/bin/server), or a bare name that resolves via 'which'."
    exit 1
fi
SERVER_BIN="$_resolved"
case "$MODE" in
    multi_conn|single_conn|pubsub|all) ;;
    *) echo "[!] Error: mode must be one of: multi_conn | single_conn | pubsub | all. Got: '$MODE'"; exit 1 ;;
esac
if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
    echo "[!] Error: runs must be a positive odd integer. Got: '$RUNS'"
    exit 1
fi
if [[ "$RUNS" -gt 1 ]] && (( RUNS % 2 == 0 )); then
    echo "[!] Error: runs must be odd (for reliable median). Got: '$RUNS'. Use 1, 3, 5, etc."
    exit 1
fi
case "$VER" in
    v1|v2|both) ;;
    *) echo "[!] Error: ver must be one of: v1 | v2 | both. Got: '$VER'"; exit 1 ;;
esac
# Arg 6 can be either threads (a number) or server_log_dir (a path starting with / or .).
# If it looks like a path, treat it as SERVER_LOG_DIR and use default threads.
# Default threads: all CPUs in remote mode (client is elsewhere), all-minus-one in local mode.
_default_threads() {
    local n; n=$(nproc)
    if [[ -n "${CLIENT_HOST:-}" ]]; then
        echo "$n"          # remote mode — server gets all cores
    else
        echo "$(( n > 1 ? n - 1 : 1 ))"  # local mode — leave 1 core for memtier
    fi
}
if [[ "${6:-}" =~ ^[0-9]+$ ]]; then
    PROACTOR_THREADS=${6}
    SERVER_LOG_DIR=${7:-""}
elif [[ "${6:-}" == /* || "${6:-}" == ./* || "${6:-}" == . ]]; then
    PROACTOR_THREADS=$(_default_threads)
    SERVER_LOG_DIR=${6}
else
    PROACTOR_THREADS=${6:-$(_default_threads)}
    SERVER_LOG_DIR=${7:-""}
fi
PORT=6379
ADMIN_PORT=8099
SERVER_HOST=${SERVER_HOST:-"127.0.0.1"}
# Server type: which server process to launch.
# 'dragonfly' (default) or 'valkey'; [binary] arg is the executable for both.
SERVER_TYPE=${SERVER_TYPE:-dragonfly}
VALKEY_IO_THREADS=${VALKEY_IO_THREADS:-1}
CLIENT_HOST=${CLIENT_HOST:-""}
SSH_USER=${SSH_USER:-"$(whoami)"}
CLIENT_DELAY_US=${CLIENT_DELAY_US:-0}
PIPELINE_FILTER=${PIPELINE:-""}
CMD=${CMD:-"set"}
BENCH_DURATION=${BENCH_DURATION:-15}
# Metrics endpoint port. Dragonfly/ok_backend exposes /metrics on ADMIN_PORT.
# ok_backend has no separate HTTP admin port — metrics are on the main PORT instead.
# Auto-detected by binary basename; can be overridden with SERVER_METRICS_PORT env var.
if [[ -z "${SERVER_METRICS_PORT:-}" ]]; then
    _bin_base=$(basename "${SERVER_BIN}")
    if [[ "$_bin_base" == "ok_backend" ]]; then
        SERVER_METRICS_PORT=$PORT
    else
        SERVER_METRICS_PORT=$ADMIN_PORT
    fi
fi
if [[ "$CMD" != "set" && "$CMD" != "zadd" ]]; then
    echo "[!] Error: CMD must be 'set' or 'zadd', got '$CMD'"
    exit 1
fi

# Validate env-provided args don't contain flags the script injects itself.
_validate_no_injected_flags() {
    local varname="$1"; shift
    local value="${!varname:-}"
    [[ -z "$value" ]] && return
    local tok flag
    local -a tokens
    read -ra tokens <<< "$value"
    for tok in "${tokens[@]}"; do
        for flag in "$@"; do
            if [[ "$tok" == "$flag" || "$tok" == "${flag}=" || "$tok" == "${flag}=*" ]]; then
                echo "[!] Error: ${varname} must not contain '${flag}' (injected by the script)."
                echo "    Got: ${varname}=\"${value}\""
                exit 1
            fi
        done
    done
}
_validate_no_injected_flags MEMTIER_ARGS  -s --server -p --port --pipeline
_validate_no_injected_flags REDIS_CLI_ARGS -h --host   -p --port -P --pipeline -n --requests
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [[ -n "$TAG" ]]; then
    LOG_FILE="${TAG}_$(date +%H%M%S).log"
else
    LOG_FILE="bench_v2_${MODE}_${GIT_SHA}_${TIMESTAMP}.log"
fi
ACCUM_FILE=$(mktemp)
SERVER_LOG=$(mktemp)

if [[ -n "$SERVER_LOG_DIR" && ! -d "$SERVER_LOG_DIR" ]]; then
    echo "[!] Error: server_log_dir '${SERVER_LOG_DIR}' does not exist. Create it first."
    exit 1
fi

# Cross-machine mode detection.
REMOTE_MODE=""
if [[ -n "$CLIENT_HOST" ]]; then
    REMOTE_MODE=1
    if [[ "$SERVER_HOST" == "127.0.0.1" ]]; then
        echo "[!] Error: CLIENT_HOST is set but SERVER_HOST is 127.0.0.1."
        echo "    The client can't reach the server. Set SERVER_HOST to this machine's IP."
        exit 1
    fi
    if ! ssh -o ConnectTimeout=5 -o BatchMode=yes -o ControlMaster=no \
            "${SSH_USER}@${CLIENT_HOST}" true 2>/dev/null; then
        echo "[!] Error: Cannot SSH to ${SSH_USER}@${CLIENT_HOST} (BatchMode, no password prompt)."
        echo "    Fix: ssh-copy-id ${SSH_USER}@${CLIENT_HOST}"
        exit 1
    fi
    echo "[*] Cross-machine mode: server=$(hostname), client=${SSH_USER}@${CLIENT_HOST}"
    echo "    Client will target ${SERVER_HOST}:${PORT}"
fi

ulimit -n 4096 2>/dev/null || true

# Loopback delay simulation (local mode only).
NETEM_ACTIVE=""
setup_netem() {
    if [[ -z "$REMOTE_MODE" && "$CLIENT_DELAY_US" -gt 0 ]]; then
        echo "[*] Adding ${CLIENT_DELAY_US}us one-way delay on lo (RTT=${CLIENT_DELAY_US}x2 us)"
        sudo tc qdisc add dev lo root netem delay "${CLIENT_DELAY_US}us" 2>/dev/null || {
            echo "[!] WARNING: tc netem failed. Run with sudo or install iproute2."
            return
        }
        NETEM_ACTIVE=1
    fi
}
teardown_netem() {
    if [[ -n "$NETEM_ACTIVE" ]]; then
        sudo tc qdisc del dev lo root 2>/dev/null || true
        NETEM_ACTIVE=""
    fi
}
setup_netem

# ---------------------------------------------------------------------------
# filter_pipelines <p1> <p2> ...
# Returns the subset of given pipeline depths that match PIPELINE_FILTER.
# If PIPELINE_FILTER is empty, returns all of them unchanged.
# ---------------------------------------------------------------------------
filter_pipelines() {
    # PIPELINE or PIPELINE_FILTER env var can override which depths run.
    local filter="${PIPELINE_FILTER:-${PIPELINE:-}}"
    if [[ -z "$filter" ]]; then
        echo "$@"
        return
    fi
    local result=()
    for p in "$@"; do
        if [[ ",$filter," == *",$p,"* ]]; then
            result+=("$p")
        fi
    done
    if [[ ${#result[@]} -eq 0 ]]; then
        echo "[!] Warning: PIPELINE=$filter matched no depths in ($*). Running all." >&2
        echo "$@"
    else
        echo "${result[@]}"
    fi
}

# Global PID tracking for cleanup.
SERVER_PID=""
SUB_PIDS=()

cleanup() {
    local exit_code=$?
    echo ""
    echo "[*] Cleaning up (exit_code=$exit_code)..."

    teardown_netem

    for pid in "${SUB_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    SUB_PIDS=()

    # Kill remote client tools if in cross-machine mode.
    if [[ -n "$REMOTE_MODE" ]]; then
            ssh -n -o ConnectTimeout=3 -o ControlMaster=no \
                "${SSH_USER}@${CLIENT_HOST}" \
                "pkill -9 redis-cli; pkill -9 redis-benchmark; pkill -9 memtier_benchmark" 2>/dev/null || true
    fi

    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi

    # Crash fallback: save log if server was still running when we exited.
    if [[ -n "$SERVER_LOG_DIR" && -s "$SERVER_LOG" ]]; then
        local save_name="server_${TAG:-${GIT_SHA}}_$(date +%Y%m%d_%H%M%S)_crash.log"
        cp "$SERVER_LOG" "${SERVER_LOG_DIR}/${save_name}"
        echo "[*] Server crash log saved to ${SERVER_LOG_DIR}/${save_name}"
    fi
    rm -f "$ACCUM_FILE" "$SERVER_LOG"
    echo "[*] Done."
}
trap cleanup EXIT INT TERM

NUM_CPUS=$(nproc)
MIN_CPUS=2  # 1 for server + 1 for client (local mode).

if [[ $NUM_CPUS -lt $MIN_CPUS ]]; then
    echo "[!] Error: Need at least $MIN_CPUS CPUs, this machine has $NUM_CPUS."
    exit 1
fi

if [[ -z "$REMOTE_MODE" ]]; then
    # How many CPU cores does the server process actually use?
    # Valkey: VALKEY_IO_THREADS. Dragonfly/ok_backend: PROACTOR_THREADS.
    if [[ "$SERVER_TYPE" == "valkey" ]]; then
        _server_thread_count=$VALKEY_IO_THREADS
    else
        _server_thread_count=$PROACTOR_THREADS
    fi

    if [[ $_server_thread_count -ge $NUM_CPUS ]]; then
        echo "[!] Error: server threads=$_server_thread_count >= available CPUs (${NUM_CPUS})."
        echo "    Leave at least 1 CPU for memtier. Max allowed: $((NUM_CPUS - 1))."
        exit 1
    fi

    SERVER_CPUS=$(seq -s ',' 0 $((_server_thread_count - 1)))
    CLIENT_CORES_START=$_server_thread_count
    CLIENT_CORES_END=$((_server_thread_count + 1))
    [[ $CLIENT_CORES_END -ge $NUM_CPUS ]] && CLIENT_CORES_END=$((NUM_CPUS - 1))
    CLIENT_CPUS=$(seq -s ',' ${CLIENT_CORES_START} ${CLIENT_CORES_END})

    SERVER_TASKSET="taskset -c ${SERVER_CPUS}"
    CLIENT_TASKSET="taskset -c ${CLIENT_CPUS}"
else
    # Remote mode: server uses all local cores, client runs on remote machine.
    SERVER_TASKSET=""
    CLIENT_TASKSET=""
    REMOTE_CPUS=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${CLIENT_HOST}" nproc 2>/dev/null || echo 0)
    if [[ $REMOTE_CPUS -lt $MIN_CPUS ]]; then
        echo "[!] Error: Client machine $CLIENT_HOST has $REMOTE_CPUS CPUs, need at least $MIN_CPUS."
        exit 1
    fi
    echo "[*] Server CPUs: $NUM_CPUS, Client CPUs: $REMOTE_CPUS"
fi

# Preflight: verify required client tools exist on the machine that will run them.
check_client_tools() {
    local missing=()
    if [[ -n "$REMOTE_MODE" ]]; then
        ssh -n "${SSH_USER}@${CLIENT_HOST}" "command -v memtier_benchmark" > /dev/null 2>&1 || missing+=("memtier_benchmark")
        ssh -n "${SSH_USER}@${CLIENT_HOST}" "command -v redis-benchmark"   > /dev/null 2>&1 || missing+=("redis-benchmark")
        ssh -n "${SSH_USER}@${CLIENT_HOST}" "command -v redis-cli"         > /dev/null 2>&1 || missing+=("redis-cli")
    else
        command -v memtier_benchmark > /dev/null 2>&1 || missing+=("memtier_benchmark")
        command -v redis-benchmark   > /dev/null 2>&1 || missing+=("redis-benchmark")
        command -v redis-cli         > /dev/null 2>&1 || missing+=("redis-cli")
    fi
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "[!] Error: Missing tools on client: ${missing[*]}"
        echo "    redis-benchmark / redis-cli: sudo apt-get install -y redis-tools"
        echo "    memtier_benchmark: must be built from source:"
        echo "      sudo apt-get install -y build-essential autoconf automake libpcre3-dev libevent-dev libssl-dev zlib1g-dev git"
        echo "      git clone https://github.com/RedisLabs/memtier_benchmark.git && cd memtier_benchmark"
        echo "      autoreconf -ivf && ./configure && make && sudo make install"
        exit 1
    fi
}
check_client_tools

# ---------------------------------------------------------------------------
# run_on_client <cmd...>
# Executes a command on the client. Local mode: runs with CLIENT_TASKSET.
# Remote mode: runs via SSH on CLIENT_HOST targeting SERVER_HOST.
# ---------------------------------------------------------------------------
run_on_client() {
    local _timeout_cmd=""
    if [[ -n "${CLIENT_TIMEOUT:-}" ]]; then
        _timeout_cmd="timeout $CLIENT_TIMEOUT"
    fi
    if [[ -n "$REMOTE_MODE" ]]; then
        $_timeout_cmd ssh -n -o ControlMaster=no -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
            "${SSH_USER}@${CLIENT_HOST}" "$(printf '%q ' "$@")"
    else
        $_timeout_cmd $CLIENT_TASKSET "$@"
    fi
}

# Target host for client tools: SERVER_HOST in remote mode, 127.0.0.1 locally.
client_target() {
    if [[ -n "$REMOTE_MODE" ]]; then
        echo "$SERVER_HOST"
    else
        echo "127.0.0.1"
    fi
}

wait_for_server() {
    local timeout=${1:-10}
    local elapsed=0
    while ! redis-cli -p $PORT ping > /dev/null 2>&1; do
        if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "[!] ${SERVER_TYPE} (PID $SERVER_PID) died during startup."
            SERVER_PID=""
            return 1
        fi
        sleep 0.2
        elapsed=$(( elapsed + 1 ))
        if [[ $elapsed -ge $(( timeout * 5 )) ]]; then
            echo "[!] Error: ${SERVER_TYPE} did not become ready within ${timeout}s."
            return 1
        fi
    done
}

get_send_count() {
    # Non-dragonfly servers have no /metrics endpoint; always return 0.
    if [[ "$SERVER_TYPE" != "dragonfly" ]]; then echo 0; return; fi
    local raw val metric_name
    raw=$(curl -s --max-time 2 "http://127.0.0.1:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    # ok_backend exposes 'reply_total'; dragonfly exposes 'dragonfly_reply_total'.
    local _bname; _bname=$(basename "${SERVER_BIN}")
    if [[ "$_bname" == "ok_backend" ]]; then
        metric_name='reply_total'
    else
        metric_name='dragonfly_reply_total'
    fi
    val=$(echo "$raw" | tr -d '\r' | grep "^${metric_name}" | awk '{sum += $NF} END {print (sum ? sum : 0)}') || true
    echo "${val:-0}"
}

get_cmd_count() {
    # Returns total commands processed. Used to compute batch density accurately
    # without relying on -n counts. Works for both dragonfly and ok_backend
    # (ok_backend exposes commands_processed_total on its main port).
    if [[ "$SERVER_TYPE" != "dragonfly" ]]; then echo 0; return; fi
    local raw val
    raw=$(curl -s --max-time 2 "http://127.0.0.1:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    val=$(echo "$raw" | tr -d '\r' | grep '^commands_processed_total' | awk '{sum += $NF} END {print (sum ? sum : 0)}') || true
    echo "${val:-0}"
}

get_replies_per_flush_raw() {
    # Returns "sum count" from the histogram for delta computation.
    local raw sum count
    raw=$(curl -s --max-time 2 "http://127.0.0.1:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    sum=$(echo "$raw" | tr -d '\r' | grep '^dragonfly_replies_per_flush_sum' | awk '{print $NF}') || true
    count=$(echo "$raw" | tr -d '\r' | grep '^dragonfly_replies_per_flush_count' | awk '{print $NF}') || true
    echo "${sum:-0} ${count:-0}"
}

compute_batch_density() {
    # Given total replies and syscall delta, compute real batch density.
    local total_replies=$1 send_delta=$2
    if [[ "$send_delta" -gt 0 ]]; then
        awk "BEGIN {printf \"%.1f\", $total_replies / $send_delta}"
    else
        echo "0"
    fi
}

_metrics_verified=0
verify_metrics_endpoint() {
    # Only dragonfly/ok_backend exposes /metrics; skip silently for other server types.
    [[ "$SERVER_TYPE" != "dragonfly" ]] && return
    [[ $_metrics_verified -eq 1 ]] && return
    _metrics_verified=1

    redis-cli -p $PORT SET __bench_probe__ 1 > /dev/null 2>&1 || true
    redis-cli -p $PORT GET __bench_probe__ > /dev/null 2>&1 || true

    local raw sample
    raw=$(curl -s --max-time 2 "http://127.0.0.1:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    # ok_backend uses 'reply_total'; dragonfly uses 'dragonfly_reply_total'.
    local _bname; _bname=$(basename "${SERVER_BIN}")
    if [[ "$_bname" == "ok_backend" ]]; then
        sample=$(echo "$raw" | tr -d '\r' | grep '^reply_total') || true
    else
        sample=$(echo "$raw" | tr -d '\r' | grep '^dragonfly_reply_total') || true
    fi

    if [[ -z "$raw" ]]; then
        echo "[!] WARNING: /metrics endpoint unreachable. SEND_SYSCALLS will show 0."
    elif [[ -z "$sample" ]]; then
        echo "[!] WARNING: /metrics reachable but expected metric not found (binary: ${_bname})."
    fi
}

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        local i=0
        while kill -0 "$SERVER_PID" 2>/dev/null && (( i < 40 )); do
            sleep 0.15; i=$(( i + 1 ))
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "[!] ${SERVER_TYPE} didn't exit after SIGTERM, sending SIGKILL"
            kill -9 "$SERVER_PID" 2>/dev/null || true
            sleep 0.4
        fi
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
    if [[ -n "$SERVER_LOG_DIR" && -s "$SERVER_LOG" ]]; then
        local save_name="server_${TAG:-${GIT_SHA}}_$(date +%Y%m%d_%H%M%S).log"
        cp "$SERVER_LOG" "${SERVER_LOG_DIR}/${save_name}"
        echo "[*] Server log saved to ${SERVER_LOG_DIR}/${save_name}"
        : > "$SERVER_LOG"  # Truncate so cleanup doesn't duplicate.
    fi
    sleep 0.6  # let OS release the TCP port
}

start_server() {
    local v2_flag=$1; shift
    local label=$1; shift
    local mode_name=$1; shift
    local extra_flags=("$@")

    # -----------------------------------------------------------------------
    # Valkey mode.
    # -----------------------------------------------------------------------
    if [[ "$SERVER_TYPE" == "valkey" ]]; then
        # For valkey, SERVER_BIN is the valkey-server binary (passed as [binary] arg).
        local _stale_bin
        _stale_bin=$(basename "$SERVER_BIN")
        local _stale_pids
        _stale_pids=$(pgrep -x "$_stale_bin" 2>/dev/null || true)
        if [[ -n "$_stale_pids" ]]; then
            echo "[!] Error: Found existing ${_stale_bin} process(es): PIDs $_stale_pids"
            echo "    Kill them first: kill $_stale_pids"
            exit 1
        fi
        echo ""
        echo ">>> Starting Valkey [${label}] mode=${mode_name} (binary=${SERVER_BIN}, io-threads=${VALKEY_IO_THREADS})"
        local vk_cmd=($SERVER_TASKSET "$SERVER_BIN"
            --port $PORT
            --bind 0.0.0.0
            --io-threads "$VALKEY_IO_THREADS"
            --io-threads-do-reads yes
            --protected-mode no
            --save ""
            --appendonly no
            --daemonize no
            --loglevel warning)
        # Append generic extra flags for valkey too
        if [[ -n "${EXTRA_SERVER_FLAGS:-}" ]]; then
            read -ra _vk_extra <<< "$EXTRA_SERVER_FLAGS"
            vk_cmd+=("${_vk_extra[@]}")
        fi
        echo "  [server cmd] ${vk_cmd[*]}"
        "${vk_cmd[@]}" > "$SERVER_LOG" 2>&1 &
        SERVER_PID=$!
        if ! wait_for_server 10; then
            echo "[!] Valkey log tail:"; tail -20 "$SERVER_LOG"
            exit 1
        fi
        redis-cli -p $PORT FLUSHALL > /dev/null 2>&1 || true
        return
    fi

    # -----------------------------------------------------------------------
    # Dragonfly / ok_backend mode (default).
    # -----------------------------------------------------------------------
    # Ensure no stale server process is running (from a previous crashed/interrupted run).
    local _stale_bin
    _stale_bin=$(basename "$SERVER_BIN")
    local _stale_pids
    _stale_pids=$(pgrep -x "$_stale_bin" 2>/dev/null || true)
    if [[ -n "$_stale_pids" ]]; then
        echo "[!] Error: Found existing ${_stale_bin} process(es): PIDs $_stale_pids"
        echo "    Kill them first: kill $_stale_pids"
        exit 1
    fi

    # Append env-provided flags (works for all server types)
    if [[ -n "${EXTRA_SERVER_FLAGS:-}" ]]; then
        read -ra _env_flags <<< "$EXTRA_SERVER_FLAGS"
        extra_flags+=("${_env_flags[@]}")
    fi

    local log_flags=()
    if [[ -n "$SERVER_LOG_DIR" ]]; then
        log_flags=(--alsologtostderr --minloglevel=0)
    fi

    echo ""
    echo ">>> Starting ${SERVER_BIN} [${label}] mode=${mode_name} (io_loop_v2=${v2_flag})"
    local server_cmd=($SERVER_TASKSET "$SERVER_BIN"
        --proactor_threads="${PROACTOR_THREADS}"
        --enable_resp_io_loop_v2="${v2_flag}"
        --bind=0.0.0.0
        --port=$PORT
        # --minloglevel=0 --vmodule=dragonfly_connection=1  (uncomment to enable verbose logging)
        --admin_port=$ADMIN_PORT
        --dbfilename ""
        "${log_flags[@]}"
        "${extra_flags[@]}")
    echo "  [server cmd] ${server_cmd[*]}"
    "${server_cmd[@]}" > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!

    if ! wait_for_server 10; then
        echo "[!] Server log tail:"; tail -20 "$SERVER_LOG"
        exit 1
    fi
    # Flush any data loaded from a saved RDB (e.g. leftover keys from a previous run
    # with a different command type — SET keys would cause WRONGTYPE on ZADD).
    redis-cli -p $PORT FLUSHALL > /dev/null 2>&1 || true
    verify_metrics_endpoint
}

# ---------------------------------------------------------------------------
# run_bench <v2_flag> <label> <threads> <clients> <data_size> <mode_name>
#           <pipelines...> [-- <extra_server_flags...>]
# ---------------------------------------------------------------------------
run_bench() {
    local v2_flag=$1; shift
    local label=$1; shift
    local threads=$1; shift
    local clients=$1; shift
    local data_size=$1; shift
    local mode_name=$1; shift
    local pipelines=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do
        pipelines+=("$1"); shift
    done
    [[ "${1:-}" == "--" ]] && shift
    local extra_server_flags=("$@")

    start_server "$v2_flag" "$label" "$mode_name" "${extra_server_flags[@]}"

    local target
    target=$(client_target)

    RESULTS_TMP=$(mktemp)
    echo -e "PIPELINE\tRPS\tAVG_LAT(ms)\tP50(ms)\tP99(ms)\tP99.9(ms)\tSEND_SYSCALLS\tBATCH_DENSITY" > "$RESULTS_TMP"

    for PIPELINE in "${pipelines[@]}"; do
        echo "  [+] pipeline=$PIPELINE ..."

        SENDS_BEFORE=$(get_send_count)
        CMDS_BEFORE=$(get_cmd_count)

        local memtier_cmd
        if [[ -n "${MEMTIER_ARGS:-}" ]]; then
            local -a _extra_args
            read -ra _extra_args <<< "$MEMTIER_ARGS"
            memtier_cmd=(memtier_benchmark -s "$target" -p $PORT --pipeline="$PIPELINE" "${_extra_args[@]}")
        else
            memtier_cmd=(memtier_benchmark
                -s "$target"
                -p $PORT
                -t "$threads"
                -c "$clients"
                --pipeline="$PIPELINE"
                --ratio=1:0
                -d "$data_size"
                --key-pattern=R:R
                --key-prefix=bench
                --test-time=$BENCH_DURATION
                --hide-histogram)
        fi
        if [[ -n "$REMOTE_MODE" ]]; then
            echo "  [client cmd] ssh ${SSH_USER}@${CLIENT_HOST} ${memtier_cmd[*]}"
        else
            echo "  [client cmd] $CLIENT_TASKSET ${memtier_cmd[*]}"
        fi

        local OUTPUT
        OUTPUT=$(run_on_client "${memtier_cmd[@]}" 2>&1) || true
        echo "  --- raw memtier output ---"
        echo "$OUTPUT"
        echo "  --- end raw output ---"

        SENDS_AFTER=$(get_send_count)
        CMDS_AFTER=$(get_cmd_count)
        SEND_DELTA=$(( SENDS_AFTER - SENDS_BEFORE ))
        CMD_DELTA=$(( CMDS_AFTER - CMDS_BEFORE ))
        BATCH_DENSITY=$(compute_batch_density "$CMD_DELTA" "$SEND_DELTA")

        RPS=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $2}' || true)
        LATENCY=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $5}' || true)
        # memtier Totals line columns (--hide-histogram):
        #   $1=Type $2=Ops/sec $3=Hits $4=Misses $5=Avg.Lat $6=p50 $7=p99 $8=p99.9 $9=KB/sec
        P50=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $6}' || true)
        P99=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $7}' || true)
        P999=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $8}' || true)
        if [[ -z "$RPS" ]]; then echo "[!] WARNING: could not parse RPS from memtier output"; fi
        RPS=${RPS:-"Error"}
        LATENCY=${LATENCY:-"N/A"}
        P50=${P50:-"N/A"}
        P99=${P99:-"N/A"}
        P999=${P999:-"N/A"}

        echo -e "$PIPELINE\t$RPS\t$LATENCY\t$P50\t$P99\t$P999\t$SEND_DELTA\t$BATCH_DENSITY" >> "$RESULTS_TMP"
        echo "${label}|${mode_name}|${PIPELINE}|${RPS}|${LATENCY}|${SEND_DELTA}|${BATCH_DENSITY}|${P50}|${P99}|${P999}" >> "$ACCUM_FILE"
    done

    stop_server

    echo ""
    echo "====================================================="
    printf "  %-4s  %s  (commit: %s)\n" "$label" "$mode_name" "$GIT_SHA"
    echo "====================================================="
    column -t -s $'\t' "$RESULTS_TMP"
    echo "====================================================="
    rm "$RESULTS_TMP"
}

# ---------------------------------------------------------------------------
# run_bench_custom <v2_flag> <label> <threads> <clients> <mode_name>
#                  <pipelines...> [-- <memtier_extra_args...>] [-- <extra_server_flags...>]
# For custom memtier commands (e.g., ZADD).
# ---------------------------------------------------------------------------
run_bench_custom() {
    local v2_flag=$1; shift
    local label=$1; shift
    local threads=$1; shift
    local clients=$1; shift
    local mode_name=$1; shift
    local pipelines=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do
        pipelines+=("$1"); shift
    done
    [[ "${1:-}" == "--" ]] && shift
    local memtier_extra=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do
        memtier_extra+=("$1"); shift
    done
    [[ "${1:-}" == "--" ]] && shift
    local extra_server_flags=("$@")

    start_server "$v2_flag" "$label" "$mode_name" "${extra_server_flags[@]}"

    local target
    target=$(client_target)

    RESULTS_TMP=$(mktemp)
    echo -e "PIPELINE\tRPS\tAVG_LAT(ms)\tP50(ms)\tP99(ms)\tP99.9(ms)\tSEND_SYSCALLS\tBATCH_DENSITY" > "$RESULTS_TMP"

    for PIPELINE in "${pipelines[@]}"; do
        echo "  [+] pipeline=$PIPELINE ..."

        SENDS_BEFORE=$(get_send_count)
        CMDS_BEFORE=$(get_cmd_count)

        local memtier_cmd
        if [[ -n "${MEMTIER_ARGS:-}" ]]; then
            local -a _extra_args
            read -ra _extra_args <<< "$MEMTIER_ARGS"
            memtier_cmd=(memtier_benchmark -s "$target" -p $PORT --pipeline="$PIPELINE" "${_extra_args[@]}")
        else
            memtier_cmd=(memtier_benchmark
                -s "$target"
                -p $PORT
                -t "$threads"
                -c "$clients"
                --pipeline="$PIPELINE"
                --key-prefix=bench
                --test-time=$BENCH_DURATION
                --hide-histogram
                "${memtier_extra[@]}")
        fi
        if [[ -n "$REMOTE_MODE" ]]; then
            echo "  [client cmd] ssh ${SSH_USER}@${CLIENT_HOST} ${memtier_cmd[*]}"
        else
            echo "  [client cmd] $CLIENT_TASKSET ${memtier_cmd[*]}"
        fi

        local OUTPUT
        OUTPUT=$(run_on_client "${memtier_cmd[@]}" 2>&1) || true
        echo "  --- raw memtier output ---"
        echo "$OUTPUT"
        echo "  --- end raw output ---"

        SENDS_AFTER=$(get_send_count)
        CMDS_AFTER=$(get_cmd_count)
        SEND_DELTA=$(( SENDS_AFTER - SENDS_BEFORE ))
        CMD_DELTA=$(( CMDS_AFTER - CMDS_BEFORE ))
        BATCH_DENSITY=$(compute_batch_density "$CMD_DELTA" "$SEND_DELTA")

        RPS=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $2}' || true)
        LATENCY=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $5}' || true)
        P50=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $6}' || true)
        P99=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $7}' || true)
        P999=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $8}' || true)
        if [[ -z "$RPS" ]]; then echo "[!] WARNING: could not parse RPS from memtier output"; fi
        RPS=${RPS:-"Error"}
        LATENCY=${LATENCY:-"N/A"}
        P50=${P50:-"N/A"}
        P99=${P99:-"N/A"}
        P999=${P999:-"N/A"}

        echo -e "$PIPELINE\t$RPS\t$LATENCY\t$P50\t$P99\t$P999\t$SEND_DELTA\t$BATCH_DENSITY" >> "$RESULTS_TMP"
        echo "${label}|${mode_name}|${PIPELINE}|${RPS}|${LATENCY}|${SEND_DELTA}|${BATCH_DENSITY}|${P50}|${P99}|${P999}" >> "$ACCUM_FILE"
    done

    stop_server

    echo ""
    echo "====================================================="
    printf "  %-4s  %s  (commit: %s)\n" "$label" "$mode_name" "$GIT_SHA"
    echo "====================================================="
    column -t -s $'\t' "$RESULTS_TMP"
    echo "====================================================="
    rm "$RESULTS_TMP"
}

# ---------------------------------------------------------------------------
# run_pubsub_bench <v2_flag> <label> <num_subscribers> <num_messages> <pipelines...>
# ---------------------------------------------------------------------------
run_pubsub_bench() {
    local v2_flag=$1; shift
    local label=$1; shift
    local num_subs=$1; shift
    local num_msgs=$1; shift
    local pipelines=("$@")

    start_server "$v2_flag" "$label" "pubsub"

    local target
    target=$(client_target)

    RESULTS_TMP=$(mktemp)
    echo -e "PIPELINE\tPUB_RPS\tSEND_SYSCALLS\tSUBSCRIBERS" > "$RESULTS_TMP"

    local msg_payload
    msg_payload=$(printf '%128s' | tr ' ' 'X')

    for PIPELINE in "${pipelines[@]}"; do
        echo "  [+] pipeline=$PIPELINE, subscribers=$num_subs ..."

        # Launch subscribers (from client machine toward server).
        local sub_cmd_str
        if [[ -n "$REMOTE_MODE" ]]; then
            sub_cmd_str="redis-cli -h $SERVER_HOST -p $PORT SUBSCRIBE bench_chan"
            echo "  [client sub cmd x${num_subs}] ssh ${SSH_USER}@${CLIENT_HOST} ${sub_cmd_str}"
        else
            sub_cmd_str="$CLIENT_TASKSET redis-cli -p $PORT SUBSCRIBE bench_chan"
            echo "  [client sub cmd x${num_subs}] ${sub_cmd_str}"
        fi
        SUB_PIDS=()
        for ((s = 0; s < num_subs; s++)); do
            if [[ -n "$REMOTE_MODE" ]]; then
            ssh -n -o ControlMaster=no -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
                "${SSH_USER}@${CLIENT_HOST}" \
                "redis-cli -h $SERVER_HOST -p $PORT SUBSCRIBE bench_chan > /dev/null 2>&1" &
            else
                $CLIENT_TASKSET redis-cli -p $PORT SUBSCRIBE bench_chan > /dev/null 2>&1 &
            fi
            SUB_PIDS+=($!)
        done
        sleep 1.2  # let all subscribers register

        SENDS_BEFORE=$(get_send_count)

        local -a _rb_opts
        if [[ -n "${REDIS_CLI_ARGS:-}" ]]; then
            read -ra _rb_opts <<< "$REDIS_CLI_ARGS"
        else
            _rb_opts=(-c 5 -q)
        fi
        local OUTPUT
        if [[ -n "$REMOTE_MODE" ]]; then
            local pub_cmd="timeout 60 redis-benchmark -h $SERVER_HOST -p $PORT -n $num_msgs -P $PIPELINE ${_rb_opts[*]} publish bench_chan '$msg_payload'"
            echo "  [client pub cmd] ssh ${SSH_USER}@${CLIENT_HOST} ${pub_cmd}"
            OUTPUT=$(ssh -o ControlMaster=no -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
                "${SSH_USER}@${CLIENT_HOST}" "$pub_cmd" 2>&1) || true
        else
            local pub_cmd="timeout 60 $CLIENT_TASKSET redis-benchmark -p $PORT -n $num_msgs -P $PIPELINE ${_rb_opts[*]} publish bench_chan <payload>"
            echo "  [client pub cmd] ${pub_cmd}"
            OUTPUT=$(timeout 60 $CLIENT_TASKSET redis-benchmark -p "$PORT" -n "$num_msgs" -P "$PIPELINE" \
                "${_rb_opts[@]}" publish bench_chan "$msg_payload" 2>&1) || true
        fi

        echo "  --- raw redis-benchmark output ---"
        echo "$OUTPUT"
        echo "  --- end raw output ---"

        SENDS_AFTER=$(get_send_count)
        SEND_DELTA=$(( SENDS_AFTER - SENDS_BEFORE ))

        RPS=$(echo "$OUTPUT" | tr '\r' '\n' | grep -i 'requests per second' | tail -n 1 | grep -oP '[0-9]+\.[0-9]+' | head -n 1 || true)
        if [[ -z "$RPS" ]]; then echo "[!] WARNING: could not parse RPS from redis-benchmark output"; fi
        RPS=${RPS:-"Error"}

        # Tear down subscribers.
        for pid in "${SUB_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done

        if [[ -n "$REMOTE_MODE" ]]; then
            # Kill all redis-cli on client. -n prevents ssh from stealing stdin.
            ssh -n -o ConnectTimeout=3 -o ControlMaster=no \
                "${SSH_USER}@${CLIENT_HOST}" "pkill -9 redis-cli" 2>/dev/null || true
        fi

        # Watchdog: if SSH proxy processes hang, force-kill after 2s.
        ( sleep 2; kill -9 "${SUB_PIDS[@]}" 2>/dev/null || true ) &
        local watchdog_pid=$!

        wait "${SUB_PIDS[@]}" 2>/dev/null || true
        kill "$watchdog_pid" 2>/dev/null || true
        SUB_PIDS=()

        echo -e "${PIPELINE}\t${RPS}\t${SEND_DELTA}\t${num_subs}" >> "$RESULTS_TMP"
        echo "${label}|pubsub|${PIPELINE}|${RPS}||${SEND_DELTA}" >> "$ACCUM_FILE"
    done

    stop_server

    echo ""
    echo "====================================================="
    printf "  %-4s  %s  (commit: %s)\n" "$label" "pubsub" "$GIT_SHA"
    echo "====================================================="
    column -t -s $'\t' "$RESULTS_TMP"
    echo "====================================================="
    rm "$RESULTS_TMP"
}

print_final_report() {
    [[ $RUNS -le 1 ]] && return
    echo ""
    echo "======================================================"
    echo "  AVERAGE RESULTS ($RUNS runs, commit: $GIT_SHA)"
    echo "======================================================"
    awk -F'|' '
    {
        key = $1 SUBSEP $2 SUBSEP $3
        if (!(key in seen)) {
            seen[key] = 1
            order[++n] = key
            lbl[key] = $1; mod[key] = $2; pip[key] = $3
        }
        if ($4+0 > 0) { rps_sum[key] += $4; rps_n[key]++ }
        if ($5+0 > 0) { lat_sum[key] += $5; lat_n[key]++ }
        sys_sum[key] += $6; sys_cnt[key]++
        if ($7+0 > 0) { bd_sum[key] += $7; bd_n[key]++ }
        if ($8+0 > 0) { p50_sum[key] += $8; p50_n[key]++ }
        if ($9+0 > 0) { p99_sum[key] += $9; p99_n[key]++ }
        if ($10+0 > 0) { p999_sum[key] += $10; p999_n[key]++ }
    }
    END {
        prev_group = ""
        for (i = 1; i <= n; i++) {
            k = order[i]
            g = lbl[k] SUBSEP mod[k]
            if (g != prev_group) {
                if (prev_group != "") print "====================================================="
                print ""
                printf "  %-4s  %s\n", lbl[k], mod[k]
                print "====================================================="
                print "PIPELINE\tRPS\tAVG_LAT(ms)\tP50(ms)\tP99(ms)\tP99.9(ms)\tSEND_SYSCALLS\tBATCH_DENSITY"
                prev_group = g
            }
            rps_avg = (rps_n[k] > 0) ? rps_sum[k] / rps_n[k] : 0
            lat_avg = (lat_n[k] > 0) ? lat_sum[k] / lat_n[k] : 0
            bd_avg  = (bd_n[k] > 0)  ? bd_sum[k] / bd_n[k]   : 0
            p50_avg = (p50_n[k] > 0) ? p50_sum[k] / p50_n[k] : 0
            p99_avg = (p99_n[k] > 0) ? p99_sum[k] / p99_n[k] : 0
            p999_avg = (p999_n[k] > 0) ? p999_sum[k] / p999_n[k] : 0
            printf "%s\t%.2f\t%.5f\t%.5f\t%.5f\t%.5f\t%.0f\t%.1f\n", pip[k], rps_avg, lat_avg, p50_avg, p99_avg, p999_avg, sys_sum[k]/sys_cnt[k], bd_avg
        }
        print "====================================================="
    }
    ' "$ACCUM_FILE" | column -t -s $'\t'
}

# ---------- MODE: multi_conn ----------
# 50 concurrent clients saturate the server. Regression-guard for peak RPS.
# Respects CMD env var: "set" (default, 2KB value) or "zadd" (sync command path).
# For non-dragonfly SERVER_TYPE: runs once with the server type as label (ver ignored).
run_multi_conn() {
    local pipelines
    read -ra pipelines <<< "$(filter_pipelines 1 10 50 100)"

    # Non-dragonfly servers have no V1/V2 distinction: run once with server type as label.
    if [[ "$SERVER_TYPE" != "dragonfly" ]]; then
        _metrics_verified=0
        if [[ "$CMD" == "zadd" ]]; then
            run_bench_custom false "$SERVER_TYPE" 2 25 "multi_conn_zadd" "${pipelines[@]}" \
                -- --command="ZADD __key__ 1 __data__" --command-key-pattern=R -d 32
        else
            run_bench false "$SERVER_TYPE" 2 25 2048 "multi_conn" "${pipelines[@]}"
        fi
        return
    fi

    if [[ "$CMD" == "zadd" ]]; then
        if [[ "$VER" != "v2" ]]; then
            _metrics_verified=0
            run_bench_custom false "V1" 2 25 "multi_conn_zadd" "${pipelines[@]}" \
                -- --command="ZADD __key__ 1 __data__" --command-key-pattern=R -d 32
        fi
        if [[ "$VER" != "v1" ]]; then
            _metrics_verified=0
            run_bench_custom true  "V2" 2 25 "multi_conn_zadd" "${pipelines[@]}" \
                -- --command="ZADD __key__ 1 __data__" --command-key-pattern=R -d 32
        fi
    else
        if [[ "$VER" != "v2" ]]; then
            _metrics_verified=0
            run_bench false "V1" 2 25 2048 "multi_conn" "${pipelines[@]}"
        fi
        if [[ "$VER" != "v1" ]]; then
            _metrics_verified=0
            run_bench true  "V2" 2 25 2048 "multi_conn" "${pipelines[@]}"
        fi
    fi
}

# ---------- MODE: single_conn ----------
# 1 client. Isolates per-connection behavior (coalescing, latency).
# Respects CMD env var: "set" (default, 2KB value) or "zadd" (sync command path).
# For non-dragonfly SERVER_TYPE: runs once with the server type as label (ver ignored).
run_single_conn() {
    local pipelines
    read -ra pipelines <<< "$(filter_pipelines 1 10 50 100)"

    # Non-dragonfly servers have no V1/V2 distinction: run once with server type as label.
    if [[ "$SERVER_TYPE" != "dragonfly" ]]; then
        _metrics_verified=0
        if [[ "$CMD" == "zadd" ]]; then
            run_bench_custom false "$SERVER_TYPE" 1 1 "single_conn_zadd" "${pipelines[@]}" \
                -- --command="ZADD __key__ 1 __data__" --command-key-pattern=R -d 32
        else
            run_bench false "$SERVER_TYPE" 1 1 2048 "single_conn" "${pipelines[@]}"
        fi
        return
    fi

    if [[ "$CMD" == "zadd" ]]; then
        if [[ "$VER" != "v2" ]]; then
            _metrics_verified=0
            run_bench_custom false "V1" 1 1 "single_conn_zadd" "${pipelines[@]}" \
                -- --command="ZADD __key__ 1 __data__" --command-key-pattern=R -d 32
        fi
        if [[ "$VER" != "v1" ]]; then
            _metrics_verified=0
            run_bench_custom true  "V2" 1 1 "single_conn_zadd" "${pipelines[@]}" \
                -- --command="ZADD __key__ 1 __data__" --command-key-pattern=R -d 32
        fi
    else
        if [[ "$VER" != "v2" ]]; then
            _metrics_verified=0
            run_bench false "V1" 1 1 2048 "single_conn" "${pipelines[@]}"
        fi
        if [[ "$VER" != "v1" ]]; then
            _metrics_verified=0
            run_bench true  "V2" 1 1 2048 "single_conn" "${pipelines[@]}"
        fi
    fi
}

# ---------- MODE: pubsub ----------
# 10 subscribers, 128-byte messages. Measures wakeup and reply batching.
# Standard Redis pub/sub protocol — works with any SERVER_TYPE.
# For non-dragonfly SERVER_TYPE: runs once with the server type as label (ver ignored).
run_pubsub() {
    local pipelines
    read -ra pipelines <<< "$(filter_pipelines 1 10 50 100)"

    # Non-dragonfly servers have no V1/V2 distinction: run once with server type as label.
    if [[ "$SERVER_TYPE" != "dragonfly" ]]; then
        _metrics_verified=0
        run_pubsub_bench false "$SERVER_TYPE" 10 50000 "${pipelines[@]}"
        return
    fi

    if [[ "$VER" != "v2" ]]; then
        _metrics_verified=0
        run_pubsub_bench false "V1" 10 50000 "${pipelines[@]}"
    fi
    if [[ "$VER" != "v1" ]]; then
        _metrics_verified=0
        run_pubsub_bench true  "V2" 10 50000 "${pipelines[@]}"
    fi
}

# ---------- Main ----------
run_selected_modes() {
    case "$MODE" in
        multi_conn)    run_multi_conn ;;
        single_conn)   run_single_conn ;;
        pubsub)        run_pubsub ;;
        all)           run_multi_conn; run_single_conn; run_pubsub ;;
    esac
}

case "$MODE" in
    multi_conn|single_conn|pubsub|all)
        {
            for ((r=1; r<=RUNS; r++)); do
                [[ $RUNS -gt 1 ]] && echo "" && echo "############## Run $r / $RUNS ##############"
                run_selected_modes
            done
            print_final_report
        } 2>&1 | tee "$LOG_FILE"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Valid: multi_conn | single_conn | pubsub | all"
        exit 1
        ;;
esac

echo ""
echo "[*] Benchmark results saved to: $LOG_FILE"
