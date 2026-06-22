#!/bin/bash
set -eo pipefail

# ---------------------------------------------------------------------------
# Result-variance warning threshold (static config).
# WARN ONLY (never stops/fails the run): if, across the repeated runs (RUNS > 1), the
# spread between the minimum and maximum Ops/sec for ANY (version, mode, pipeline) result
# exceeds this percentage, where spread% = (max - min) / min * 100, a warning is written to
# the log. Helps flag noisy/unreliable benchmark environments without aborting the run.
# Set to a large value (e.g. 100000) to effectively silence the warning.
# ---------------------------------------------------------------------------
MAX_RESULT_VARIANCE_PCT=10

# ---------------------------------------------------------------------------
# Adaptive per-cell averaging (active only when averaging, i.e. effective runs > 1).
#
# A "cell" = one identical (memtier cmd + dragonfly cmd) = a (version, mode, pipeline-depth,
# data-size) point. Cells are fully independent: p=1 vs p=100, single- vs multi-conn have very
# different noise, so they are judged separately. The goal per cell is a TRUSTED GROUP: >= MIN_RUNS
# samples whose RPS all fall within MAX_RESULT_VARIANCE_PCT of each other.
#
# Flow: run the full matrix MIN_RUNS times (baseline), then RE-RUN ONLY the cells that still lack
# a trusted group - one extra sample per round, reusing all prior samples - until every cell has a
# trusted group or reaches MAX_RUNS samples. Cells that already agree are NEVER re-run, which is
# where the time is saved. The final report averages each cell's tightest trusted group and lists
# the dropped (untrusted) samples.
#   * MIN_RUNS   minimum samples per cell AND trusted-group size (always >= 3).
#   * MAX_RUNS   per-cell hard cap on samples (stop retrying a cell that never settles).
# Set ADAPTIVE_AVG=0 to restore plain fixed-count averaging (no targeted retries).
# A single run (runs==1, e.g. the NUM_RUNS=1 smoke test) is never averaged and never retried.
# ---------------------------------------------------------------------------
MIN_RUNS="${MIN_RUNS:-3}"
MAX_RUNS="${MAX_RUNS:-9}"
ADAPTIVE_AVG="${ADAPTIVE_AVG:-1}"

usage() {
    cat <<EOF
Usage: ./bench_v2.sh [binary] [mode] [runs] [tag] [ver] [threads] [server_log_dir]
       ./bench_v2.sh --batch <description-file>     # run many invocations from a file (see "Batch mode")
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
  mode:         multi_conn | single_conn | pubsub | conn | all (default: all)
  runs:         how many times to repeat the full benchmark (default: 3).
                Must be a positive integer. If runs > 1, a final average report is printed.
  tag:          optional label for the log file.
                If given, log file is: {tag}_{HHMMSS}.log
                If omitted:             bench_v2_{mode}_{sha}_{YYYYMMDD_HHMMSS}.log
  ver:          which version(s) to run: v1 | v2 | both (default: both)
  threads:      number of server threads (proactor threads for dragonfly/ok_backend).
                For SERVER_TYPE=valkey, use VALKEY_IO_THREADS instead; this arg is ignored.
                Default: nproc-1 in local mode (leaves 1 core for memtier),
                         nproc   in remote mode (CLIENT_IP is set, client is elsewhere).
  server_log_dir: optional directory to save the server log.
                If given, the log is copied there on exit as server_{tag_or_sha}_{TIMESTAMP}.log.
                If omitted, the log is discarded.

Batch mode (--batch <file> | -b <file>):
  Runs many bench_v2.sh invocations described in a plain text file, one run per line.
  Replaces wrapper scripts like run_stage1.sh. By convention, name the file *.bench
  (e.g. stage1.bench) — the extension is not enforced but makes them easy to spot.
  --batch is EXCLUSIVE: it cannot be combined
  with positional arguments, and the following per-run env vars must NOT be set in the
  environment (the script exits with an error if they are):
    SERVER_TYPE, VALKEY_IO_THREADS, PIPELINE, CMD, EXTRA_SERVER_FLAGS, MEMTIER_ARGS,
    REDIS_CLI_ARGS, NUM_RUNS, BENCH_DURATION, CLIENT_TIMEOUT, CLIENT_DELAY_US,
    SERVER_METRICS_PORT, SERVER_LOG_DIR.
  Put those per-run knobs on the relevant line instead.

  Still honored from the environment with --batch (they are global, not per-run):
    SERVER_IP, CLIENT_IP, SSH_USER  - cross-machine topology, applied to every run.
    BENCH_LOG_DIR                       - directory that collects all logs (see below).
    METRICS_DIR                         - directory for /metrics snapshots (defaults to
                                          BENCH_LOG_DIR/metrics; set METRICS_DIR=off to disable).
    BATCH_MODE_RUN_ONLY                 - comma-separated 1-based run indices to run, e.g.
                                          BATCH_MODE_RUN_ONLY=4,7,9 runs only those lines.
                                          Any index outside 1..N aborts immediately before
                                          any run starts.
    BATCH_RERUN_ON_VARIANCE             - max attempts per run when a HIGH VARIANCE WARNING
                                          is detected (default 1 = no auto-rerun). On a
                                          flagged attempt the run's .log/server/metrics files
                                          are renamed with a _HIGH_VARIANCE_WARNING suffix and
                                          the run is retried up to this many times. Runs still
                                          flagged after the last attempt are listed in a final
                                          recommendation block.

  Description file format:
    * One run per line, using the same syntax you would type after ./bench_v2.sh:
        [ENV=val ...] <binary> <mode> <runs> <tag> [ver] [threads] [server_log_dir]
    * Lines are shell-tokenized, so quote values that contain spaces, e.g.
        EXTRA_SERVER_FLAGS='--maxmemory 8gb'
    * Blank lines and lines starting with '#' are ignored. Comment a line out to skip it
      (this replaces run_stage1.sh's START_ROW/END_ROW).
    * The file is tokenized by the shell (like run_stage1.sh's eval) — only run files you trust.

  Logs survive every run:
    All per-run .log files, all server logs, and a top-level batch_run_<ts>.log are written
    into a single output directory. Default: ./bench_batch_<file>_<timestamp>/.
    Override it with BENCH_LOG_DIR=/path/to/dir. A failing run does not stop the batch; the
    final summary reports failures and the script exits non-zero if any run failed.

Environment variables (for cross-machine benchmarking):
  CLIENT_IP:  SSH host of the client machine that runs memtier/redis-benchmark.
                If unset, client tools run locally (single-machine mode).
  SERVER_IP:  IP the client uses to reach this server (default: 127.0.0.1).
                Set to the server's private IP when CLIENT_IP is set.
  SSH_USER:     SSH username for CLIENT_IP (default: current user).
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
  KEY_MAXIMUM:  Upper bound on the memtier key space (--key-maximum). Caps the dataset
                so memory stops growing once all keys are written - prevents the server
                from filling RAM and thrashing/OOMing on long write (SET) runs.
                Default: 1000000 (~2 GB at the default 2 KB value). Raise for a larger
                working set; set to 0 or 'off' to use memtier's own default (10M, which
                can OOM with large values). Ignored when MEMTIER_ARGS is set
                (put --key-maximum directly in MEMTIER_ARGS).
                Example: KEY_MAXIMUM=5000000
  SERVER_MAXMEMORY: Safety-net memory cap passed to the server as --maxmemory.
                Default: ~70% of total RAM (auto-detected). At the limit Dragonfly
                rejects writes and stays alive instead of swapping/freezing. Set to
                'off' or 0 to disable; skipped if EXTRA_SERVER_FLAGS already sets
                --maxmemory. Example: SERVER_MAXMEMORY=20gb
  SERVER_PIN:   Remote mode only. 'on' (default) pins the local server with taskset;
                'off' disables it. With a spare core the server is pinned to cores
                1..threads, leaving core 0 free for the OS and NIC (ENA) IRQs - pass
                threads < nproc (e.g. 3 on a 4-core box) to create that spare core, and
                pin the NIC IRQs to core 0 (see README). Local mode always pins.
  SERVER_CPUS:  Remote mode only. Explicit core list for the server taskset
                (e.g. SERVER_CPUS=1-3 or 0,2). Overrides the automatic selection.
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
  conn           - multi_conn + single_conn (no pubsub).
  all            - Run all modes sequentially.

Examples:
  # Local (single machine):
  ./bench_v2.sh ./build-opt/dragonfly pubsub 5 my_tag v2

  # Cross-machine (run this ON THE SERVER):
  SERVER_IP=172.31.30.209 CLIENT_IP=172.31.20.3 ./bench_v2.sh ./build-opt/dragonfly all 3

  # Run only pipeline=10 cross-machine, V2 only:
  PIPELINE=10 SERVER_IP=172.31.30.209 CLIENT_IP=172.31.20.3 ./bench_v2.sh ./build-opt/dragonfly multi_conn 1 mytag v2

  # Benchmark ZADD (sync command path) with 50 connections:
  CMD=zadd ./bench_v2.sh ./build-opt/dragonfly multi_conn 1 mytag both

  # Run pipeline=10 and pipeline=100 only:
  PIPELINE=10,100 ./bench_v2.sh ./build-opt/dragonfly multi_conn 1 mytag both

  # Benchmark Valkey (pass valkey-server binary as first arg):
  SERVER_TYPE=valkey ./bench_v2.sh ./valkey-server single_conn 3 valkey_1t
  SERVER_TYPE=valkey ./bench_v2.sh ~/bin/valkey-server single_conn 3 valkey_1t
  SERVER_IP=172.31.30.209 CLIENT_IP=172.31.20.3 SERVER_TYPE=valkey \\
    ./bench_v2.sh /usr/local/bin/valkey-server multi_conn 3 valkey_cross

  # Benchmark with multishot recv enabled (V2 only):
  EXTRA_SERVER_FLAGS="--uring_recv_buffer_cnt=16000" \\
    ./bench_v2.sh ./build-opt/dragonfly multi_conn 3 dfly_multishot v2

  # Benchmark ok_backend (same flags as dragonfly/ok_backend):
  ./bench_v2.sh ./build-opt/ok_backend multi_conn 3 ok_backend_4t both

  # Quick smoke test — 1 run, 2 seconds per pipeline depth:
  NUM_RUNS=1 BENCH_DURATION=2 ./bench_v2.sh ./build-opt/dragonfly multi_conn _ my_tag both 4

  # Batch mode — run a whole matrix from a description file (replaces run_stage1.sh):
  SERVER_IP=172.31.30.209 CLIENT_IP=172.31.20.3 ./bench_v2.sh --batch stage1.bench

  # Batch mode locally, collecting every log under a chosen directory:
  BENCH_LOG_DIR=./results_stage1 ./bench_v2.sh --batch stage1.bench

  # Example *.bench file contents (recommended extension: .bench):
  # Row 1: Valkey baseline
  SERVER_TYPE=valkey ../valkey/src/valkey-server single_conn 3 valkey_1t_single
  # Row 2: ok_backend V1+V2
  ./build-opt/ok_backend multi_conn 3 ok_v1v2_4t_multi both 4
  # Row 3: Dragonfly with extra flags (quote values with spaces)
  EXTRA_SERVER_FLAGS='--maxmemory 8gb' ./build-opt/dragonfly multi_conn 3 dfly_4t both 4
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" || $# -eq 0 ]]; then
    usage
    exit 0
fi

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

# ---------------------------------------------------------------------------
# _batch_split_line <raw line>
# Tokenizes one description-file line (respecting shell quoting) and splits it
# into leading KEY=VALUE assignments (-> parsed_envs) and the remaining
# bench_v2.sh positional args (-> parsed_args). Returns non-zero if the line
# cannot be tokenized. The description file is a trusted, user-authored config.
# ---------------------------------------------------------------------------
_batch_split_line() {
    local raw="$1"
    local -a _toks=()
    eval "_toks=($raw)" 2>/dev/null || return 1
    parsed_envs=()
    parsed_args=()
    local seen=0 t
    for t in "${_toks[@]}"; do
        if [[ $seen -eq 0 && "$t" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
            parsed_envs+=("$t")
        else
            seen=1
            parsed_args+=("$t")
        fi
    done
    return 0
}

# ---------------------------------------------------------------------------
# run_batch_mode <description-file> [stray args...]
# Runs every line of the description file as an independent bench_v2.sh
# invocation (re-executing this script), so each run gets a clean global state.
# All console output, per-run .log files and server logs are collected in one
# output directory so nothing is overwritten between runs.
# ---------------------------------------------------------------------------

# Renames the output files produced by one (discarded) high-variance attempt so
# they are clearly marked and never confused with the trusted final attempt.
# Reads the exact paths the child printed into the captured run-output file $2.
# Adds a _HIGH_VARIANCE_WARNING suffix before the extension.
_mark_variance_files() {
    local run_out=$1 p
    # Main per-run console log: "[*] Benchmark results saved to: <path>"
    while IFS= read -r p; do
        [[ -f "$p" ]] && mv -f "$p" "${p%.log}_HIGH_VARIANCE_WARNING.log"
    done < <(grep -oP 'Benchmark results saved to: \K.*' "$run_out" 2>/dev/null)
    # Server log(s): "[*] Server log saved to <path>"
    while IFS= read -r p; do
        [[ -f "$p" ]] && mv -f "$p" "${p%.log}_HIGH_VARIANCE_WARNING.log"
    done < <(grep -oP 'Server log saved to \K.*' "$run_out" 2>/dev/null)
    # Metrics snapshot(s): "[*] Metrics snapshot saved to <path>"
    while IFS= read -r p; do
        [[ -f "$p" ]] && mv -f "$p" "${p%.prom}_HIGH_VARIANCE_WARNING.prom"
    done < <(grep -oP 'Metrics snapshot saved to \K.*' "$run_out" 2>/dev/null)
}

run_batch_mode() {
    local desc_file="$1"; shift
    local -a leftover=("$@")

    # --- Conflict checks: --batch is exclusive -------------------------------
    if [[ ${#leftover[@]} -gt 0 ]]; then
        echo "[!] Error: --batch cannot be combined with positional arguments."
        echo "    Offending extra args: ${leftover[*]}"
        echo "    Put each run (binary mode runs tag ver threads) on its own line in '${desc_file}'."
        exit 1
    fi
    if [[ ! -f "$desc_file" ]]; then
        echo "[!] Error: batch description file '${desc_file}' not found or not a regular file."
        exit 1
    fi
    # Per-run knobs must live on each line, not in the global environment, so one
    # run's settings never silently leak into every other run.
    local -a forbidden=(SERVER_TYPE VALKEY_IO_THREADS PIPELINE CMD EXTRA_SERVER_FLAGS
                        MEMTIER_ARGS REDIS_CLI_ARGS NUM_RUNS BENCH_DURATION
                        CLIENT_TIMEOUT CLIENT_DELAY_US SERVER_METRICS_PORT SERVER_LOG_DIR
                        METRICS_DIR)
    local v
    for v in "${forbidden[@]}"; do
        if [[ -n "${!v:-}" ]]; then
            echo "[!] Error: environment variable '${v}' cannot be combined with --batch."
            echo "    It is a per-run setting. Put it on the relevant line in '${desc_file}', e.g.:"
            echo "        ${v}=<value> ./build-opt/dragonfly multi_conn 3 mytag both 4"
            exit 1
        fi
    done

    # --- Output directory: every log from every run is collected here ---------
    local ts; ts=$(date +%Y%m%d_%H%M%S)
    local descname; descname=$(basename -- "$desc_file"); descname="${descname%.*}"
    local outdir="${BENCH_LOG_DIR:-bench_batch_${descname:-runs}_${ts}}"
    mkdir -p "$outdir" || { echo "[!] Error: cannot create output dir '${outdir}'."; exit 1; }
    outdir=$(cd "$outdir" && pwd) || { echo "[!] Error: cannot resolve output dir '${outdir}'."; exit 1; }

    # Copy the bench/desc file into the output dir for reference (provenance), so a
    # results folder is always self-contained and the exact run matrix is preserved.
    cp -- "$desc_file" "$outdir/" 2>/dev/null \
        || echo "[!] Warning: could not copy bench file '${desc_file}' into '${outdir}'."

    # Also copy the bench.sh engine itself into the output dir, so the results folder
    # records the exact script version that produced them (alongside the commit hash in
    # each run's AVERAGE RESULTS header).
    cp -- "${BASH_SOURCE[0]}" "$outdir/" 2>/dev/null \
        || echo "[!] Warning: could not copy bench script '${BASH_SOURCE[0]}' into '${outdir}'."

    # --- Collect runnable lines (skip blanks and '#' comments) ---------------
    local -a run_lines=()
    local line trimmed
    while IFS= read -r line || [[ -n "$line" ]]; do
        trimmed="${line#"${line%%[![:space:]]*}"}"   # left-trim
        [[ -z "$trimmed" ]] && continue              # blank line
        [[ "$trimmed" == \#* ]] && continue          # comment line
        run_lines+=("$line")
    done < "$desc_file"
    if [[ ${#run_lines[@]} -eq 0 ]]; then
        echo "[!] Error: no runnable lines in '${desc_file}' (only blanks/comments)."
        exit 1
    fi

    # --- BATCH_MODE_RUN_ONLY: optional subset of run indices (1-based) --------
    # e.g. BATCH_MODE_RUN_ONLY=4,7,9 runs only those lines. Any index out of the
    # 1..N range aborts immediately (before any run starts), as requested.
    if [[ -n "${BATCH_MODE_RUN_ONLY:-}" ]]; then
        local _ro _clean="${BATCH_MODE_RUN_ONLY//[[:space:]]/}"
        local -a _ro_list
        IFS=',' read -ra _ro_list <<< "$_clean"
        for _ro in "${_ro_list[@]}"; do
            if ! [[ "$_ro" =~ ^[0-9]+$ ]] || (( _ro < 1 || _ro > ${#run_lines[@]} )); then
                echo "[!] Error: BATCH_MODE_RUN_ONLY has invalid run index '${_ro}'."
                echo "    Valid range for '${desc_file}' is 1..${#run_lines[@]}."
                exit 1
            fi
        done
        echo "[*] BATCH_MODE_RUN_ONLY active: running only run(s) ${_clean} of ${#run_lines[@]}."
    fi

    # --- BATCH_RERUN_ON_VARIANCE: max attempts per run when a HIGH VARIANCE ---
    # WARNING is detected (default 1 = single attempt, no auto-rerun). On a
    # flagged attempt the run's output files are renamed with a
    # _HIGH_VARIANCE_WARNING suffix and the run is retried, up to this many times.
    local _max_attempts="${BATCH_RERUN_ON_VARIANCE:-1}"
    if ! [[ "$_max_attempts" =~ ^[0-9]+$ ]] || (( _max_attempts < 1 )); then
        echo "[!] Error: BATCH_RERUN_ON_VARIANCE must be a positive integer (got '${_max_attempts}')."
        exit 1
    fi

    # --- Preflight: parse every line and verify its binary exists ------------
    local -a parsed_envs=() parsed_args=()
    local idx=0 ok=1
    echo "========================================"
    echo " Batch preflight: ${desc_file} (${#run_lines[@]} run(s))"
    echo "========================================"
    for line in "${run_lines[@]}"; do
        idx=$((idx + 1))
        if ! _batch_split_line "$line"; then
            echo "  [!!] run ${idx}: cannot parse line: ${line}"; ok=0; continue
        fi
        if [[ ${#parsed_args[@]} -eq 0 ]]; then
            echo "  [!!] run ${idx}: no binary/arguments: ${line}"; ok=0; continue
        fi
        if _resolve_binary "${parsed_args[0]}" >/dev/null 2>&1; then
            echo "  [ok] run ${idx}: ${parsed_args[0]}"
        else
            echo "  [!!] run ${idx}: binary not found: ${parsed_args[0]}"; ok=0
        fi
    done
    if [[ $ok -ne 1 ]]; then
        echo ""
        echo "[!] Error: batch preflight failed. Fix the lines above and retry."
        exit 1
    fi
    echo "  All ${#run_lines[@]} run(s) look valid."

    # --- Execute every run ----------------------------------------------------
    # BENCH_LOG_DIR is injected first so each child writes its .log and server
    # log(s) into ${outdir}; a per-line BENCH_LOG_DIR= would override it.
    local self="$0"
    local batch_log="${outdir}/batch_run_${ts}.log"
    local failures=0 rc
    set +e
    {
        echo "========================================"
        echo " Batch run: ${desc_file}"
        echo " Output dir: ${outdir}"
        echo " Runs: ${#run_lines[@]}"
        echo "========================================"
        idx=0
        variance_runs=()   # "idx|tag|attempts" for runs still flagged at the end
        for line in "${run_lines[@]}"; do
            idx=$((idx + 1))
            _batch_split_line "$line"

            # Honor BATCH_MODE_RUN_ONLY: skip runs not in the selected set.
            if [[ -n "${BATCH_MODE_RUN_ONLY:-}" ]] &&
               [[ ",${BATCH_MODE_RUN_ONLY//[[:space:]]/}," != *",${idx},"* ]]; then
                echo ""
                echo "# [${idx}/${#run_lines[@]}] SKIPPED (not in BATCH_MODE_RUN_ONLY)"
                continue
            fi

            tag="${parsed_args[3]:-run${idx}}"
            attempt=1
            while true; do
                echo ""
                echo "##########################################################################################"
                echo "# [${idx}/${#run_lines[@]}] attempt ${attempt}/${_max_attempts}: ${line}"
                echo "##########################################################################################"
                run_out=$(mktemp)
                # Pass the run's 1-based position in the batch so the child can label
                # its AVERAGE RESULTS header with "Run idx/total".
                env BENCH_LOG_DIR="$outdir" BATCH_RUN_INDEX="$idx" BATCH_RUN_TOTAL="${#run_lines[@]}" \
                    "${parsed_envs[@]}" bash "$self" "${parsed_args[@]}" 2>&1 | tee "$run_out"
                rc=${PIPESTATUS[0]}

                if [[ $rc -ne 0 ]]; then
                    echo "# [${idx}/${#run_lines[@]}] FAILED (exit ${rc})"
                    failures=$((failures + 1))
                    rm -f "$run_out"
                    break
                fi

                if grep -q "HIGH VARIANCE WARNING" "$run_out"; then
                    if (( attempt < _max_attempts )); then
                        echo "# [${idx}/${#run_lines[@]}] HIGH VARIANCE on attempt ${attempt}; marking files _HIGH_VARIANCE_WARNING and retrying."
                        _mark_variance_files "$run_out"
                        attempt=$((attempt + 1))
                        rm -f "$run_out"
                        continue
                    fi
                    echo "# [${idx}/${#run_lines[@]}] OK but HIGH VARIANCE persisted after ${attempt} attempt(s)."
                    variance_runs+=("${idx}|${tag}|${attempt}")
                else
                    echo "# [${idx}/${#run_lines[@]}] OK"
                fi
                rm -f "$run_out"
                break
            done
        done
        echo ""
        echo "========================================"
        echo " Batch complete: $(( ${#run_lines[@]} - failures )) run(s) succeeded, ${failures} failed."
        echo " All logs saved under: ${outdir}"
        echo "========================================"

        # Final recommendation: list every run still flagged HIGH VARIANCE so the
        # user knows exactly which numbers are untrusted and how to re-run them.
        if [[ ${#variance_runs[@]} -gt 0 ]]; then
            vidx=()
            echo ""
            echo "########################################################################"
            echo " [!] HIGH VARIANCE RECOMMENDATION - the following run(s) are UNTRUSTED"
            echo "########################################################################"
            for v in "${variance_runs[@]}"; do
                IFS='|' read -r vi vt va <<< "$v"
                vidx+=("$vi")
                echo "   - run ${vi}: ${vt}  (still high after ${va} attempt(s))"
            done
            vcsv=$(IFS=,; echo "${vidx[*]}")
            echo ""
            echo "   Re-run only these in isolation before trusting their numbers:"
            echo "     BATCH_MODE_RUN_ONLY=${vcsv} ./bench.sh --batch ${desc_file}"
            echo "   (optionally add BATCH_RERUN_ON_VARIANCE=3 to auto-retry up to 3x each)"
            echo "########################################################################"
        else
            echo ""
            echo "[*] No HIGH VARIANCE warnings remained. All completed runs are trusted."
        fi
        [[ $failures -eq 0 ]]

    } 2>&1 | tee "$batch_log"
    local batch_rc=${PIPESTATUS[0]}
    set -e

    echo ""
    echo "[*] Batch console log: ${batch_log}"
    echo "[*] Per-run logs and server logs are under: ${outdir}"
    return "$batch_rc"
}

# Batch mode is exclusive: handle it before any single-run positional parsing.
if [[ "${1:-}" == "--batch" || "${1:-}" == "-b" ]]; then
    _batch_file="${2:-}"
    if [[ -z "$_batch_file" ]]; then
        echo "[!] Error: ${1} requires a description-file argument."
        echo "    Usage: ./bench_v2.sh --batch <description-file>"
        exit 1
    fi
    shift 2
    run_batch_mode "$_batch_file" "$@" || exit $?
    exit 0
fi

SERVER_BIN=${1:-"./build-opt/dragonfly"}
MODE=${2:-"all"}
RUNS=${NUM_RUNS:-${3:-3}}
TAG=${4:-""}
VER=${5:-"both"}

if ! _resolved=$(_resolve_binary "$SERVER_BIN"); then
    echo "[!] Error: binary '$SERVER_BIN' not found or not a regular file/symlink."
    echo "    Use a relative path (./build-opt/dragonfly), absolute path (/usr/bin/valkey-server),"
    echo "    tilde path (~/bin/server), or a bare name that resolves via 'which'."
    exit 1
fi
SERVER_BIN="$_resolved"
case "$MODE" in
    multi_conn|single_conn|pubsub|conn|all) ;;
    *) echo "[!] Error: mode must be one of: multi_conn | single_conn | pubsub | conn | all. Got: '$MODE'"; exit 1 ;;
esac
if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
    echo "[!] Error: runs must be a positive integer. Got: '$RUNS'"
    exit 1
fi
case "$VER" in
    v1|v2|both) ;;
    *) echo "[!] Error: ver must be one of: v1 | v2 | both. Got: '$VER'"; exit 1 ;;
esac

# Adaptive per-cell averaging setup. Only engages when averaging (RUNS > 1). RUNS is the number of
# baseline full-matrix passes; per-cell targeted retries then top up failing cells up to MAX_RUNS.
if ! [[ "$MIN_RUNS" =~ ^[0-9]+$ ]] || [[ "$MIN_RUNS" -lt 1 ]]; then
    echo "[!] Error: MIN_RUNS must be a positive integer. Got: '$MIN_RUNS'"; exit 1
fi
if ! [[ "$MAX_RUNS" =~ ^[0-9]+$ ]] || [[ "$MAX_RUNS" -lt "$MIN_RUNS" ]]; then
    echo "[!] Error: MAX_RUNS must be an integer >= MIN_RUNS (${MIN_RUNS}). Got: '$MAX_RUNS'"; exit 1
fi
if [[ "$RUNS" -gt 1 && "$ADAPTIVE_AVG" == "1" ]]; then
    ADAPTIVE=1
    [[ "$RUNS" -lt "$MIN_RUNS" ]] && RUNS=$MIN_RUNS   # always run at least MIN_RUNS baseline passes
    [[ "$RUNS" -gt "$MAX_RUNS" ]] && MAX_RUNS=$RUNS    # honor an explicit large request
else
    ADAPTIVE=0
fi
# Arg 6 can be either threads (a number) or server_log_dir (a path starting with / or .).
# If it looks like a path, treat it as SERVER_LOG_DIR and use default threads.
# Default threads: all CPUs in remote mode (client is elsewhere), all-minus-one in local mode.
_default_threads() {
    local n; n=$(nproc)
    if [[ -n "${CLIENT_IP:-}" ]]; then
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
SERVER_IP=${SERVER_IP:-"127.0.0.1"}
# Server type: which server process to launch.
# 'dragonfly' (default) or 'valkey'; [binary] arg is the executable for both.
SERVER_TYPE=${SERVER_TYPE:-dragonfly}
VALKEY_IO_THREADS=${VALKEY_IO_THREADS:-1}
CLIENT_IP=${CLIENT_IP:-""}
SSH_USER=${SSH_USER:-"$(whoami)"}
CLIENT_DELAY_US=${CLIENT_DELAY_US:-0}
PIPELINE_FILTER=${PIPELINE:-""}
CMD=${CMD:-"set"}
BENCH_DURATION=${BENCH_DURATION:-15}

# Value size in bytes. For single_conn/multi_conn this is memtier's -d (the SET
# value size); for pubsub it is the published message length. Lets a batch line
# sweep payload sizes (e.g. DATA_SIZE=256 vs DATA_SIZE=4096) without editing the
# script. Default 2048 preserves prior single/multi behavior. _DATA_SIZE_EXPLICIT
# records whether the caller set it, so pubsub keeps its historical 128-byte
# message unless DATA_SIZE was given explicitly.
_DATA_SIZE_EXPLICIT=${DATA_SIZE:+1}
DATA_SIZE=${DATA_SIZE:-2048}

# DATA_SIZE may be a single value or a comma-separated list (like PIPELINE), e.g.
# DATA_SIZE=256,4096 runs the whole matrix once per size. Parse into DATA_SIZES; the
# scalar DATA_SIZE is then set to each value in turn by the main run loop, so every
# existing "$DATA_SIZE" reference keeps working unchanged. DATA_SIZE_RAW preserves the
# original list for the report header; DATA_SIZE_SUFFIX (set in the loop) is appended to
# mode names when more than one size runs so per-size results never collide in reports.
DATA_SIZE_RAW="$DATA_SIZE"
IFS=',' read -ra DATA_SIZES <<< "$DATA_SIZE"
DATA_SIZE_SUFFIX=""

# --- Keyspace bound (memtier --key-maximum) --------------------------------
# Caps the number of distinct keys so the dataset stops growing once they are all
# written. Without a bound, a write-only run (SET, --ratio=1:0) keeps inserting new
# keys until the server fills RAM and the box swaps/thrashes (or OOMs). Default 1M
# keys ~= 2 GB at the default 2 KB value size. Set to 0/off to use memtier's own
# default (10M, which can OOM with large values). Only applied to the script's
# default memtier commands - ignored when MEMTIER_ARGS is set.
KEY_MAXIMUM=${KEY_MAXIMUM:-1000000}
MEMTIER_KEYMAX_ARGS=()
if [[ -n "$KEY_MAXIMUM" && "$KEY_MAXIMUM" != "0" && "$KEY_MAXIMUM" != "off" ]]; then
    MEMTIER_KEYMAX_ARGS=(--key-maximum="$KEY_MAXIMUM")
fi

# --- Server memory safety net (--maxmemory) --------------------------------
# A hard cap passed to the server so a runaway write workload can never freeze the
# machine: at the limit Dragonfly rejects writes (and stays alive) instead of
# swapping. Defaults to ~70% of total RAM. Set SERVER_MAXMEMORY=off (or 0) to
# disable. Skipped if EXTRA_SERVER_FLAGS already sets --maxmemory (no duplicate).
if [[ -z "${SERVER_MAXMEMORY:-}" ]]; then
    _mem_kb=$(awk '/^MemTotal:/{print $2}' /proc/meminfo 2>/dev/null || echo 0)
    if [[ "${_mem_kb:-0}" -gt 0 ]]; then
        SERVER_MAXMEMORY="$(( _mem_kb * 7 / 10 / 1024 ))mb"
    else
        SERVER_MAXMEMORY="off"
    fi
fi
SERVER_MAXMEM_ARGS=()
if [[ -n "$SERVER_MAXMEMORY" && "$SERVER_MAXMEMORY" != "0" && "$SERVER_MAXMEMORY" != "off" ]]; then
    if [[ " ${EXTRA_SERVER_FLAGS:-} " == *"--maxmemory"* ]]; then
        echo "[*] SERVER_MAXMEMORY=${SERVER_MAXMEMORY} ignored: EXTRA_SERVER_FLAGS already sets --maxmemory."
    else
        SERVER_MAXMEM_ARGS=(--maxmemory "$SERVER_MAXMEMORY")
    fi
fi

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
for _ds in "${DATA_SIZES[@]}"; do
    if ! [[ "$_ds" =~ ^[0-9]+$ ]] || [[ "$_ds" -lt 1 ]]; then
        echo "[!] Error: DATA_SIZE must be a positive integer (bytes), or a comma-separated"
        echo "    list of positive integers (e.g. 256,4096). Got: '$DATA_SIZE_RAW'"
        exit 1
    fi
done

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

# BENCH_LOG_DIR (optional; also set automatically for each run started by --batch
# mode): a single directory that collects BOTH this run's console .log and the
# server log(s). When set, the .log is written there and the server log defaults
# there too (unless an explicit server_log_dir positional was given). This is how
# logs from every run survive a batch without overwriting each other.
if [[ -n "${BENCH_LOG_DIR:-}" ]]; then
    mkdir -p "$BENCH_LOG_DIR" || {
        echo "[!] Error: cannot create BENCH_LOG_DIR '${BENCH_LOG_DIR}'."
        exit 1
    }
    LOG_FILE="${BENCH_LOG_DIR%/}/${LOG_FILE}"
    # Server logs go under BENCH_LOG_DIR/server (unless an explicit server_log_dir
    # positional was given).
    if [[ -z "$SERVER_LOG_DIR" ]]; then
        SERVER_LOG_DIR="${BENCH_LOG_DIR%/}/server"
        mkdir -p "$SERVER_LOG_DIR" || {
            echo "[!] Error: cannot create server log dir '${SERVER_LOG_DIR}'."
            exit 1
        }
    fi
    # Metrics snapshots go under BENCH_LOG_DIR/metrics unless the caller already
    # set METRICS_DIR (or explicitly disabled it with METRICS_DIR=off).
    if [[ -z "${METRICS_DIR:-}" ]]; then
        METRICS_DIR="${BENCH_LOG_DIR%/}/metrics"
    fi
fi

ACCUM_FILE=$(mktemp)
STATS_FILE=$(mktemp)
SERVER_LOG=$(mktemp)

if [[ -n "$SERVER_LOG_DIR" && ! -d "$SERVER_LOG_DIR" ]]; then
    echo "[!] Error: server_log_dir '${SERVER_LOG_DIR}' does not exist. Create it first."
    exit 1
fi

# Cross-machine mode detection.
REMOTE_MODE=""
if [[ -n "$CLIENT_IP" ]]; then
    REMOTE_MODE=1
    if [[ "$SERVER_IP" == "127.0.0.1" ]]; then
        echo "[!] Error: CLIENT_IP is set but SERVER_IP is 127.0.0.1."
        echo "    The client can't reach the server. Set SERVER_IP to this machine's IP."
        exit 1
    fi
    if ! ssh -o ConnectTimeout=5 -o BatchMode=yes -o ControlMaster=no \
            "${SSH_USER}@${CLIENT_IP}" true 2>/dev/null; then
        echo "[!] Error: Cannot SSH to ${SSH_USER}@${CLIENT_IP} (BatchMode, no password prompt)."
        echo "    Fix: ssh-copy-id ${SSH_USER}@${CLIENT_IP}"
        exit 1
    fi
    echo "[*] Cross-machine mode: server=$(hostname), client=${SSH_USER}@${CLIENT_IP}"
    echo "    Client will target ${SERVER_IP}:${PORT}"
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
    # Which depths run is decided ONCE at startup by PIPELINE_FILTER (captured from the user's
    # PIPELINE env at the top of the script). We must NOT fall back to the bare $PIPELINE here:
    # run_bench/run_bench_custom use `for PIPELINE in ...` with PIPELINE as the (non-local) loop
    # variable, so after the first run PIPELINE is left set to the last depth (e.g. 100). Consulting
    # it would silently filter every subsequent run down to that single depth and wreck the
    # multi-run averages.
    local filter="${PIPELINE_FILTER:-}"
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
# Full server command string of the most recent start_server call. Printed in the
# AVERAGE RESULTS header so each batch run's summary records exactly how the server
# was launched (it is identical across all repeats of a single batch line).
SERVER_CMD_STR=""

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
                "${SSH_USER}@${CLIENT_IP}" \
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
    rm -f "$ACCUM_FILE" "$STATS_FILE" "$SERVER_LOG"
    echo "[*] Done."
}
trap cleanup EXIT INT TERM

NUM_CPUS=$(nproc)
MIN_CPUS=2  # 1 for server + 1 for client (local mode).

if [[ $NUM_CPUS -lt $MIN_CPUS ]]; then
    echo "[!] Error: Need at least $MIN_CPUS CPUs, this machine has $NUM_CPUS."
    exit 1
fi

# How many CPU cores does the server process actually use?
# Valkey: VALKEY_IO_THREADS. Dragonfly/ok_backend: PROACTOR_THREADS.
if [[ "$SERVER_TYPE" == "valkey" ]]; then
    _server_thread_count=$VALKEY_IO_THREADS
else
    _server_thread_count=$PROACTOR_THREADS
fi

if [[ -z "$REMOTE_MODE" ]]; then
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
    # Remote mode: the client runs on the remote machine, so all local cores are free
    # for the server. Pin the server anyway (steadier, less jitter) and, when a spare
    # core exists, leave core 0 for the OS and NIC (ENA) IRQs - pair this with IRQ
    # affinity pinning (see README). Controls:
    #   SERVER_PIN=off    - disable pinning entirely.
    #   SERVER_CPUS=1-3   - explicit core list (overrides the automatic selection).
    # To get a spare core for IRQs, pass threads < nproc (e.g. 3 on a 4-core box):
    # the server is then pinned to cores 1..threads with core 0 left free.
    CLIENT_TASKSET=""
    if [[ "${SERVER_PIN:-on}" == "off" ]]; then
        SERVER_TASKSET=""
    else
        if [[ -z "${SERVER_CPUS:-}" ]]; then
            if [[ $_server_thread_count -lt $NUM_CPUS ]]; then
                _pin_end=$_server_thread_count
                [[ $_pin_end -ge $NUM_CPUS ]] && _pin_end=$((NUM_CPUS - 1))
                SERVER_CPUS=$(seq -s ',' 1 ${_pin_end})        # cores 1..N, core 0 left free
            else
                SERVER_CPUS=$(seq -s ',' 0 $((NUM_CPUS - 1)))  # threads == cores: pin to all
            fi
        fi
        SERVER_TASKSET="taskset -c ${SERVER_CPUS}"
    fi
    REMOTE_CPUS=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${CLIENT_IP}" nproc 2>/dev/null || echo 0)
    if [[ $REMOTE_CPUS -lt $MIN_CPUS ]]; then
        echo "[!] Error: Client machine $CLIENT_IP has $REMOTE_CPUS CPUs, need at least $MIN_CPUS."
        exit 1
    fi
    echo "[*] Server CPUs: $NUM_CPUS (server pinned to: ${SERVER_CPUS:-none}), Client CPUs: $REMOTE_CPUS"
fi

# Preflight: verify required client tools exist on the machine that will run them.
check_client_tools() {
    local missing=()
    if [[ -n "$REMOTE_MODE" ]]; then
        ssh -n "${SSH_USER}@${CLIENT_IP}" "command -v memtier_benchmark" > /dev/null 2>&1 || missing+=("memtier_benchmark")
        ssh -n "${SSH_USER}@${CLIENT_IP}" "command -v redis-benchmark"   > /dev/null 2>&1 || missing+=("redis-benchmark")
        ssh -n "${SSH_USER}@${CLIENT_IP}" "command -v redis-cli"         > /dev/null 2>&1 || missing+=("redis-cli")
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
# Remote mode: runs via SSH on CLIENT_IP targeting SERVER_IP.
# ---------------------------------------------------------------------------
run_on_client() {
    local _timeout_cmd=""
    if [[ -n "${CLIENT_TIMEOUT:-}" ]]; then
        _timeout_cmd="timeout $CLIENT_TIMEOUT"
    fi
    if [[ -n "$REMOTE_MODE" ]]; then
        $_timeout_cmd ssh -n -o ControlMaster=no -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
            "${SSH_USER}@${CLIENT_IP}" "$(printf '%q ' "$@")"
    else
        $_timeout_cmd $CLIENT_TASKSET "$@"
    fi
}

# Target host for client tools: SERVER_IP in remote mode, 127.0.0.1 locally.
client_target() {
    if [[ -n "$REMOTE_MODE" ]]; then
        echo "$SERVER_IP"
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
    local _metrics_host; _metrics_host=${SERVER_IP:-127.0.0.1}
    raw=$(curl -s --max-time 2 "http://${_metrics_host}:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    # Both dragonfly and ok_backend expose 'dragonfly_reply_total'.
    metric_name='dragonfly_reply_total'
    val=$(echo "$raw" | tr -d '\r' | grep "^${metric_name}" | awk '{sum += $NF} END {print (sum ? sum : 0)}') || true
    echo "${val:-0}"
}

get_cmd_count() {
    # Returns total commands processed. Used to compute batch density accurately
    # without relying on -n counts. Works for both dragonfly and ok_backend
    # (ok_backend exposes commands_processed_total on its main port).
    if [[ "$SERVER_TYPE" != "dragonfly" ]]; then echo 0; return; fi
    local raw val metric_name
    local _metrics_host; _metrics_host=${SERVER_IP:-127.0.0.1}
    raw=$(curl -s --max-time 2 "http://${_metrics_host}:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    # Both dragonfly and ok_backend expose 'dragonfly_commands_processed_total'.
    metric_name='dragonfly_commands_processed_total'
    val=$(echo "$raw" | tr -d '\r' | grep "^${metric_name}" | awk '{sum += $NF} END {print (sum ? sum : 0)}') || true
    echo "${val:-0}"
}

get_replies_per_flush_raw() {
    # Returns "sum count" from the histogram for delta computation.
    local raw sum count
    local _metrics_host; _metrics_host=${SERVER_IP:-127.0.0.1}
    raw=$(curl -s --max-time 2 "http://${_metrics_host}:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
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
    local _metrics_host; _metrics_host=${SERVER_IP:-127.0.0.1}
    raw=$(curl -s --max-time 2 "http://${_metrics_host}:${SERVER_METRICS_PORT}/metrics" 2>/dev/null) || true
    # Both dragonfly and ok_backend expose 'dragonfly_reply_total'.
    sample=$(echo "$raw" | tr -d '\r' | grep '^dragonfly_reply_total') || true

    if [[ -z "$raw" ]]; then
        echo "[!] WARNING: /metrics endpoint unreachable. SEND_SYSCALLS will show 0."
    elif [[ -z "$sample" ]]; then
        echo "[!] WARNING: /metrics reachable but expected metric not found (binary: ${_bname})."
    fi
}

# ---------------------------------------------------------------------------
# save_metrics_snapshot <label> <mode_name> [pipeline]
# Fetches /metrics from the running server and writes it to METRICS_DIR.
# File name:
#   metrics_<tag>_d<size>_p<pipe>_<label>_<mode>_run<r>_<ts>.prom
# <pipe> is the SINGLE pipeline depth when the 3rd arg is given (per-depth snapshot,
# the default in the run loops), otherwise the PIPELINE_FILTER (commas→hyphens,
# empty→"all") for a blended snapshot. Snapshots are taken once per pipeline depth and
# the server's cumulative counters are reset between depths (see reset_server_stats), so
# each .prom belongs to exactly one (config, mode, pipeline, run) — never a blend.
# METRICS_DIR is auto-set to BENCH_LOG_DIR in batch mode; set METRICS_DIR=off
# to disable explicitly.
# ---------------------------------------------------------------------------
save_metrics_snapshot() {
    [[ -z "${METRICS_DIR:-}" || "${METRICS_DIR}" == "off" ]] && return
    local label=$1 mode_name=$2 pipe=${3:-}
    mkdir -p "$METRICS_DIR" 2>/dev/null || true
    local ts; ts=$(date +%Y%m%d_%H%M%S)
    local _pstr
    if [[ -n "$pipe" ]]; then
        _pstr="$pipe"                                   # single depth → unambiguous
    else
        _pstr=$(echo "${PIPELINE_FILTER:-all}" | tr ',' '-')
    fi
    local fname="${METRICS_DIR}/metrics_${TAG:-${GIT_SHA}}_d${DATA_SIZE}_p${_pstr}_${label}_${mode_name}_run${r:-1}_${ts}.prom"
    local _host; _host=${SERVER_IP:-127.0.0.1}
    local raw
    if raw=$(curl -sf --max-time 3 "http://${_host}:${SERVER_METRICS_PORT}/metrics" 2>/dev/null); then
        printf '%s\n' "$raw" > "$fname"
        echo "[*] Metrics snapshot saved: ${label} ${mode_name} pipeline=${pipe:-${_pstr}} run=${r:-1} -> ${fname}"
    else
        echo "[!] /metrics fetch from ${_host}:${SERVER_METRICS_PORT} failed — snapshot skipped (${label} ${mode_name} pipeline=${pipe:-${_pstr}} run=${r:-1})"
    fi
}

# ---------------------------------------------------------------------------
# reset_server_stats
# Resets the server's cumulative stat counters via CONFIG RESETSTAT so the NEXT
# per-pipeline metrics snapshot reflects only that pipeline depth, never a blend of all
# depths run against the same long-lived server process. Clears (among others)
# squash_batch_size_hist, proactor_reads, and the reply/command counters used for
# batch-density. (The pipeline_latency_seconds residence histogram is NOT cleared by
# RESETSTAT — it decays instead; client latency is taken from memtier, not from it.)
# No-op for non-dragonfly servers.
# ---------------------------------------------------------------------------
reset_server_stats() {
    [[ "$SERVER_TYPE" != "dragonfly" ]] && return
    redis-cli -p "$PORT" CONFIG RESETSTAT > /dev/null 2>&1 || true
}

# stop_server [label] [mode_name]
# Optional label and mode_name are used to build a descriptive server log filename.
# When omitted (e.g. from the cleanup trap) the filename falls back to generic fields.
stop_server() {
    local _sv_label=${1:-} _sv_mode=${2:-}
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
        local ts; ts=$(date +%Y%m%d_%H%M%S)
        local _pstr; _pstr=$(echo "${PIPELINE_FILTER:-all}" | tr ',' '-')
        # Build descriptive filename: tag_d<size>_p<pipes>_label_mode_ts.log
        local _parts="${TAG:-${GIT_SHA}}_d${DATA_SIZE}_p${_pstr}"
        [[ -n "$_sv_label" ]] && _parts+="_${_sv_label}"
        [[ -n "$_sv_mode"  ]] && _parts+="_${_sv_mode}"
        _parts+="_${ts}"
        local save_name="server_${_parts}.log"
        cp "$SERVER_LOG" "${SERVER_LOG_DIR}/${save_name}"
        echo "[*] Server log saved to ${SERVER_LOG_DIR}/${save_name}"
        : > "$SERVER_LOG"  # Truncate so cleanup doesn't duplicate.
    fi
    sleep 0.6 # let OS release the TCP port
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
            "${SERVER_MAXMEM_ARGS[@]}"
            --loglevel warning)
        # Append generic extra flags for valkey too
        if [[ -n "${EXTRA_SERVER_FLAGS:-}" ]]; then
            read -ra _vk_extra <<< "$EXTRA_SERVER_FLAGS"
            vk_cmd+=("${_vk_extra[@]}")
        fi
        SERVER_CMD_STR="${vk_cmd[*]}"
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
        "${SERVER_MAXMEM_ARGS[@]}"
        "${log_flags[@]}"
        "${extra_flags[@]}")
    SERVER_CMD_STR="${server_cmd[*]}"
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
    mode_name="${mode_name}${DATA_SIZE_SUFFIX:-}"  # per-size suffix so reports don't collide
    local pipelines=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do
        pipelines+=("$1"); shift
    done
    [[ "${1:-}" == "--" ]] && shift
    local extra_server_flags=("$@")

    _retry_filter_pipelines "$label" "$mode_name" || return 0

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
                "${MEMTIER_KEYMAX_ARGS[@]}"
                --key-prefix=bench
                --test-time=$BENCH_DURATION
                --hide-histogram)
        fi
        if [[ -n "$REMOTE_MODE" ]]; then
            echo "  [client cmd] ssh ${SSH_USER}@${CLIENT_IP} ${memtier_cmd[*]}"
        else
            echo "  [client cmd] $CLIENT_TASKSET ${memtier_cmd[*]}"
        fi

        local OUTPUT
        OUTPUT=$(run_on_client "${memtier_cmd[@]}" 2>&1) || true
        echo "  --- raw memtier output ---"
        echo "$OUTPUT"
        echo "  --- end raw output ---"
        capture_stats_block "$OUTPUT" "Pass ${r:-1}  ${label}  ${mode_name}  pipeline=${PIPELINE}"

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

        # Per-pipeline metrics: snapshot THIS depth, then reset counters so the next
        # depth's snapshot is clean (one .prom == one config/mode/pipeline/run).
        save_metrics_snapshot "$label" "$mode_name" "$PIPELINE"
        reset_server_stats
    done

    stop_server "$label" "$mode_name"

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
    mode_name="${mode_name}${DATA_SIZE_SUFFIX:-}"  # per-size suffix so reports don't collide
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

    _retry_filter_pipelines "$label" "$mode_name" || return 0

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
                "${MEMTIER_KEYMAX_ARGS[@]}"
                --test-time=$BENCH_DURATION
                --hide-histogram
                "${memtier_extra[@]}")
        fi
        if [[ -n "$REMOTE_MODE" ]]; then
            echo "  [client cmd] ssh ${SSH_USER}@${CLIENT_IP} ${memtier_cmd[*]}"
        else
            echo "  [client cmd] $CLIENT_TASKSET ${memtier_cmd[*]}"
        fi

        local OUTPUT
        OUTPUT=$(run_on_client "${memtier_cmd[@]}" 2>&1) || true
        echo "  --- raw memtier output ---"
        echo "$OUTPUT"
        echo "  --- end raw output ---"
        capture_stats_block "$OUTPUT" "Pass ${r:-1}  ${label}  ${mode_name}  pipeline=${PIPELINE}"

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

        # Per-pipeline metrics: snapshot THIS depth, then reset counters so the next
        # depth's snapshot is clean (one .prom == one config/mode/pipeline/run).
        save_metrics_snapshot "$label" "$mode_name" "$PIPELINE"
        reset_server_stats
    done

    stop_server "$label" "$mode_name"

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
    # Per-size suffix so multiple DATA_SIZE values don't collide in the report grouping.
    local mode_name="pubsub${DATA_SIZE_SUFFIX:-}"

    _retry_filter_pipelines "$label" "$mode_name" || return 0

    start_server "$v2_flag" "$label" "$mode_name"

    local target
    target=$(client_target)

    RESULTS_TMP=$(mktemp)
    echo -e "PIPELINE\tPUB_RPS\tP50(ms)\tSEND_SYSCALLS\tSUBSCRIBERS" > "$RESULTS_TMP"

    local msg_payload _msg_bytes=128
    # Honor an explicit DATA_SIZE for the pubsub payload; otherwise keep the
    # historical 128-byte message so existing pubsub runs are unchanged.
    [[ -n "$_DATA_SIZE_EXPLICIT" ]] && _msg_bytes=$DATA_SIZE
    msg_payload=$(printf "%${_msg_bytes}s" | tr ' ' 'X')

    for PIPELINE in "${pipelines[@]}"; do
        echo "  [+] pipeline=$PIPELINE, subscribers=$num_subs ..."

        # Launch subscribers (from client machine toward server).
        local sub_cmd_str
        if [[ -n "$REMOTE_MODE" ]]; then
            sub_cmd_str="redis-cli -h $SERVER_IP -p $PORT SUBSCRIBE bench_chan"
            echo "  [client sub cmd x${num_subs}] ssh ${SSH_USER}@${CLIENT_IP} ${sub_cmd_str}"
        else
            sub_cmd_str="$CLIENT_TASKSET redis-cli -p $PORT SUBSCRIBE bench_chan"
            echo "  [client sub cmd x${num_subs}] ${sub_cmd_str}"
        fi
        SUB_PIDS=()
        for ((s = 0; s < num_subs; s++)); do
            if [[ -n "$REMOTE_MODE" ]]; then
            ssh -n -o ControlMaster=no -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
                "${SSH_USER}@${CLIENT_IP}" \
                "redis-cli -h $SERVER_IP -p $PORT SUBSCRIBE bench_chan > /dev/null 2>&1" &
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
            local pub_cmd="timeout 60 redis-benchmark -h $SERVER_IP -p $PORT -n $num_msgs -P $PIPELINE ${_rb_opts[*]} publish bench_chan '$msg_payload'"
            echo "  [client pub cmd] ssh ${SSH_USER}@${CLIENT_IP} ${pub_cmd}"
            OUTPUT=$(ssh -o ControlMaster=no -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
                "${SSH_USER}@${CLIENT_IP}" "$pub_cmd" 2>&1) || true
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
        # redis-benchmark -q reports p50 on the final summary line: "X rps, p50=Y msec"
        PUB_P50=$(echo "$OUTPUT" | tr '\r' '\n' | grep -i 'requests per second' | tail -n 1 | grep -oP 'p50=\K[0-9]+\.[0-9]+' | head -n 1 || true)
        PUB_P50=${PUB_P50:-"N/A"}

        # Tear down subscribers.
        for pid in "${SUB_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done

        if [[ -n "$REMOTE_MODE" ]]; then
            # Kill all redis-cli on client. -n prevents ssh from stealing stdin.
            ssh -n -o ConnectTimeout=3 -o ControlMaster=no \
                "${SSH_USER}@${CLIENT_IP}" "pkill -9 redis-cli" 2>/dev/null || true
        fi

        # Watchdog: if SSH proxy processes hang, force-kill after 2s.
        ( sleep 2; kill -9 "${SUB_PIDS[@]}" 2>/dev/null || true ) &
        local watchdog_pid=$!

        wait "${SUB_PIDS[@]}" 2>/dev/null || true
        kill "$watchdog_pid" 2>/dev/null || true
        SUB_PIDS=()

        echo -e "${PIPELINE}\t${RPS}\t${PUB_P50}\t${SEND_DELTA}\t${num_subs}" >> "$RESULTS_TMP"
        echo "${label}|${mode_name}|${PIPELINE}|${RPS}|N/A|${SEND_DELTA}|N/A|${PUB_P50}|N/A|N/A" >> "$ACCUM_FILE"

        # Per-pipeline metrics: snapshot THIS depth, then reset counters so the next
        # depth's snapshot is clean (one .prom == one config/mode/pipeline/run).
        save_metrics_snapshot "$label" "$mode_name" "$PIPELINE"
        reset_server_stats
    done

    stop_server "$label" "$mode_name"

    echo ""
    echo "====================================================="
    printf "  %-4s  %s  (commit: %s)\n" "$label" "$mode_name" "$GIT_SHA"
    echo "====================================================="
    column -t -s $'\t' "$RESULTS_TMP"
    echo "====================================================="
    rm "$RESULTS_TMP"
}

# Append the memtier "ALL STATS" Totals block from $1 to STATS_FILE, tagged with header $2.
capture_stats_block() {
    local out=$1 hdr=$2 block
    block=$(printf '%s\n' "$out" | awk '
        /^=+ *$/ { sep = $0 }
        !done && /^Type[[:space:]]+Ops\/sec/ { print sep; show = 1 }
        show { print }
        show && /^Totals[[:space:]]/ { show = 0; done = 1 }
    ')
    [[ -n "$block" ]] || return 0
    { echo ""; echo "# ${hdr}"; printf '%s\n' "$block"; } >> "$STATS_FILE"
}

# Print every captured per-run Totals block, in run order, at the very end.
print_all_stats() {
    [[ -s "$STATS_FILE" ]] || return 0
    echo ""
    echo "######################################################"
    echo "  PER-RUN MEMTIER TOTALS (every run, in order)"
    echo "######################################################"
    cat "$STATS_FILE"
}

# --- Adaptive per-cell retry state -----------------------------------------
# CELL_RETRY maps "label|mode_name|pipeline" -> 1 for every cell that still needs another sample.
# RETRY_MODE=1 switches run_bench/run_bench_custom/run_pubsub_bench into "only re-run those cells".
declare -A CELL_RETRY
RETRY_MODE=0

# Targeted-retry filter. Called from run_bench/run_bench_custom/run_pubsub_bench (bash dynamic
# scope lets it see and trim the caller's local `pipelines` array). When RETRY_MODE=1 it keeps only
# the depths whose cell is still in CELL_RETRY, and returns non-zero when none remain so the caller
# can `|| return 0` and skip starting a server for an all-trusted config. No-op outside retry mode.
_retry_filter_pipelines() {
    local label="$1" mode_name="$2"
    [[ "$RETRY_MODE" == "1" ]] || return 0
    local kept=() p
    for p in "${pipelines[@]}"; do
        [[ -n "${CELL_RETRY["${label}|${mode_name}|${p}"]:-}" ]] && kept+=("$p")
    done
    pipelines=("${kept[@]}")
    (( ${#pipelines[@]} > 0 ))
}

# Recompute per-cell trusted-group status from ACCUM_FILE and refill CELL_RETRY with the cells that
# still need another sample: NOT trusted AND fewer than MAX_RUNS attempts so far. A cell is TRUSTED
# when some subset of >= MIN_RUNS positive RPS samples all fall within MAX_RESULT_VARIANCE_PCT of
# each other (the same tightest-window rule the final report averages). When CELL_RETRY ends up
# empty, every cell is either trusted or capped, and the retry loop stops.
_recompute_cell_status() {
    CELL_RETRY=()
    [[ -s "$ACCUM_FILE" ]] || return 0
    local lbl mod pip cnt trusted
    while IFS='|' read -r lbl mod pip cnt trusted; do
        [[ "$trusted" == "1" ]] && continue
        (( cnt >= MAX_RUNS )) && continue
        CELL_RETRY["${lbl}|${mod}|${pip}"]=1
    done < <(awk -F'|' -v thr="$MAX_RESULT_VARIANCE_PCT" -v minn="$MIN_RUNS" '
    {
        key = $1 SUBSEP $2 SUBSEP $3
        if (!(key in seen)) { seen[key]=1; order[++n]=key; lbl[key]=$1; mod[key]=$2; pip[key]=$3; kc[key]=0 }
        i = ++kc[key]; rps[key,i] = $4 + 0
    }
    END {
        for (idx = 1; idx <= n; idx++) {
            k = order[idx]; m = kc[k]; np = 0
            for (i = 1; i <= m; i++) if (rps[k,i] > 0) so[++np] = i
            for (a = 1; a <= np; a++) for (b = a+1; b <= np; b++)
                if (rps[k,so[b]] < rps[k,so[a]]) { t = so[a]; so[a] = so[b]; so[b] = t }
            tr = 0
            for (ksz = np; ksz >= minn && !tr; ksz--)
                for (s = 1; s + ksz - 1 <= np; s++) {
                    e = s + ksz - 1; lo = rps[k,so[s]]; hi = rps[k,so[e]]
                    if (lo > 0 && (hi - lo) / lo * 100.0 <= thr) { tr = 1; break }
                }
            printf "%s|%s|%s|%d|%d\n", lbl[k], mod[k], pip[k], m, tr
        }
    }' "$ACCUM_FILE")
}

# Untrusted-sample report (WARN ONLY, never fails the run). For each (version, mode, pipeline)
# cell it determines the tightest trusted group (>= MIN_RUNS positive samples within
# MAX_RESULT_VARIANCE_PCT) that the AVERAGE table used, then:
#   * lists the samples that were TRIMMED (kept around but excluded from the average), and
#   * flags any cell that NEVER formed a trusted group even at the MAX_RUNS cap (the real warning).
report_untrusted_samples() {
    [[ $RUNS -le 1 ]] && return 0
    [[ -s "$ACCUM_FILE" ]] || return 0
    awk -F'|' -v thr="$MAX_RESULT_VARIANCE_PCT" -v minn="$MIN_RUNS" '
    {
        key = $1 SUBSEP $2 SUBSEP $3
        if (!(key in seen)) { seen[key]=1; order[++n]=key; lbl[key]=$1; mod[key]=$2; pip[key]=$3; kc[key]=0 }
        i = ++kc[key]; rps[key,i] = $4 + 0
    }
    END {
        warn_hdr = 0; trim_hdr = 0
        for (idx = 1; idx <= n; idx++) {
            k = order[idx]; m = kc[k]
            np = 0
            for (i = 1; i <= m; i++) if (rps[k,i] > 0) ord[++np] = rps[k,i]
            for (a = 1; a <= np; a++) for (b = a+1; b <= np; b++)
                if (ord[b] < ord[a]) { t = ord[a]; ord[a] = ord[b]; ord[b] = t }
            errs = m - np
            # tightest largest trusted window over sorted positive samples
            lo = 1; hi = 0; found = 0
            for (ksz = np; ksz >= minn && !found; ksz--) {
                best = -1
                for (s = 1; s + ksz - 1 <= np; s++) {
                    e = s + ksz - 1
                    if (ord[s] > 0 && (ord[e]-ord[s])/ord[s]*100.0 <= thr && (best < 0 || (ord[e]-ord[s]) < best)) {
                        best = ord[e]-ord[s]; lo = s; hi = e; found = 1
                    }
                }
            }
            if (!found) {
                if (!warn_hdr) {
                    print ""
                    print "======================================================"
                    printf "  [!] CELLS WITHOUT A TRUSTED GROUP (no %d samples within %.1f%% even at the cap)\n", minn, thr
                    print "      The AVERAGE row for these used all positive samples - treat as unreliable."
                    print "======================================================"
                    warn_hdr = 1
                }
                printf "  %-6s %-18s p%-5s samples:", lbl[k], mod[k], pip[k]
                for (j = 1; j <= np; j++) printf " %.0f", ord[j]
                if (errs > 0) printf "  (+%d errored)", errs
                print ""
            } else {
                dropped = ""
                for (j = 1; j <= np; j++) if (j < lo || j > hi) dropped = dropped sprintf(" %.0f", ord[j])
                if (dropped != "" || errs > 0) {
                    if (!trim_hdr) {
                        print ""
                        print "------------------------------------------------------"
                        print "  Trimmed samples (kept in the log, excluded from the trusted-group average):"
                        print "------------------------------------------------------"
                        trim_hdr = 1
                    }
                    printf "  %-6s %-18s p%-5s dropped:%s", lbl[k], mod[k], pip[k], dropped
                    if (errs > 0) printf "  (+%d errored)", errs
                    print ""
                }
            }
        }
        if (warn_hdr) print "======================================================"
        exit 0
    }
    ' "$ACCUM_FILE"
}

print_final_report() {
    [[ $RUNS -le 1 ]] && return
    echo ""
    echo "######################################################################"
    if [[ -n "${BATCH_RUN_INDEX:-}" ]]; then
        echo "  AVERAGE RESULTS  ::  Run ${BATCH_RUN_INDEX}/${BATCH_RUN_TOTAL}  ::  tag=${TAG:-<none>}"
    else
        echo "  AVERAGE RESULTS  ::  tag=${TAG:-<none>}"
    fi
    echo "######################################################################"
    echo "  commit=${GIT_SHA}  server_type=${SERVER_TYPE}  ver=${VER}  mode=${MODE}  runs=${RUNS}  threads=${PROACTOR_THREADS}"
    echo "  data_size=${DATA_SIZE_RAW}B  pipelines=${PIPELINE_FILTER:-1,10,50,100}  cmd=${CMD}"
    [[ -n "${EXTRA_SERVER_FLAGS:-}" ]] && echo "  extra_server_flags=${EXTRA_SERVER_FLAGS}"
    echo "  server_cmd: ${SERVER_CMD_STR:-<unknown>}"
    echo "######################################################################"
    awk -F'|' -v thr="$MAX_RESULT_VARIANCE_PCT" -v minn="$MIN_RUNS" '
    {
        key = $1 SUBSEP $2 SUBSEP $3
        if (!(key in seen)) {
            seen[key] = 1
            order[++n] = key
            lbl[key] = $1; mod[key] = $2; pip[key] = $3
            kc[key] = 0
        }
        i = ++kc[key]
        rps[key,i]=$4+0; lat[key,i]=$5+0; sys[key,i]=$6+0; bd[key,i]=$7+0
        p50[key,i]=$8+0; p99[key,i]=$9+0; p999[key,i]=$10+0
    }
    END {
        prev_group = ""
        for (idx = 1; idx <= n; idx++) {
            k = order[idx]
            m = kc[k]

            # Collect positive-RPS sample indices and sort them ascending by RPS.
            np = 0
            for (i = 1; i <= m; i++) if (rps[k,i] > 0) so[++np] = i
            for (a = 1; a <= np; a++)
                for (b = a+1; b <= np; b++)
                    if (rps[k,so[b]] < rps[k,so[a]]) { t = so[a]; so[a] = so[b]; so[b] = t }

            # Choose the LARGEST contiguous window (size >= minn) over the sorted RPS values whose
            # spread is within threshold; among windows of that size, take the tightest. This is
            # the "find the group of MIN..N runs that fall close enough to each other" rule. If no
            # window of size >= minn qualifies, fall back to averaging all positive samples.
            lo = 1; hi = np; found = 0
            for (ksz = np; ksz >= minn && !found; ksz--) {
                bestsp = -1
                for (s = 1; s + ksz - 1 <= np; s++) {
                    e = s + ksz - 1
                    lo_rps = rps[k, so[s]]; hi_rps = rps[k, so[e]]
                    sp = (lo_rps > 0) ? (hi_rps - lo_rps) / lo_rps * 100.0 : 1e9
                    if (sp <= thr && (bestsp < 0 || sp < bestsp)) { bestsp = sp; lo = s; hi = e; found = 1 }
                }
            }
            if (!found) { lo = 1; hi = np }

            # Average every metric over the selected window (samples so[lo..hi]); each metric only
            # counts its own positive values, matching the previous behavior.
            rps_s=0; rps_c=0; lat_s=0; lat_c=0; sys_s=0; sys_c=0; bd_s=0; bd_c=0
            p50_s=0; p50_c=0; p99_s=0; p99_c=0; p999_s=0; p999_c=0
            for (s = lo; s <= hi; s++) {
                i = so[s]
                rps_s += rps[k,i]; rps_c++
                if (lat[k,i]  > 0) { lat_s  += lat[k,i];  lat_c++ }
                sys_s += sys[k,i]; sys_c++
                if (bd[k,i]   > 0) { bd_s   += bd[k,i];   bd_c++ }
                if (p50[k,i]  > 0) { p50_s  += p50[k,i];  p50_c++ }
                if (p99[k,i]  > 0) { p99_s  += p99[k,i];  p99_c++ }
                if (p999[k,i] > 0) { p999_s += p999[k,i]; p999_c++ }
            }

            g = lbl[k] SUBSEP mod[k]
            if (g != prev_group) {
                if (prev_group != "") print "====================================================="
                print ""
                printf "  %-4s  %s\n", lbl[k], mod[k]
                print "====================================================="
                print "PIPELINE\tRPS\tAVG_LAT(ms)\tP50(ms)\tP99(ms)\tP99.9(ms)\tSEND_SYSCALLS\tBATCH_DENSITY\tRUNS_USED"
                prev_group = g
            }
            rps_avg = (rps_c > 0) ? rps_s / rps_c : 0
            lat_str = (lat_c > 0) ? sprintf("%.5f", lat_s / lat_c) : "N/A"
            bd_avg  = (bd_c  > 0) ? bd_s / bd_c   : 0
            p50_str = (p50_c > 0) ? sprintf("%.5f", p50_s / p50_c) : "N/A"
            p99_str = (p99_c > 0) ? sprintf("%.5f", p99_s / p99_c) : "N/A"
            p999_str= (p999_c > 0)? sprintf("%.5f", p999_s / p999_c) : "N/A"
            sys_avg = (sys_c > 0) ? sys_s / sys_c : 0
            runs_str = sprintf("%d/%d", rps_c, m)
            printf "%s\t%.2f\t%s\t%s\t%s\t%s\t%.0f\t%.1f\t%s\n", pip[k], rps_avg, lat_str, p50_str, p99_str, p999_str, sys_avg, bd_avg, runs_str
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
            run_bench false "$SERVER_TYPE" 2 25 $DATA_SIZE "multi_conn" "${pipelines[@]}"
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
            run_bench false "V1" 2 25 $DATA_SIZE "multi_conn" "${pipelines[@]}"
        fi
        if [[ "$VER" != "v1" ]]; then
            _metrics_verified=0
            run_bench true  "V2" 2 25 $DATA_SIZE "multi_conn" "${pipelines[@]}"
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
            run_bench false "$SERVER_TYPE" 1 1 $DATA_SIZE "single_conn" "${pipelines[@]}"
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
            run_bench false "V1" 1 1 $DATA_SIZE "single_conn" "${pipelines[@]}"
        fi
        if [[ "$VER" != "v1" ]]; then
            _metrics_verified=0
            run_bench true  "V2" 1 1 $DATA_SIZE "single_conn" "${pipelines[@]}"
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
        conn)          run_multi_conn; run_single_conn ;;
        all)           run_multi_conn; run_single_conn; run_pubsub ;;
    esac
}

case "$MODE" in
    multi_conn|single_conn|pubsub|conn|all)
        {
            r=0
            # Phase 1: RUNS baseline full-matrix passes - one sample per cell.
            while (( r < RUNS )); do
                r=$((r + 1))
                [[ $RUNS -gt 1 ]] && echo "" && echo "############## Pass $r / $RUNS (baseline) ##############"
                for _ds in "${DATA_SIZES[@]}"; do
                    DATA_SIZE="$_ds"
                    if [[ ${#DATA_SIZES[@]} -gt 1 ]]; then
                        DATA_SIZE_SUFFIX="_d${_ds}"
                        echo "" && echo "------------- data_size=${_ds}B -------------"
                    else
                        DATA_SIZE_SUFFIX=""
                    fi
                    run_selected_modes
                done
            done

            # Phase 2: targeted retries. Re-run ONLY the cells that still lack a trusted group
            # (>= MIN_RUNS samples within MAX_RESULT_VARIANCE_PCT), one extra sample per round,
            # reusing all prior samples, until every cell is trusted or hits MAX_RUNS samples.
            # Cells that already agree are never re-run - that is where the time is saved.
            if (( ADAPTIVE )); then
                RETRY_MODE=1
                _recompute_cell_status
                while (( ${#CELL_RETRY[@]} > 0 )); do
                    r=$((r + 1))
                    echo ""
                    echo "############## Pass $r (retry: ${#CELL_RETRY[@]} untrusted cell(s), cap=${MAX_RUNS}) ##############"
                    for _ds in "${DATA_SIZES[@]}"; do
                        DATA_SIZE="$_ds"
                        if [[ ${#DATA_SIZES[@]} -gt 1 ]]; then
                            DATA_SIZE_SUFFIX="_d${_ds}"
                        else
                            DATA_SIZE_SUFFIX=""
                        fi
                        run_selected_modes
                    done
                    _recompute_cell_status
                done
                RETRY_MODE=0
            fi

            print_final_report
            print_all_stats
        } 2>&1 | tee "$LOG_FILE"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Valid: multi_conn | single_conn | pubsub | conn | all"
        exit 1
        ;;
esac

# Untrusted-sample report: WARN ONLY. Lists the samples trimmed from each cell's average and
# flags any cell that never formed a trusted group (>= MIN_RUNS within MAX_RESULT_VARIANCE_PCT)
# even at the MAX_RUNS cap. This NEVER stops or fails the run - it is purely informational.
untrusted_report=$(report_untrusted_samples)
if [[ -n "$untrusted_report" ]]; then
    printf '%s\n' "$untrusted_report" | tee -a "$LOG_FILE"
fi

echo ""
echo "[*] Benchmark results saved to: $LOG_FILE"
