#!/bin/bash
set -e
set +m  # Disable monitor mode to suppress "Killed" messages

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

CONFIG_FILE="bench.conf"
BASE_RESULTS_DIR="results"
CURRENT_DOCKER=""
REMOTE_DOCKER=""
REMOTE_SERVER_PID=""

get_ip() {
    if [[ "$1" == "local" ]]; then echo "127.0.0.1"; else echo "${1#*@}"; fi
}

show_help() {
    echo -e "${GREEN}Dragonfly Benchmark Orchestrator${NC}"
    echo "Usage: ./df_bench_runner.sh [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands & Options:"
    echo "  run      Execute the benchmark matrix defined in the config file."
    echo "           -c, --config <file>  Specify a custom config file"
    echo "           -o, --output <path>  Specify a custom output directory"
    echo ""
    echo "  report   Parse a results directory and generate a CSV summary."
    echo "           -d, --dir <path>     Specify a directory to parse."
    echo ""
    echo "  help     Show this help message."
}

load_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        echo -e "${RED}Error: Configuration file '$CONFIG_FILE' not found.${NC}"
        exit 1
    fi

    # Validate syntax without executing
    if ! bash -n "$CONFIG_FILE"; then
        echo -e "${RED}Error: Syntax error in $CONFIG_FILE.${NC}"
        exit 1
    fi
    source "$CONFIG_FILE"
}

wait_for_port() {
    local host=$1
    local port=$2
    local state=$3
    local timeout=15
    local i=0

    while [ $i -lt $timeout ]; do
        if [[ "$state" == "open" ]]; then
            if nc -z -w1 "$host" "$port" 2>/dev/null; then return 0; fi
        else
            if ! nc -z -w1 "$host" "$port" 2>/dev/null; then return 0; fi
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

cleanup_server() {
    if [[ -n "${TOP_PID:-}" ]]; then
        disown $TOP_PID 2>/dev/null || true
        kill -9 "$TOP_PID" 2>/dev/null || true
    fi
    if [[ -n "${SERVER_PID:-}" ]]; then
        disown $SERVER_PID 2>/dev/null || true
        kill -9 "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "${CURRENT_DOCKER:-}" ]]; then
        docker rm -f "$CURRENT_DOCKER" > /dev/null 2>&1 || true
    fi
    if [[ -n "${REMOTE_SERVER_PID:-}" ]]; then
        ssh "$SERVER_CONNECTION" "sudo -n kill -9 $REMOTE_SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "${REMOTE_DOCKER:-}" ]]; then
        ssh "$SERVER_CONNECTION" "sudo -n docker rm -f $REMOTE_DOCKER" > /dev/null 2>&1 || true
    fi

    CURRENT_DOCKER=""
    REMOTE_SERVER_PID=""
    REMOTE_DOCKER=""
}

cmd_run() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--config) CONFIG_FILE="$2"; shift 2 ;;
            -o|--output) CUSTOM_OUTPUT_DIR="$2"; shift 2 ;;
            *) shift ;;
        esac
    done

    load_config

    local server_ip=$(get_ip "$SERVER_CONNECTION")
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local desc="${RUN_DESC:-"benchmark"}"
    local base_dir="${CUSTOM_OUTPUT_DIR:-$BASE_RESULTS_DIR}"
    local master_dir="${base_dir}/${desc}_${timestamp}"
    local total_runs=$((${#RATIOS[@]} * ${#PIPELINES[@]} * ${#TARGETS[@]}))
    local current_run=1

    mkdir -p "$master_dir"
    echo -e "${GREEN}Starting Benchmark Matrix ($total_runs total runs)${NC}"
    echo "Saving artifacts to: $master_dir"

    for ratio in "${RATIOS[@]}"; do
        for pipe in "${PIPELINES[@]}"; do
            for target in "${TARGETS[@]}"; do

                IFS='|' read -r t_alias t_proto t_port t_loc t_env t_path t_args <<< "$target"
                t_alias=$(echo "$t_alias" | xargs); t_proto=$(echo "$t_proto" | xargs)
                t_port=$(echo "$t_port" | xargs); t_loc=$(echo "$t_loc" | xargs | tr '[:upper:]' '[:lower:]')
                t_env=$(echo "$t_env" | xargs | tr '[:upper:]' '[:lower:]')
                t_path=$(echo "$t_path" | xargs); t_args=$(echo "$t_args" | xargs)

                local run_name="${t_alias}_ratio${ratio/:/_}_pipe${pipe}"
                local out_dir="$master_dir/$run_name"
                mkdir -p "$out_dir"

                echo -e "\n${YELLOW}--- [Run $current_run/$total_runs] $t_alias | Loc: $t_loc | Env: $t_env | Ratio: $ratio | Pipe: $pipe ---${NC}"

                local connect_ip="$server_ip"
                if [[ "$t_loc" == "local" ]]; then
                    connect_ip="127.0.0.1"
                elif [[ "$SERVER_CONNECTION" == "$CLIENT_CONNECTION" ]]; then
                    connect_ip="127.0.0.1"
                fi

                # --- SERVER STARTUP ---
                if [[ "$t_loc" == "local" ]]; then
                    fuser -k -n tcp "${t_port}" >/dev/null 2>&1 || true
                    wait_for_port "127.0.0.1" "$t_port" "closed" || echo "Warning: Local port $t_port still open!"

                    if [[ "$t_env" == "raw" ]]; then
                        bash -c "exec ${t_path/#\~/$HOME} $t_args" > "$out_dir/server.log" 2>&1 &
                        SERVER_PID=$!
                        wait_for_port "127.0.0.1" "$t_port" "open" || { echo -e "${RED}Local start failed${NC}"; exit 1; }
                        top -b -d 5 -p $SERVER_PID > "$out_dir/resources.log" &
                        TOP_PID=$!
                        echo -e "${GREEN}Local server started (PID: $SERVER_PID)${NC}"
                    elif [[ "$t_env" == "docker" ]]; then
                        docker rm -f "$t_alias" 2>/dev/null || true

                        if ! docker run -d --rm --name "$t_alias" --network host "$t_path" $t_args >/dev/null 2>&1; then
                            echo -e "${RED}ERROR: Local docker run failed.${NC}"
                            exit 1
                        fi

                        CURRENT_DOCKER="$t_alias"
                        wait_for_port "127.0.0.1" "$t_port" "open" || { echo -e "${RED}Local Docker failed to open port${NC}"; exit 1; }
                        SERVER_PID=$(docker inspect --format '{{.State.Pid}}' "$t_alias" 2>/dev/null || echo "")
                        docker logs -f "$t_alias" > "$out_dir/server.log" 2>&1 &
                        if [[ -n "$SERVER_PID" ]]; then
                            top -b -d 5 -p $SERVER_PID > "$out_dir/resources.log" &
                            TOP_PID=$!
                        fi
                        echo -e "${GREEN}Local Docker started successfully${NC}"
                    fi
               elif [[ "$t_loc" == "remote" ]]; then
                    echo "Preparing remote port $t_port on $SERVER_CONNECTION..."
                    ssh "$SERVER_CONNECTION" "sudo -n fuser -k -n tcp ${t_port} >/dev/null 2>&1" || true
                    ssh "$SERVER_CONNECTION" "timeout 10 bash -c 'until ! nc -z localhost $t_port; do sleep 1; done'" || echo "Warning: Port closure check timed out"

                    if [[ "$t_env" == "raw" ]]; then
                        echo "Verifying remote binary: $t_path"
                        if ! ssh "$SERVER_CONNECTION" "[ -f \"$t_path\" ] && [ -x \"$t_path\" ]"; then
                            echo -e "${RED}ERROR: Binary not found or not executable on remote: $t_path${NC}"
                            exit 1
                        fi

                        echo "Starting remote raw server..."
                        REMOTE_SERVER_PID=$(ssh "$SERVER_CONNECTION" "nohup ${t_path} ${t_args} > /tmp/server_start.log 2>&1 </dev/null & echo \$!")

                        if ! ssh "$SERVER_CONNECTION" "timeout 15 bash -c 'until nc -z localhost $t_port; do sleep 1; done'"; then
                            echo -e "${RED}Remote raw start failed! Remote log output:${NC}"
                            ssh "$SERVER_CONNECTION" "cat /tmp/server_start.log"
                            exit 1
                        fi
                        echo -e "${GREEN}Remote server started (PID: $REMOTE_SERVER_PID)${NC}"

                    elif [[ "$t_env" == "docker" ]]; then
                        echo "Starting remote docker server..."
                        ssh "$SERVER_CONNECTION" "sudo -n docker rm -f $t_alias 2>/dev/null" || true

                        if ! ssh "$SERVER_CONNECTION" "sudo -n docker run -d --rm --name $t_alias --network host $t_path $t_args >/dev/null 2>&1"; then
                            echo -e "${RED}ERROR: Remote docker run failed. Check if image exists and docker is running.${NC}"
                            exit 1
                        fi

                        REMOTE_DOCKER="$t_alias"
                        ssh "$SERVER_CONNECTION" "timeout 15 bash -c 'until nc -z localhost $t_port; do sleep 1; done'" || { echo -e "${RED}Remote Docker failed to open port $t_port${NC}"; exit 1; }
                        echo -e "${GREEN}Remote Docker started successfully${NC}"
                    fi
                fi

                # --- CLIENT DEPENDENCY CHECK ---
                if [[ "$CLIENT_CONNECTION" == "local" ]]; then
                    if ! command -v memtier_benchmark >/dev/null 2>&1; then
                        echo -e "${RED}ERROR: memtier_benchmark is not installed locally or not in PATH.${NC}"
                        cleanup_server
                        exit 1
                    fi
                else
                    if ! ssh "$CLIENT_CONNECTION" "command -v memtier_benchmark >/dev/null 2>&1"; then
                        echo -e "${RED}ERROR: memtier_benchmark is not installed on remote client ($CLIENT_CONNECTION).${NC}"
                        cleanup_server
                        exit 1
                    fi
                fi

                # --- CONNECTIVITY CHECK ---
                echo "Verifying connectivity from Client to Server ($connect_ip:$t_port)..."
                # -z: scan mode, -v: verbose, -w 3: 3 second timeout
                local check_cmd="nc -zv -w 3 $connect_ip $t_port"

                if [[ "$CLIENT_CONNECTION" == "local" ]]; then
                    if ! bash -c "$check_cmd" >/dev/null 2>&1; then
                        echo -e "${RED}ERROR: Cannot connect to $connect_ip:$t_port locally. Is the server running?${NC}"
                        cleanup_server
                        exit 1
                    fi
                else
                    if ! ssh "$CLIENT_CONNECTION" "$check_cmd" >/dev/null 2>&1; then
                        echo -e "${RED}ERROR: Remote Client ($CLIENT_CONNECTION) cannot reach Server at $connect_ip:$t_port.${NC}"
                        echo -e "${YELLOW}Tip: Check your AWS Security Group to ensure port $t_port is open for the client's IP.${NC}"
                        cleanup_server
                        exit 1
                    fi
                fi
                echo -e "${GREEN}Connectivity verified!${NC}"

                # --- CLIENT EXECUTION ---
                local client_cmd="memtier_benchmark -s $connect_ip -p $t_port -P $t_proto --ratio=$ratio --pipeline=$pipe \
                  -c $CLIENT_CONNS -t $CLIENT_THREADS --test-time=$TEST_TIME --key-maximum=$KEY_MAX"

                local memtier_status=0
                if [[ "$CLIENT_CONNECTION" == "local" ]]; then
                    if [[ -n "${CLIENT_TASKSET:-}" ]]; then
                        client_cmd="taskset -c $CLIENT_TASKSET $client_cmd"
                    fi
                    echo "Running load generator locally..."
                    set +e; set -o pipefail
                    stdbuf -o0 bash -c "$client_cmd" 2>&1 | stdbuf -o0 tee "$out_dir/memtier.log" | stdbuf -o0 tr '\r' '\n' | grep --line-buffered "^\[" | stdbuf -o0 tr '\n' '\r'
                    memtier_status=${PIPESTATUS[0]}
                    echo "" # Print a final newline
                    set +o pipefail; set -e
                else
                    echo "Executing load generator remotely on $CLIENT_CONNECTION..."
                    set +e; set -o pipefail
                    ssh -t "$CLIENT_CONNECTION" "$client_cmd" 2>&1 | stdbuf -o0 tee "$out_dir/memtier.log" | stdbuf -o0 tr '\r' '\n' | grep --line-buffered "^\[" | stdbuf -o0 tr '\n' '\r'
                    memtier_status=${PIPESTATUS[0]}
                    echo "" # Print a final newline
                    set +o pipefail; set -e
                fi

                # Abort if the benchmark failed
                if [[ $memtier_status -ne 0 ]]; then
                    echo -e "${RED}ERROR: memtier_benchmark failed with exit code $memtier_status.${NC}"
                    echo "memtier_status=$memtier_status" > "$out_dir/FAILED"
                    cleanup_server
                    exit 1
                fi


                # --- METADATA & CLEANUP ---
                echo "PROTO=\"$t_proto\"" > "$out_dir/meta.env"
                echo "ENGINE=\"$t_alias\"" >> "$out_dir/meta.env"
                echo "RATIO=\"$ratio\"" >> "$out_dir/meta.env"
                echo "PIPE=\"$pipe\"" >> "$out_dir/meta.env"

                cleanup_server
                current_run=$((current_run + 1))
                sleep 1
            done
        done
    done
    echo -e "\n${GREEN}All benchmarks completed! Results in: $master_dir${NC}"
}

cmd_report() {
    local target_dir=""
    while [[ $# -gt 0 ]]; do
        case $1 in -d|--dir) target_dir="$2"; shift 2 ;; *) shift ;; esac
    done

    if [ -z "$target_dir" ]; then
        target_dir=$(ls -td ${BASE_RESULTS_DIR}/*/ 2>/dev/null | head -1)
    fi
    # Handle empty directory
    if [[ -z "$target_dir" ]]; then
        echo -e "${RED}ERROR: No results directory found. Run benchmarks first.${NC}"
        exit 1
    fi
    if [[ "${target_dir}" != */ ]]; then
        target_dir="${target_dir}/"
    fi

    local csv_file="${target_dir}summary.csv"

    echo "Protocol,Engine,Ratio,Pipeline,Ops_sec,KB_sec,Avg_Latency,p50_Latency,p99_Latency,p99.9_Latency" > "$csv_file"
    local temp_file=$(mktemp)
    local count=0

    set +e
    for d in "$target_dir"*/; do
        if [[ ! -f "${d}memtier.log" || ! -f "${d}meta.env" ]]; then continue; fi
        source "${d}meta.env"

        local line=$(grep "^Totals" "${d}memtier.log" | awk '{print $2","$9","$5","$6","$7","$8}' || true)
        if [[ -n "$line" ]]; then
            local print_proto="RESP"
            if [[ "$PROTO" == *"memcache"* ]]; then
                print_proto="Memcached"
            fi
            echo "$print_proto,$ENGINE,$RATIO,$PIPE,$line" >> "$temp_file"
            count=$((count + 1))
        fi
    done
    set -e

    if [ -s "$temp_file" ]; then
        sort -t, -k4,4n -k3,3 -k1,1 "$temp_file" >> "$csv_file"
    fi
    rm -f "$temp_file"

    echo -e "\n=========================================== BENCHMARK RESULTS ==========================================="
    column -s, -t < "$csv_file" || true
    echo -e "=========================================================================================================\n"
    echo -e "${GREEN}Report saved: $csv_file ($count runs)${NC}"
}

COMMAND=${1:-"help"}
shift || true
trap cleanup_server EXIT INT TERM
case "$COMMAND" in run) cmd_run "$@" ;; report) cmd_report "$@" ;; *) show_help ;; esac
