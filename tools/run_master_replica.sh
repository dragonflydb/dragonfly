#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DFLY_BIN="${DFLY_BIN:-${ROOT_DIR}/build-dbg/dragonfly}"
MASTER_PORT="${MASTER_PORT:-6379}"
REPLICA_PORT="${REPLICA_PORT:-6380}"

if [[ ! -x "${DFLY_BIN}" ]]; then
    echo "Dragonfly binary not found. Build it first (e.g., cd build-dbg && ninja dragonfly)." >&2
    exit 1
fi

MASTER_DIR="$(mktemp -d -t dfly-master-XXXXXX)"
REPLICA_DIR="$(mktemp -d -t dfly-replica-XXXXXX)"
MASTER_LOG_DIR="$(mktemp -d -t dfly-master-logs-XXXXXX)"
REPLICA_LOG_DIR="$(mktemp -d -t dfly-replica-logs-XXXXXX)"

cleanup() {
  if [[ -n "${REPLICA_PID:-}" ]]; then
    kill "${REPLICA_PID}" 2>/dev/null || true
  fi
  if [[ -n "${MASTER_PID:-}" ]]; then
    kill "${MASTER_PID}" 2>/dev/null || true
  fi
  rm -rf "${MASTER_DIR}" "${REPLICA_DIR}" "${MASTER_LOG_DIR}" "${REPLICA_LOG_DIR}"
}
trap cleanup EXIT

set -x
echo "Starting master on port ${MASTER_PORT} (threads=4, shards=3)..."
"${DFLY_BIN}" \
  --port "${MASTER_PORT}" --dir "${MASTER_DIR}" --omit_basic_usage \
  --dbfilename="" \
  --proactor_threads 4 --num_shards 3 \
  --log_dir "${MASTER_LOG_DIR}" &
MASTER_PID=$!

echo "Starting replica on port ${REPLICA_PORT} (threads=3, shards=2)..."
"${DFLY_BIN}" \
  --port "${REPLICA_PORT}" --dir "${REPLICA_DIR}" --omit_basic_usage \
  --dbfilename="" \
  --proactor_threads 3 --num_shards 2 \
  --replicaof "127.0.0.1:${MASTER_PORT}" \
  --log_dir "${REPLICA_LOG_DIR}" \
  --replicaof_no_one_start_journal &
REPLICA_PID=$!

set +x
sleep 0.5
echo -e "\n\n\nReplication running. Master PID: ${MASTER_PID}, Replica PID: ${REPLICA_PID}"
echo "Master port: ${MASTER_PORT}, Replica port: ${REPLICA_PORT}"
echo "Press Ctrl+C to stop."

wait "${MASTER_PID}" "${REPLICA_PID}"
