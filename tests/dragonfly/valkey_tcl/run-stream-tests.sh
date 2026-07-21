#!/bin/bash
# Run the Valkey stream TCL suites against a Dragonfly binary in external-server mode.
# Exits 0 only if every non-skipped test passes (harness returns 1 on any failure).
#
# Usage: ./run-stream-tests.sh <path-to-dragonfly-binary> [port]
# Fail fast on any setup error; -e is relaxed only around the harness to capture its status.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UPSTREAM="$SCRIPT_DIR/upstream"
SKIPLIST="$SCRIPT_DIR/skiplist.txt"

DRAGONFLY_BIN="${1:?usage: run-stream-tests.sh <dragonfly-binary> [port]}"
PORT="${2:-6399}"

if [ ! -d "$UPSTREAM/tests" ]; then
  echo "ERROR: $UPSTREAM not found. Run ./sync-valkey-tcl-tests.sh first." >&2
  exit 2
fi
DRAGONFLY_BIN="$(cd "$(dirname "$DRAGONFLY_BIN")" && pwd)/$(basename "$DRAGONFLY_BIN")"  # absolute
if [ ! -x "$DRAGONFLY_BIN" ]; then
  echo "ERROR: dragonfly binary not executable: $DRAGONFLY_BIN" >&2
  exit 2
fi

# The harness's set_executable_path.tcl requires an executable src/valkey-server to exist,
# even though external mode never runs it. Point it at the Dragonfly binary.
mkdir -p "$UPSTREAM/src"
ln -sf "$DRAGONFLY_BIN" "$UPSTREAM/src/valkey-server"

# Port-ownership verification is mandatory (fail-closed): without it a PONG could come from a
# FOREIGN server already on $PORT and the suite would FLUSHALL it.
if command -v ss >/dev/null 2>&1; then
  port_listeners() { ss -ltnp "sport = :$PORT" 2>/dev/null | tail -n +2; }
  port_owned_by_child() { port_listeners | grep -q "pid=$DF_PID,"; }
elif command -v lsof >/dev/null 2>&1; then
  port_listeners() { lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | tail -n +2; }
  port_owned_by_child() { lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | grep -qx "$DF_PID"; }
else
  echo "ERROR: need ss or lsof to verify port ownership; refusing to run (fail-closed)" >&2
  exit 2
fi

# The readiness probe must confirm the server answers commands, not merely accepts TCP.
if ! command -v redis-cli >/dev/null 2>&1; then
  echo "ERROR: redis-cli is required for the readiness PING; refusing to run (fail-closed)" >&2
  exit 2
fi

# Refuse to start if anything is already listening on the port.
if [ -n "$(port_listeners)" ]; then
  echo "ERROR: something is already listening on port $PORT, refusing to run against it" >&2
  port_listeners >&2
  exit 3
fi

LOGDIR="$(mktemp -d)"
DF_LOG="$LOGDIR/dragonfly.log"
"$DRAGONFLY_BIN" --bind 127.0.0.1 --port "$PORT" --dbfilename= --dbnum=16 --noversion_check \
  --logtostderr > "$DF_LOG" 2>&1 &
DF_PID=$!
cleanup() {
  local rc=$?
  kill "$DF_PID" 2>/dev/null || true
  for _ in $(seq 1 25); do kill -0 "$DF_PID" 2>/dev/null || break; sleep 0.2; done
  kill -9 "$DF_PID" 2>/dev/null || true
  wait "$DF_PID" 2>/dev/null || true
  # Keep the logs for diagnosis on failure; don't accumulate temp dirs on success.
  if [ "$rc" -eq 0 ]; then
    rm -rf "$LOGDIR"
  else
    echo "Logs kept at $LOGDIR" >&2
  fi
}
trap cleanup EXIT

ping_ok() {
  timeout 2 redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG
}
up=0
for _ in $(seq 1 100); do
  if ! kill -0 "$DF_PID" 2>/dev/null; then
    echo "ERROR: Dragonfly (pid $DF_PID) exited during startup" >&2
    cat "$DF_LOG" >&2 || true
    exit 3
  fi
  # Verify the listener is OUR child before the first PING (fail-closed ordering).
  if port_owned_by_child && ping_ok; then up=1; break; fi
  sleep 0.2
done
if [ "$up" -ne 1 ]; then
  echo "ERROR: Dragonfly did not become ready on port $PORT" >&2
  cat "$DF_LOG" >&2 || true
  exit 3
fi
echo "Dragonfly ready on port $PORT (pid $DF_PID)"

# Feed the harness a clean skipfile (strip comments and blank lines). A missing skiplist would
# run the known-HANGING tests and wedge the whole suite, so fail hard instead.
if [ ! -f "$SKIPLIST" ]; then
  echo "ERROR: skiplist not found: $SKIPLIST" >&2
  exit 2
fi
CLEAN_SKIP="$LOGDIR/skiplist.clean"
grep -vE '^[[:space:]]*(#|$)' "$SKIPLIST" > "$CLEAN_SKIP" || true
if [ ! -s "$CLEAN_SKIP" ]; then
  echo "ERROR: skiplist $SKIPLIST produced no entries - refusing to run the hanging tests" >&2
  exit 2
fi
echo "Skipping $(wc -l < "$CLEAN_SKIP") known-failing test(s) (see skiplist.txt)"

# --timeout bounds a single stuck test (the harness aborts the run on timeout); the outer
# `timeout` is a hard backstop so a hung blocking test can never wedge CI indefinitely
# (--kill-after escalates to SIGKILL if tclsh ignores the TERM).
cd "$UPSTREAM"
set +e  # capture the harness exit status instead of dying mid-report
timeout --kill-after=30 900 tclsh tests/test_helper.tcl \
  --host 127.0.0.1 --port "$PORT" \
  --single unit/type/stream \
  --single unit/type/stream-cgroups \
  --tags "-needs:debug -needs:repl -external:skip -large-memory" \
  --skipfile "$CLEAN_SKIP" \
  --timeout 120 \
  --durable
STATUS=$?
set -e

echo "TCL harness exit status: $STATUS"
exit $STATUS
