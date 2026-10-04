#!/usr/bin/env bash

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
print_error() { echo -e "${RED}[ERROR]${NC} $*"; }
print_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

usage() {
    echo "Usage: $0 <crash_id> [crashes_dir]"
    echo ""
    echo "Packages a crash and its RECORD files into a self-contained archive"
    echo "that can be sent to another developer for reproduction."
    echo ""
    echo "Arguments:"
    echo "  crash_id      Crash ID (e.g. 000000)"
    echo "  crashes_dir   Path to crashes directory (default: fuzz/artifacts/resp/default/crashes)"
    echo ""
    echo "Example:"
    echo "  $0 000000"
    echo "  $0 000001 /path/to/crashes"
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

CRASH_ID="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRASHES_DIR="${2:-$SCRIPT_DIR/artifacts/resp/default/crashes}"

if [[ ! -d "$CRASHES_DIR" ]]; then
    print_error "Crashes directory not found: $CRASHES_DIR"
    exit 1
fi

# Find the crash input file
CRASH_FILE=$(find "$CRASHES_DIR" -maxdepth 1 -name "id:${CRASH_ID},*" ! -name "RECORD:*" | head -1)
if [[ -z "$CRASH_FILE" ]]; then
    print_error "Crash input not found for id:${CRASH_ID} in $CRASHES_DIR"
    exit 1
fi

# AFL numbers RECORD sets by crash event, counting unsaved calibration/trim aborts too, while the
# crash file's id: counts only saved crashes — so RECORD:${CRASH_ID} is usually another crash's
# history. The ring saves every testcase before running it, so the crash's own set is the one whose
# newest (highest-cnt) record is byte-identical to the crash input; pair by that.
declare -A NEWEST_REC
while IFS= read -r f; do
    base=${f##*/}
    ev=${base#RECORD:}; ev=${ev%%,*}
    cur=${NEWEST_REC[$ev]:-}
    if [[ -z "$cur" ]] || (( 10#${base##*,cnt:} > 10#${cur##*,cnt:} )); then
        NEWEST_REC[$ev]=$base
    fi
done < <(find "$CRASHES_DIR" -maxdepth 1 -name 'RECORD:*,cnt:*')

RECORD_EVENT=""
MATCHES=0
for ev in $(printf '%s\n' "${!NEWEST_REC[@]}" | sort -n); do
    if cmp -s "$CRASHES_DIR/${NEWEST_REC[$ev]}" "$CRASH_FILE"; then
        MATCHES=$((MATCHES + 1))
        if [[ -z "$RECORD_EVENT" ]]; then
            RECORD_EVENT="$ev"
        fi
    fi
done

ARCHIVE_NAME="crash-${CRASH_ID}"
TMPDIR=$(mktemp -d)
DEST="$TMPDIR/$ARCHIVE_NAME"
mkdir -p "$DEST/crashes"

print_info "Packaging crash ${CRASH_ID}..."
print_info "Crash input: $(basename "$CRASH_FILE")"

# Copy the crash input, then this crash's RECORD history renamed to RECORD:${CRASH_ID} so the
# bundled replay (which keys off the crash id) finds it; the cnt suffix is kept so replay order is
# unchanged. The set's newest record is byte-identical to the crash input, and replay sends the
# crash input on its own after the records, so drop that newest record to avoid replaying it twice.
cp "$CRASH_FILE" "$DEST/crashes/"
RECORD_COUNT=0
if [[ -n "$RECORD_EVENT" ]]; then
    if [[ $MATCHES -gt 1 ]]; then
        print_warn "The crash input is the newest record of ${MATCHES} sets whose earlier inputs"
        print_warn "differ; their histories cannot be told apart. Using set ${RECORD_EVENT} — replay"
        print_warn "may rebuild a different state than the fuzz run."
    fi
    NEWEST_BASE="${NEWEST_REC[$RECORD_EVENT]}"
    while IFS= read -r rec; do
        base=${rec##*/}
        if [[ "$base" == "$NEWEST_BASE" ]]; then
            continue
        fi
        cp "$rec" "$DEST/crashes/RECORD:${CRASH_ID},${base#*,}"
        RECORD_COUNT=$((RECORD_COUNT + 1))
    done < <(find "$CRASHES_DIR" -maxdepth 1 -name "RECORD:${RECORD_EVENT},cnt:*")
    print_info "RECORD files: ${RECORD_COUNT} (from set ${RECORD_EVENT}; crash input sent separately)"
else
    print_warn "No RECORD set matches the crash input by content; its true history was not saved"
    print_warn "(an unsaved abort reused its number). Packaging the crash input alone."
fi

# Copy replay_crash.py
cp "$SCRIPT_DIR/replay_crash.py" "$DEST/"

# Copy repro.env — contains exact Dragonfly flags + memory limit used during fuzzing.
# triage_crashes.sh reads this to start Dragonfly identically to the fuzz run.
# repro.env lives one level above the fuzzer instance dir (i.e. OUTPUT_DIR):
#   crashes_dir  = .../artifacts/<target>/default/crashes
#   repro.env    = .../artifacts/<target>/repro.env
REPRO_ENV="$(dirname "$(dirname "$CRASHES_DIR")")/repro.env"
GUESSED_ARCHIVE=0
if [[ -f "$REPRO_ENV" ]]; then
    cp "$REPRO_ENV" "$DEST/"
    grep -q '^GUESSED=1' "$REPRO_ENV" && GUESSED_ARCHIVE=1
    print_info "Reproduction environment: repro.env included"
else
    # Without the run's repro.env the archive would replay under guessed flags and the wrong
    # protocol. Write one from the run_fuzzer.sh defaults; the protocol comes from the
    # artifacts path (fuzz/artifacts/<protocol>/...) or from PROTOCOL in the environment.
    # The artifacts path is authoritative; PROTOCOL only settles an ambiguous path.
    PATH_PROTOCOL=""
    [[ "$CRASHES_DIR" == */artifacts/memcache/* ]] && PATH_PROTOCOL=memcache
    [[ "$CRASHES_DIR" == */artifacts/resp/* ]] && PATH_PROTOCOL=resp
    if [[ -n "${PROTOCOL:-}" && "$PROTOCOL" != "resp" && "$PROTOCOL" != "memcache" ]]; then
        print_error "PROTOCOL must be 'resp' or 'memcache', got: $PROTOCOL"
        rm -rf "$TMPDIR"
        exit 1
    fi
    if [[ -n "$PATH_PROTOCOL" && -n "${PROTOCOL:-}" && "$PROTOCOL" != "$PATH_PROTOCOL" ]]; then
        print_error "PROTOCOL=$PROTOCOL contradicts the artifacts path ($PATH_PROTOCOL): $CRASHES_DIR"
        rm -rf "$TMPDIR"
        exit 1
    fi
    GUESSED_PROTOCOL="${PATH_PROTOCOL:-${PROTOCOL:-}}"
    if [[ -z "$GUESSED_PROTOCOL" ]]; then
        print_error "repro.env not found at $REPRO_ENV and the protocol cannot be inferred from" \
            "$CRASHES_DIR; set PROTOCOL=resp|memcache"
        rm -rf "$TMPDIR"
        exit 1
    fi
    GUESSED_ARCHIVE=1
    print_warn "repro.env not found at $REPRO_ENV — writing one from the run_fuzzer.sh defaults (protocol: $GUESSED_PROTOCOL)"
    {
        echo "# Dragonfly reproduction environment — generated by package_crash.sh from the"
        echo "# run_fuzzer.sh defaults because the fuzz run's repro.env was missing."
        echo "# Start the server on an EMPTY --dir: the fuzzer clears it on every restart."
        echo "GUESSED=1"
        echo "PROTOCOL=${GUESSED_PROTOCOL}"
        echo "MEM_LIMIT_KB=$((4096 * 1024))"
        echo "--port=6379"
        echo "--logtostderr"
        echo "--minloglevel=2"
        echo "--proactor_threads=1"
        echo "--dbfilename="
        echo "--omit_basic_usage"
        echo "--restricted_commands=SHUTDOWN,DEBUG,FLUSHALL,FLUSHDB"
        echo "--max_bulk_len=1048576"
        [[ "$GUESSED_PROTOCOL" == "memcache" ]] && echo "--memcached_port=11211"
    } > "$DEST/repro.env"
fi

# A surviving replay proves a false positive only if it rebuilt the fuzz-run state. Record in the
# archive when that cannot be guaranteed — the history is missing (no set matched) or ambiguous
# (several sets share this crash's final input but differ earlier) — so the recipient's triage
# reports INCONCLUSIVE instead of a confident false positive (the stdout warnings above are not shipped).
if [[ -z "$RECORD_EVENT" ]]; then
    echo "HISTORY_MISSING=1" >> "$DEST/repro.env"
elif [[ $MATCHES -gt 1 ]]; then
    echo "HISTORY_AMBIGUOUS=${MATCHES}" >> "$DEST/repro.env"
fi

REPLAY_PORT=6379
MODE_HINT="resp"
if [[ -f "$DEST/repro.env" ]]; then
    _mc_port=$(grep '^--memcached_port=' "$DEST/repro.env" | cut -d= -f2 || true)
    if [[ -n "$_mc_port" ]]; then
        REPLAY_PORT="$_mc_port"
        MODE_HINT="memcache"
    else
        _resp_port=$(grep '^--port=' "$DEST/repro.env" | cut -d= -f2 || true)
        REPLAY_PORT="${_resp_port:-6379}"
    fi
fi

# Create archive
OUTPUT="$(pwd)/${ARCHIVE_NAME}.tar.gz"
tar -czf "$OUTPUT" -C "$TMPDIR" "$ARCHIVE_NAME"
rm -rf "$TMPDIR"

SIZE=$(du -h "$OUTPUT" | cut -f1)
print_info "Archive created: ${OUTPUT} (${SIZE})"
echo ""

echo "To reproduce:"
echo "  1. Extract the archive:"
echo "     tar xzf ${ARCHIVE_NAME}.tar.gz && cd ${ARCHIVE_NAME}"
if [[ $GUESSED_ARCHIVE -eq 1 ]]; then
    echo "  2. Start a plain (non-AFL) Dragonfly with the DEFAULT flags in repro.env (the fuzz run's"
    echo "     own flags were not available; a surviving server is inconclusive)"
else
    echo "  2. Start a plain (non-AFL) Dragonfly with the exact flags from the fuzz run (repro.env)"
fi
echo "     on an empty --dir:"
echo "     MEM_KB=\$(grep '^MEM_LIMIT_KB=' repro.env | cut -d= -f2)"
echo "     readarray -t DF_FLAGS < <(grep '^--' repro.env)"
echo "     WORK=\$(mktemp -d)   # empty --dir; also the cwd, where a relative --tiered_prefix lands"
echo "     (cd \"\$WORK\" && ulimit -v \"\$MEM_KB\" && exec <absolute-path-to-dragonfly> \"\${DF_FLAGS[@]}\" --dir=\"\$WORK\" --bind=127.0.0.1) &"
echo "     DF_PID=\$!   # loopback only: the replay server has no authentication"
echo "  3. Replay:"
if [[ "$MODE_HINT" == "memcache" ]]; then
    echo "     python3 replay_crash.py crashes ${CRASH_ID} 127.0.0.1 ${REPLAY_PORT} --protocol memcache --pid \"\$DF_PID\""
else
    echo "     python3 replay_crash.py crashes ${CRASH_ID} 127.0.0.1 ${REPLAY_PORT} --pid \"\$DF_PID\""
fi
echo "  4. Stop the server and remove its directory:"
echo "     kill \"\$DF_PID\"; wait \"\$DF_PID\" 2>/dev/null; rm -rf \"\$WORK\""
echo ""
echo "Or use the triage script to reproduce all crashes from a zip:"
echo "  ./fuzz/triage_crashes.sh <path-to-dragonfly> ${MODE_HINT:-resp} <crashes.zip>"
