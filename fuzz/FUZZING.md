# AFL++ Fuzzing for Dragonfly

## Install AFL++

AFL++ must be built from source with `AFL_PERSISTENT_RECORD` enabled for crash replay.

```bash
sudo apt update
sudo apt install llvm-18-dev clang-18 lld-18 gcc-13-plugin-dev

git clone --depth=1 --branch v5.00c https://github.com/AFLplusplus/AFLplusplus.git
cd AFLplusplus

# Enable AFL_PERSISTENT_RECORD (required for stateful crash replay)
sed -i 's|// #define AFL_PERSISTENT_RECORD|#define AFL_PERSISTENT_RECORD|' include/config.h

make distrib
sudo make install
```

## Prepare System

```bash
sudo afl-system-config
```

`run_fuzzer.sh` also runs these checks automatically (core_pattern, CPU governor).

## Build Dragonfly

```bash
cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -C build-dbg dragonfly
```

This binary is the fuzz target only: outside `afl-fuzz` it runs the stdin fuzz loop and exits
at once, so `triage_crashes.sh` and `replay_crash.py` need a plain Debug build (no `USE_AFL`),
for example:

```bash
cmake -B build-dbg-plain -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -C build-dbg-plain dragonfly
```

`triage_crashes.sh` refuses an AFL-instrumented binary.

## Run Fuzzer

```bash
cd fuzz
./run_fuzzer.sh              # RESP protocol (default)
./run_fuzzer.sh memcache     # Memcache text protocol
```

Configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AFL_PROACTOR_THREADS` | `1` | Server threads (1 = most stable coverage) |
| `AFL_LOOP_LIMIT` | `10000` | Iterations before server restart (= `AFL_PERSISTENT_RECORD`) |
| `AFL_ENABLE_SAVE` | off | Set to `1` to enable SAVE/BGSAVE (tests snapshot serialization) |
| `AFL_ENABLE_TIERING` | off | Set to `1` to enable tiered storage (offload/fetch code paths) |
| `AFL_TIER_DIR` | `/tmp` | Base directory for tiering backing files |
| `AFL_TIER_COOLING` | unset | Pin `tiered_experimental_cooling`; unset = `AFL_RUN_NUMBER` parity in CI, `true` locally |
| `BUILD_DIR` | `build-dbg` | Path to build directory |

Save mode (`AFL_ENABLE_SAVE=1`) enables `--dbfilename=dump` and writes snapshots
to a temp directory. Enabled automatically in nightly (long) fuzzing campaigns.
The dump directory is cleaned before each AFL++ loop cycle to ensure RECORD files
capture the full state needed for crash reproduction.

Tiering mode (`AFL_ENABLE_TIERING=1`) starts the server with tiered storage
(`--tiered_offload_threshold=1.0`, eager offload) so offload/fetch paths are
fuzzed. Backing files are opened with O_DIRECT when the backing filesystem
supports it (probed at startup; matches the production default) and with
buffered IO otherwise (tmpfs/overlayfs). CI places backing files on a real
disk (`AFL_TIER_DIR` points at the runner temp dir) and alternates
`tiered_experimental_cooling` by run number parity, so both cooling code
paths get nightly coverage. The chosen values are recorded in `repro.env`;
`triage_crashes.sh` re-probes O_DIRECT locally and falls back to buffered IO
if the local filesystem rejects it.

## Custom Mutators

Each target has a custom AFL++ mutator that operates at the protocol level.
Instead of flipping random bytes (which mostly breaks protocol framing and
gets rejected by the parser), they:

- Parse input into a list of commands
- Mutate at the command/argument level (replace command, change argument,
  insert/remove commands, swap order)
- Serialize back to valid protocol format

| Target | Mutator | Details |
|--------|---------|---------|
| `resp` | `resp_mutator.py` | every registered command (~280), wraps in MULTI/EXEC |
| `memcache` | `memcache_mutator.py` | Store/get/meta commands, noreply toggle |

Mutators are loaded automatically by `run_fuzzer.sh`. CI runs with
`AFL_CUSTOM_MUTATOR_ONLY=1` (set in the fuzzing action), so **only** the custom
mutator runs — AFL++'s byte-level stages and the dictionary are disabled. Reasons:

- Byte-level parser fuzzing was not finding anything (the protocol parsers are
  well-covered), so it mostly wasted cycles.
- It made the AFL++ queue explode: byte mutation produces arbitrary keys/values,
  so almost every input looks like "new coverage", the queue grows without bound,
  and afl-fuzz is eventually OOM-killed (exit 137). The custom mutator uses a
  bounded command/key/value vocabulary, keeping the queue and memory bounded.

Locally you can drop `AFL_CUSTOM_MUTATOR_ONLY` to also run the byte-level stages
and the dictionary (`dict/*.dict`) for ad-hoc parser-edge-case exploration.

## Crash Replay

Dragonfly uses AFL++ persistent mode — the server accumulates state across
iterations. A crash at iteration N depends on state built by inputs 1..N-1.

`run_fuzzer.sh` syncs `AFL_PERSISTENT_RECORD` with `afl_loop_limit`
so the full state history is always available on crash.

When a crash occurs, AFL++ saves:
```
crashes/id:000000,sig:06,...           # the crashing input
crashes/RECORD:000000,cnt:000000      # first input after server start
crashes/RECORD:000000,cnt:000001      # second input
...
crashes/RECORD:000000,cnt:NNNNNN      # input before the crash
```

The saved `id:*` input is the one that was **in flight** when the process died, which is often
not the one that ran the fatal command: after an assertion the process keeps its listener up for
tens of milliseconds while the stack trace is printed, and the fuzzer has already moved on to
the next input. The in-process harness also reads only one reply and closes, and the server
drops the unexecuted tail of a pipeline when the peer closes, so the same input can survive
thousands of times and die once. Both effects are handled by the tools below.

### Triage crashes from CI

Download the crashes zip from CI artifacts and run:

```bash
./fuzz/triage_crashes.sh ./build-dbg-plain/dragonfly resp crashes.zip
./fuzz/triage_crashes.sh ./build-dbg-plain/dragonfly memcache crashes.zip
```

Each crash gets up to two passes on a fresh server: **drain** (a barrier command is appended to
every input on the same connection and its reply is awaited, so every recorded command before
it has executed; reproduces deterministic crashes and prints `Server died after input
cnt:NNNNNN`, the input that ran the fatal command) and, only if the server survived,
**harness** (the exact fuzzer behaviour byte for byte, for timing-dependent bugs). A confirmed
crash is printed with the fatal message and stack trace from the server's stderr. With
`AFL_DEBUG_CHILD=1` (set by `run_fuzzer.sh`) the same message is also visible in the CI job log.
The server runs under the memory limit recorded in the archive; if it cannot start under it on
your machine, set `TRIAGE_MEM_LIMIT_KB` (a number, or `unlimited`). Any such deviation from the
fuzz run's configuration (an overridden memory limit, buffered IO forced because the filesystem
has no O_DIRECT for tiering) turns a surviving server into INCONCLUSIVE rather than a false
positive, since an OOM- or O_DIRECT-dependent crash would not reproduce; a signal death under
such a deviation is reported as a crash under a different configuration and counted apart
from confirmed crashes (exit code 2, not 1). An archive recorded for the other protocol is
skipped and counted as failed. Tiering archives are, by policy, always replayed under a
deviation: the fuzz run keeps its backing files on the runner's own disk and tiering crashes
are IO- and timing-dependent, so locally they can only be INCONCLUSIVE or "crash under a
different configuration", never a plain CONFIRMED or FALSE POSITIVE.

Exit codes of `triage_crashes.sh`: 0 = every crash a false positive; 1 = at least one crash
confirmed under the recorded configuration; 2 = some crash failed, was inconclusive, or
crashed only under a changed configuration; 3 = bad arguments or the triage could not start.

### Replay a single crash

Each crash has a `repro.env` with the Dragonfly flags of the fuzz run (an archive packaged
without the run's file carries guessed defaults instead, marked `GUESSED=1`, and a surviving
server is then inconclusive). For tiering runs it holds the placeholder
`--tiered_prefix=tiered_backing`, a path relative to the working directory: start the server
from an empty, writable directory, or replace it with an absolute path on a real disk.
Use it to start the server with the same configuration:

> Note: tiering-run archives may record `--backing_file_direct=true` (the fuzz
> machine supported O_DIRECT). If your filesystem rejects O_DIRECT
> (tmpfs/overlayfs on older kernels), the server aborts at startup — append
> `--backing_file_direct=false` after `"${DF_FLAGS[@]}"` (the last occurrence
> wins). `triage_crashes.sh` handles this automatically.

```bash
# Load flags from repro.env and start the server on an empty --dir, as the fuzzer does:
MEM_KB=$(grep '^MEM_LIMIT_KB=' fuzz/artifacts/resp/repro.env | cut -d= -f2)
readarray -t DF_FLAGS < <(grep '^--' fuzz/artifacts/resp/repro.env)
DF=$(realpath ./build-dbg-plain/dragonfly)   # absolute: the server starts from another directory
WORK=$(mktemp -d)   # empty --dir; also the cwd, where a relative --tiered_prefix lands
(cd "$WORK" && ulimit -v "$MEM_KB" && exec "$DF" "${DF_FLAGS[@]}" --dir="$WORK" --bind=127.0.0.1) &
DF_PID=$!   # loopback only: the replay server has no authentication

# RESP replay (drain mode: every command executes; reports the input after which the server died).
# The replay waits up to --wait seconds (default 10) for the listener:
python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000 --pid "$DF_PID"

# Exact fuzzer behaviour (send, one read, close), byte for byte. The drain above already changed
# the server's state, so start a fresh server on a new empty --dir first, as triage does:
kill "$DF_PID"; wait "$DF_PID" 2>/dev/null; rm -rf "$WORK"   # tiering backing files can be large
DF=$(realpath ./build-dbg-plain/dragonfly)   # absolute: the server starts from another directory
WORK=$(mktemp -d)   # empty --dir; also the cwd, where a relative --tiered_prefix lands
(cd "$WORK" && ulimit -v "$MEM_KB" && exec "$DF" "${DF_FLAGS[@]}" --dir="$WORK" --bind=127.0.0.1) &
DF_PID=$!   # loopback only: the replay server has no authentication
python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000 --pid "$DF_PID" --mode harness
kill "$DF_PID"; wait "$DF_PID" 2>/dev/null; rm -rf "$WORK"   # always stop the replay server
# Useful options: --tail N (last N RECORD inputs only), --no-crash-input, --verbose (per-input
# verification status), --timeout SEC (drain cap per input).
```

Exit codes: 3 = the server died and the killing input is printed (if the fatal message in the
server log predates that input, the previous one ran the fatal command, see above; when
earlier inputs were unverified a note says so, since the state may then differ from the fuzz
run); 4 = the server survived but some inputs could not be verified (they are listed with the
reason: the input ends in the middle of a command and was sent without a barrier, a blocking
command or open MULTI never answered, or the server closed the connection); 0 = survived (in
drain mode every input was verified; harness mode verifies nothing). Liveness is checked
through `--pid` or a bare TCP connect, never with an extra command, and with `--pid` the
replay refuses to send anything unless `/proc` proves that the listener on the port is that
process's own socket (use the pid of the server you started, as the same user, on a local
address). Inputs longer than 64 KiB are cut to 64 KiB, the size of the harness's read buffer,
because the fuzzer never sent more than that in one iteration. `repro.env` records the
archive's protocol; `triage_crashes.sh` refuses to replay it under the other mode. An archive
packaged without the run's `repro.env` carries guessed default flags (`GUESSED=1`): the triage
reports a surviving server as INCONCLUSIVE rather than as a false positive.

For memcache, use the memcache repro.env and pass the memcache port:

```bash
MEM_KB=$(grep '^MEM_LIMIT_KB=' fuzz/artifacts/memcache/repro.env | cut -d= -f2)
readarray -t DF_FLAGS < <(grep '^--' fuzz/artifacts/memcache/repro.env)
DF=$(realpath ./build-dbg-plain/dragonfly)   # absolute: the server starts from another directory
WORK=$(mktemp -d)   # empty --dir; also the cwd, where a relative --tiered_prefix lands
(cd "$WORK" && ulimit -v "$MEM_KB" && exec "$DF" "${DF_FLAGS[@]}" --dir="$WORK" --bind=127.0.0.1) &
DF_PID=$!   # loopback only: the replay server has no authentication

# Memcache replay:
python3 fuzz/replay_crash.py fuzz/artifacts/memcache/default/crashes 000000 127.0.0.1 11211 --protocol memcache --pid "$DF_PID"
kill "$DF_PID"; wait "$DF_PID" 2>/dev/null; rm -rf "$WORK"
```

### Package crash for sharing

```bash
# RESP
./fuzz/package_crash.sh 000000
# Memcache
./fuzz/package_crash.sh 000000 fuzz/artifacts/memcache/default/crashes
# All RESP crashes
for f in fuzz/artifacts/resp/default/crashes/id:*; do
  id=$(basename "$f" | grep -oP '(?<=id:)\d+')
  ./fuzz/package_crash.sh "$id"
done
```

Creates `crash-000000.tar.gz` containing crash data, `replay_crash.py`, and `repro.env`.
The recipient extracts and runs:

```bash
tar xzf crash-000000.tar.gz && cd crash-000000

# Start a plain (non-AFL) Dragonfly with the fuzz run flags on an empty --dir:
MEM_KB=$(grep '^MEM_LIMIT_KB=' repro.env | cut -d= -f2)
readarray -t DF_FLAGS < <(grep '^--' repro.env)
WORK=$(mktemp -d)   # empty --dir; also the cwd, where a relative --tiered_prefix lands
(cd "$WORK" && ulimit -v "$MEM_KB" && exec <absolute-path-to-dragonfly> "${DF_FLAGS[@]}" --dir="$WORK" --bind=127.0.0.1) &
DF_PID=$!   # loopback only: the replay server has no authentication

# RESP:
python3 replay_crash.py crashes 000000 --pid "$DF_PID"
# Memcache:
python3 replay_crash.py crashes 000000 127.0.0.1 11211 --protocol memcache --pid "$DF_PID"
kill "$DF_PID"; wait "$DF_PID" 2>/dev/null; rm -rf "$WORK"
```

## Seed Corpus

| Target | Directory | Seeds | Coverage |
|--------|-----------|-------|----------|
| `resp` | `seeds/resp/` | 122 | string, list, hash, set, zset, stream, JSON, search, bloom, geo, HLL, bitops, scripting, ACL, pub/sub, transactions, server ops, CMS, Top-K, field expiry, hash expiry, SADDEX, GEORADIUS, hybrid vector search (FT.HYBRID), sharded pub/sub, bloom SCANDUMP/LOADCHUNK, RESP3 reply serialization (`HELLO 3`), RM, BF.INFO, option-grammar depth seeds (`*_gap.resp`) |
| `memcache` | `seeds/memcache/` | 15 | set/get, add/replace, append/prepend, cas, incr/decr, delete, multiget, gat, noreply, meta commands (ms/mg/md/ma with valid flags), stats/version |

Every seed must consist of commands that **execute successfully** on a fresh
fuzz server (no unknown-command, arity, syntax, wrong-type, or missing-state
errors): the custom mutator starts from these examples, so a seed that
dead-ends in an error reply gives it nothing valid to mutate. Build the state a
command needs earlier in the same file. `fuzz/check_fuzz_coverage.py` enforces
that every mutator command appears in command position in a strictly-parseable
seed (for both `resp` and `memcache`); it rejects a seed the RESP/memcache
parser cannot read, so a mis-framed seed fails CI instead of silently
contributing nothing.

The one sanctioned exception is a command whose sole fuzzing value is a parser
branch that has **no** successful dispatch path: memcache `cas`. Dragonfly's
`McTypeToCmdName` has no `MP::CAS` case, so every `cas` returns `CLIENT_ERROR`
at dispatch — but it uniquely exercises `MemcacheParser::ParseStore`'s CAS
branch (the `cas_unique` field), which is real parser-fuzzing surface. It is
therefore kept in `memcache_mutator.COMMANDS` and seeded once (in
`seeds/memcache/cas.mc`) purely to give the mutator a correctly-framed example;
its `CLIENT_ERROR` reply is the accepted trade-off, not a seed defect.

To add a new RESP seed (lines MUST be CRLF-terminated — `\r\n`, not `\n`):
```
*3
$3
SET
$3
key
$5
value
```
RESP framing requires `\r\n`. A seed written with bare `\n` is rejected by the
server (`invalid multibulk length`) and cannot be parsed by the custom mutator,
so its commands never execute. Bulk lengths must match the byte count of the
value, and the array length (`*N`) must match the number of elements.

To add a new memcache seed:
```
set mykey 0 0 5
hello
get mykey
```
