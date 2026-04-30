# AFL++ Fuzzing for Dragonfly

## Install AFL++

AFL++ must be built from source with `AFL_PERSISTENT_RECORD` enabled for crash replay.

```bash
sudo apt update
sudo apt install llvm-18-dev clang-18 lld-18 gcc-13-plugin-dev

git clone --depth=1 --branch v4.34c https://github.com/AFLplusplus/AFLplusplus.git
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
| `BUILD_DIR` | `build-dbg` | Path to build directory |

Save mode (`AFL_ENABLE_SAVE=1`) enables `--dbfilename=dump` and writes snapshots
to a temp directory. Enabled automatically in nightly (long) fuzzing campaigns.
The dump directory is cleaned before each AFL++ loop cycle to ensure RECORD files
capture the full state needed for crash reproduction.

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
| `resp` | `resp_mutator.py` | 150+ Redis commands, wraps in MULTI/EXEC |
| `memcache` | `memcache_mutator.py` | Store/get/meta commands, noreply toggle |

Mutators are loaded automatically by `run_fuzzer.sh`. AFL++'s built-in
byte-level mutations also run alongside them (useful for parser edge cases).

To use only the custom mutator: `export AFL_CUSTOM_MUTATOR_ONLY=1`.

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

### Triage crashes from CI

Download the crashes zip from CI artifacts and run:

```bash
./fuzz/triage_crashes.sh ./build-dbg/dragonfly resp crashes.zip
./fuzz/triage_crashes.sh ./build-dbg/dragonfly memcache crashes.zip
```

### Replay a single crash

Each crash has a `repro.env` with the exact Dragonfly flags used during fuzzing.
Use it to start the server with the same configuration:

```bash
# Load flags from repro.env:
MEM_KB=$(grep '^MEM_LIMIT_KB=' fuzz/artifacts/resp/repro.env | cut -d= -f2)
readarray -t DF_FLAGS < <(grep -v '^#' fuzz/artifacts/resp/repro.env | grep -v '^MEM_LIMIT_KB=' | grep -v '^$')
(ulimit -v "$MEM_KB"; exec ./build-dbg/dragonfly "${DF_FLAGS[@]}") &

# RESP replay:
python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000
```

For memcache, use the memcache repro.env and pass the memcache port:

```bash
MEM_KB=$(grep '^MEM_LIMIT_KB=' fuzz/artifacts/memcache/repro.env | cut -d= -f2)
readarray -t DF_FLAGS < <(grep -v '^#' fuzz/artifacts/memcache/repro.env | grep -v '^MEM_LIMIT_KB=' | grep -v '^$')
(ulimit -v "$MEM_KB"; exec ./build-dbg/dragonfly "${DF_FLAGS[@]}") &

# Memcache replay:
python3 fuzz/replay_crash.py fuzz/artifacts/memcache/default/crashes 000000 127.0.0.1 11211
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

# Start Dragonfly with the fuzz run flags:
MEM_KB=$(grep '^MEM_LIMIT_KB=' repro.env | cut -d= -f2)
readarray -t DF_FLAGS < <(grep -v '^#' repro.env | grep -v '^MEM_LIMIT_KB=' | grep -v '^$')
(ulimit -v "$MEM_KB"; exec ./build-dbg/dragonfly "${DF_FLAGS[@]}") &

# RESP:
python3 replay_crash.py crashes 000000
# Memcache:
python3 replay_crash.py crashes 000000 127.0.0.1 11211
```

## Seed Corpus

| Target | Directory | Seeds | Coverage |
|--------|-----------|-------|----------|
| `resp` | `seeds/resp/` | 96 | string, list, hash, set, zset, stream, JSON, search, bloom, geo, HLL, bitops, scripting, ACL, pub/sub, transactions, server ops, CMS, Top-K, field expiry, hash expiry, SADDEX, GEORADIUS |
| `memcache` | `seeds/memcache/` | 15 | set/get, add/replace, append/prepend, cas, incr/decr, delete, multiget, gat, noreply, meta commands, flush, stats |

To add a new RESP seed:
```
*3
$3
SET
$3
key
$5
value
```

To add a new memcache seed:
```
set mykey 0 0 5
hello
get mykey
```
