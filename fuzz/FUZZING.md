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
| `BUILD_DIR` | `build-dbg` | Path to build directory |

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

### Replay (RESP)

```bash
./build/dragonfly --port 6379 --logtostderr --proactor_threads 1 --dbfilename=""

python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000
```

### Replay (memcache)

```bash
./build/dragonfly --port 6379 --memcached_port=11211 --logtostderr --proactor_threads 1 --dbfilename=""

python3 fuzz/replay_crash.py fuzz/artifacts/memcache/default/crashes 000000 127.0.0.1 11211
```

### Package crash for sharing

```bash
cd fuzz
# RESP
./package_crash.sh 000000
# Memcache
./package_crash.sh 000000 fuzz/artifacts/memcache/default/crashes
```

Creates `crash-000000.tar.gz` containing crash data and `replay_crash.py`.
The recipient runs:

```bash
# RESP
./build/dragonfly --port 6379 --logtostderr --proactor_threads 1 --dbfilename=""
python3 replay_crash.py crashes 000000

# Memcache
./build/dragonfly --port 6379 --memcached_port=11211 --logtostderr --proactor_threads 1 --dbfilename=""
python3 replay_crash.py crashes 000000 127.0.0.1 11211
```

## Seed Corpus

| Target | Directory | Seeds | Coverage |
|--------|-----------|-------|----------|
| `resp` | `seeds/resp/` | 79 | string, list, hash, set, zset, stream, JSON, search, bloom, geo, HLL, bitops, scripting, ACL, pub/sub, transactions, server ops |
| `memcache` | `seeds/memcache/` | 15 | set/get, add/replace, append/prepend, cas, incr/decr, delete, multiget, gat, noreply, meta commands, flush, stats |
| `replication` | `seeds/replication/` | 13 | basic writes, multi-type, network drops, MULTI/EXEC, replica reads, DELETE/EXPIRE propagation, FLUSHDB, interleaved commands |

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

To add or regenerate replication seeds:
```bash
python3 fuzz/generate_replication_seeds.py
```

---

## Master-Replica Fuzzing

Tests two Dragonfly instances (master + replica) for crashes and data
consistency violations under random command sequences and network drops.

### How It Works

Unlike the single-instance fuzzers, the replication fuzzer uses a
**black-box orchestrator** approach:

```
AFL++ (dumb mode, -n)
  └─► replication_orchestrator.py   ← AFL++ target (no instrumentation)
        ├─► master :7379             ← plain Dragonfly build
        └─► replica :7380            ← plain Dragonfly build
```

The orchestrator reads a **multiplexed binary input** encoding:
- Commands to send to master (`ACTION_MASTER = 0`)
- Commands to send to replica (`ACTION_REPLICA = 1`)
- Network drop events (`ACTION_NET_DROP = 2`) – disconnect replica then reconnect
- Wait events (`ACTION_WAIT = 3`) – poll master `INFO replication` until `lag=0`

After each iteration the orchestrator checks data consistency via
`DBSIZE` + full `SCAN` key-set comparison.  If either instance crashes, or if
the key sets diverge, it signals `SIGABRT` → AFL++ saves the input as a crash.

Because Dragonfly is **not instrumented**, AFL++ runs in dumb mode (`-n`) and
uses the custom Python mutator (`replication_mutator.py`) as the sole mutation
source.  Coverage guidance is traded for the ability to fuzz across two
independent processes with a fully reproducible single-file crash format.

### Build and Run

```bash
# Normal (uninstrumented) build
./helio/blaze.sh
cd build-dbg && ninja dragonfly

# Run fuzzer
cd <repo-root>
./fuzz/run_replication_fuzzer.sh
```

Configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MASTER_PORT` | `7379` | Master listen port |
| `REPLICA_PORT` | `7380` | Replica listen port |
| `BUILD_DIR` | `build-dbg` | Path to build directory |
| `TIMEOUT` | `30000` | AFL++ per-iteration timeout (ms) |
| `WAIT_TIMEOUT_MS` | `3000` | Max ms to wait for replica sync per iteration |

### Crash Replay

```bash
# 1. Start instances
./build-dbg/dragonfly --port 7379 --logtostderr --proactor_threads 2 --dbfilename=""
./build-dbg/dragonfly --port 7380 --logtostderr --proactor_threads 2 --dbfilename=""
redis-cli -p 7380 REPLICAOF 127.0.0.1 7379

# 2. Replay crash 0
python3 fuzz/replay_replication_crash.py \
    fuzz/artifacts/replication/default/crashes 0

# 3. Verbose output
python3 fuzz/replay_replication_crash.py \
    fuzz/artifacts/replication/default/crashes 0 --verbose
```

State-history replay (for stateful crashes) works automatically when
`AFL_PERSISTENT_RECORD` is set: AFL++ saves RECORD files and
`replay_replication_crash.py` replays them in order before the crashing input.

### Persistent Mode (Faster)

Install `python-afl` for higher iteration throughput:

```bash
pip install python-afl
```

The orchestrator detects `python-afl` automatically and switches to persistent
mode (Dragonfly instances stay alive across AFL++ loop iterations).
