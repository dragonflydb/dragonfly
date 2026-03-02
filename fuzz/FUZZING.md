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
