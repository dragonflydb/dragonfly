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
./run_fuzzer.sh
```

Configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AFL_PROACTOR_THREADS` | `1` | Server threads (1 = most stable coverage) |
| `AFL_LOOP_LIMIT` | `10000` | Iterations before server restart (= `AFL_PERSISTENT_RECORD`) |
| `BUILD_DIR` | `build-dbg` | Path to build directory |

## Custom Mutator

`resp_mutator.py` is a custom AFL++ mutator that operates at the RESP protocol
level. Instead of flipping random bytes (which mostly breaks RESP framing and
gets rejected by the parser), it:

- Parses input into a list of Redis commands
- Mutates at the command/argument level (replace command, change argument,
  insert/remove commands, wrap in MULTI/EXEC, swap order)
- Serializes back to valid RESP

The mutator is loaded automatically by `run_fuzzer.sh`. AFL++'s built-in
byte-level mutations also run alongside it (useful for parser edge cases).

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

### Replay

```bash
# Start dragonfly (non-AFL build)
./build/dragonfly --port 6379 --logtostderr --proactor_threads 1 --dbfilename=""

# Replay crash 000000
python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000
```

### Package crash for sharing

```bash
cd fuzz
./package_crash.sh 000000
```

Creates `crash-000000.tar.gz` containing crash data and `replay_crash.py`.
The recipient runs:

```bash
./build/dragonfly --port 6379 --logtostderr --proactor_threads 1 --dbfilename=""

tar xzf crash-000000.tar.gz
cd crash-000000
python3 replay_crash.py crashes 000000
```

## Seed Corpus

`seeds/resp/` contains 79 seed files covering all major command families:
string, list, hash, set, sorted set, stream, transactions, pub/sub, geo,
HyperLogLog, Bloom filter, bitops, JSON, search, scripting, ACL, and
server introspection.

To add a new seed, create a file with RESP-encoded commands:

```
*3
$3
SET
$3
key
$5
value
```
