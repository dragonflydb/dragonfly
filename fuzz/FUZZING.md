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

Uses 1 proactor thread by default for stable coverage feedback.
Override with `AFL_PROACTOR_THREADS=N` if needed.

## Crash Replay

Dragonfly uses AFL++ persistent mode — the server accumulates state across
iterations. A crash at iteration 5000 depends on state built by inputs 1–4999.

`run_fuzzer.sh` syncs `AFL_PERSISTENT_RECORD` with `afl_loop_limit`
(default: 10000). The server restarts every N iterations and the last N inputs
are recorded, so the full state history is always available on crash.

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
# Start dragonfly (debug build, without AFL++)
./build/dragonfly --port 6379 --logtostderr --proactor_threads 1 --dbfilename=""

# Replay crash 000000
python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000
```

### Package crash for sharing

```bash
cd fuzz
./package_crash.sh 000000
```

Creates `crash-000000.tar.gz` with all RECORD files and a replay script.

Creates `crash-000000.tar.gz` containing crash data and `replay_crash.py`.
The recipient runs:

```bash
# Start dragonfly
./build/dragonfly --port 6379 --logtostderr --proactor_threads 1 --dbfilename=""

# Extract and replay
tar xzf crash-000000.tar.gz
cd crash-000000
python3 replay_crash.py crashes 000000
```

## Seed Corpus

`seeds/resp/` contains seed files covering major command families:
string, list, hash, set, sorted set, transactions, pub/sub, geo, HyperLogLog,
Bloom filter, bitops, copy, rename, sort, scan, and server introspection.

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
