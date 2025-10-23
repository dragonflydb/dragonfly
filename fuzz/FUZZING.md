# AFL++ Fuzzing for Dragonfly

## Install AFL++

```bash
# From package manager (Ubuntu/Debian)
sudo apt update
sudo apt install afl++

# Or build from source
git clone https://github.com/AFLplusplus/AFLplusplus
cd AFLplusplus
make distrib
sudo make install
```

## Prepare System

```bash
sudo afl-system-config
```

Sets core_pattern and CPU governors for optimal AFL++ performance.

## Build

```bash
cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -C build-dbg dragonfly
```

## Run

```bash
cd fuzz
./run_fuzzer.sh
```

## Replay Crash on production dragonfly:
```bash
./dragonfly --port=6379 &
nc localhost 6379 < artifacts/resp/default/crashes/id:000000,...
```
