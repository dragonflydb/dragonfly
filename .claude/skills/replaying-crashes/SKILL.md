---
name: replaying-crashes
description: >
  Downloads and replays AFL++ fuzzer crashes from GitHub Actions runs. Use when
  the user provides a GitHub Actions URL with crash artifacts, mentions "replay
  crash", "reproduce crash", "AFL crash", "fuzz crash", or wants to debug a
  fuzzer-found crash.
allowed-tools: Bash(gh run download *), Bash(ls *), Bash(tar *), Bash(xxd *), Bash(cd build-dbg && ninja dragonfly *), Bash(./build-dbg/dragonfly *), Bash(python3 *), Bash(kill *), Bash(tail *), Bash(grep *), Bash(sleep *), Bash(rm -rf /tmp/df-crash-*), Bash(rm -rf build-dbg/crash-*), Read, Glob, Grep
argument-hint: "[GitHub Actions URL or run ID]"
---

# Replaying AFL++ Crashes from CI

Download crash artifacts from a GitHub Actions fuzzing run and replay them
against a local Dragonfly instance to reproduce the bug.

## Input

A GitHub Actions run URL, e.g.:
`https://github.com/dragonflydb/dragonfly/actions/runs/22484692959`

Or a run ID: `22484692959`

## Steps

### 1. Extract Run ID and Download Artifacts

Parse the run ID from the URL, then download crash artifacts:

```bash
# Extract run ID from URL (everything after /runs/)
RUN_ID=22484692959

# Download all crash artifacts
gh run download $RUN_ID --repo dragonflydb/dragonfly --dir /tmp/df-crash-$RUN_ID
```

### 2. Extract Crash Tarballs

The artifact contains `packaged/crash-NNNNNN.tar.gz` files. Extract them into `build-dbg/`:

```bash
for tarball in /tmp/df-crash-$RUN_ID/packaged/crash-*.tar.gz; do
  tar xzf "$tarball" -C build-dbg/
done
```

This creates directories like `build-dbg/crash-000000/` containing:
- `replay_crash.py` — the replay script
- `crashes/` — crash inputs and RECORD files

### 3. Build Dragonfly (Debug)

A debug build is needed to get useful crash information:

```bash
cd build-dbg && ninja dragonfly
```

### 4. Start Dragonfly, Replay, and Check

Use a non-default port (e.g., 16399) to avoid conflicts. Do all of this in one step:

```bash
PORT=16399
LOG=/tmp/df-crash-log-$RUN_ID.txt

# Start dragonfly in background
./build-dbg/dragonfly --port $PORT --logtostderr --proactor_threads=4 &>$LOG &
DF_PID=$!
sleep 4  # Wait for startup

# Replay each crash
for crash_dir in build-dbg/crash-*/; do
  CRASH_ID=$(basename "$crash_dir" | sed 's/crash-//')
  echo "=== Replaying crash $CRASH_ID ==="
  python3 "$crash_dir/replay_crash.py" "$crash_dir/crashes" "$CRASH_ID" localhost $PORT
done

# Check if dragonfly survived
sleep 1
if kill -0 $DF_PID 2>/dev/null; then
  echo "Dragonfly still running (no crash). Checking logs..."
  tail -20 $LOG
  kill $DF_PID 2>/dev/null
else
  echo "DRAGONFLY CRASHED! Full log:"
  cat $LOG
fi
```

- If Dragonfly crashes, the bug is reproduced — examine the crash output/logs
- If Dragonfly survives, check the log tail for errors/warnings — the bug may depend on ASAN or thread timing

## How the Replay Works

The `replay_crash.py` script:
1. Finds `RECORD:NNNNNN,cnt:*` files (AFL persistent-mode accumulated state)
2. Replays them in sorted order via TCP connections to build up server state
3. Sends the final crash input (`id:NNNNNN,*`)
4. Each input uses a fresh TCP connection with a 200ms timeout

## Cleanup

```bash
rm -rf /tmp/df-crash-$RUN_ID
rm -rf build-dbg/crash-*
```
