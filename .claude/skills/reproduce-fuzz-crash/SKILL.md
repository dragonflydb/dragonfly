---
name: reproduce-fuzz-crash
description: >
  Reproduce AFL++ fuzz crashes from GitHub Actions. Use when user provides a
  GitHub Actions fuzz run URL and wants to reproduce and analyze the crash locally.
argument-hint: <github-actions-run-url>
allowed-tools: Bash, Read, Grep, Glob, Write
---

# Reproduce Fuzz Crash

Given a GitHub Actions fuzz run URL, download crash artifacts, replay them
against a local Dragonfly build, and produce a crash analysis report.

**Input**: `$ARGUMENTS` — a GitHub Actions run URL like:
`https://github.com/dragonflydb/dragonfly/actions/runs/22906484769`
or with query params like `?pr=6855`.

## Workflow

### Step 1: Parse the URL

Extract `owner/repo` and `run_id` from the URL.

```
https://github.com/{owner}/{repo}/actions/runs/{run_id}[?...]
```

Strip any query parameters from `run_id`.

### Step 2: Download artifacts

Use `gh` CLI to download all artifacts from the run:

```bash
gh run download {run_id} --repo {owner}/{repo} --dir /tmp/fuzz-repro-{run_id}
```

### Step 3: Discover crashes

Look for crash tarballs inside the downloaded artifacts:

```bash
find /tmp/fuzz-repro-{run_id} -name 'crash-*.tar.gz'
```

The typical structure is:
```
{artifact-name}/packaged/crash-NNNNNN.tar.gz
```

Each tarball contains:
```
crash-NNNNNN/
  replay_crash.py
  crashes/
    id:NNNNNN,sig:...,src:...    # the crash input
    RECORD:NNNNNN,cnt:000000     # preceding inputs (persistent mode state)
    RECORD:NNNNNN,cnt:000001
    ...
```

Extract all tarballs into `/tmp/fuzz-repro-{run_id}/extracted/`.

If no crash tarballs found, report that the run has no crash artifacts and stop.

### Step 4: Check Dragonfly binary

Check if the debug binary already exists and runs:

```bash
./build-dbg/dragonfly --version
```

Only build if the binary doesn't exist or fails to run:

```bash
cd build-dbg && ninja dragonfly
```

If `build-dbg` doesn't exist, run `./helio/blaze.sh` first.

### Step 5: Replay each crash

For each extracted `crash-NNNNNN` directory:

1. **Start Dragonfly** on a non-conflicting port (use 16399) with output captured:

```bash
./build-dbg/dragonfly --port 16399 --logtostderr --proactor_threads 1 \
    --dbfilename="" > /tmp/fuzz-repro-{run_id}/dragonfly-NNNNNN.log 2>&1 &
DFPID=$!
```

2. **Wait for ready** — poll with `redis-cli -p 16399 ping` until PONG (up to 15s).

3. **Replay** using the repo's `fuzz/replay_crash.py`:

```bash
python3 fuzz/replay_crash.py \
    /tmp/fuzz-repro-{run_id}/extracted/crash-NNNNNN/crashes \
    NNNNNN 127.0.0.1 16399
```

4. **Wait briefly** (1-2s), then check if the process is still alive:

```bash
kill -0 $DFPID 2>/dev/null
redis-cli -p 16399 ping
```

5. **Collect result**:
   - If process died: read the log file for the crash output and stack trace.
   - If process is still alive: the crash is non-deterministic (timing-dependent).
     Kill it with `kill $DFPID` and note this in the report.

6. **Decode the crash input**: Use both `strings` and `xxd` on the crash input
   file (`id:NNNNNN,...`). `strings` gives a clean view of the Redis commands;
   `xxd` shows the full binary content for completeness.

7. **Read the source at the crash location**: Use the stack trace to identify
   the source file and line number of the assertion/crash, then read that code
   to provide a more informed analysis.

### Step 6: Produce the report

Print a structured report to the user. Format:

```
## Fuzz Crash Report

**Run**: {url}
**Artifacts**: {number} crash(es) found

---

### Crash NNNNNN

**Reproduced**: Yes / No (non-deterministic)
**Signal**: SIGABRT (6) / SIGSEGV (11) / etc.

**Stack trace**:
\```
<cleaned up stack trace from the log, showing function names>
\```

**Failing assertion** (if SIGABRT):
\```
<the CHECK/DCHECK failure message>
\```

**Crash input** (decoded):
\```
<the Redis commands from xxd/strings of the crash input file>
\```

**Analysis**:
<1-3 sentences explaining the likely root cause based on the stack trace,
the assertion message, and the crash input. Identify the source file and
line number. Suggest what to investigate.>
```

### Step 7: Cleanup

After reporting, clean up the dragonfly process if still running:

```bash
kill $DFPID 2>/dev/null
```

Do NOT delete `/tmp/fuzz-repro-{run_id}/` — the user may want to inspect it.

## Important Notes

- Use port **16399** to avoid conflicts with any running Dragonfly instance.
- The replay script is at `fuzz/replay_crash.py` in the project root.
- Crashes from persistent-mode fuzzing depend on accumulated state from
  RECORD files. The replay script handles this automatically.
- Some crashes are non-deterministic (thread timing). If replay doesn't
  crash, note this clearly — it doesn't mean the bug is invalid.
- If `gh run download` fails with permissions, suggest the user authenticate
  with `gh auth login`.
