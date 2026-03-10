---
name: reproduce-fuzz-crash
description: >
  Reproduce AFL++ fuzz crashes from GitHub Actions. Use when user provides a
  GitHub Actions fuzz run URL and wants to reproduce and analyze the crash locally.
argument-hint: <github-actions-run-url>
allowed-tools: Bash, Read, Grep, Glob, Write
---

# Reproduce Fuzz Crash

Given a GitHub Actions fuzz run URL, download crash artifacts, triage them
with `fuzz/triage_crashes.sh`, and produce a crash analysis report.

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

List crash artifacts via the GitHub API, then download each as a `.zip` directly:

```bash
# List artifacts — filter for names containing "crash"
gh api repos/{owner}/{repo}/actions/runs/{run_id}/artifacts

# Download each crash artifact by ID
mkdir -p /tmp/fuzz-repro-{run_id}
gh api repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip > /tmp/fuzz-repro-{run_id}/<artifact-name>.zip
```

This gives real `.zip` files that the triage script can consume directly.

If no crash artifacts are found, report that the run has no crash artifacts and stop.

Note: there may be duplicate artifact names (same name, different IDs) from
retried jobs. Download the **most recent** one (highest artifact ID).

### Step 3: Determine mode

Infer the protocol mode from the artifact name:
- Contains "memcache" → `memcache`
- Otherwise → `resp`

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

### Step 5: Run triage_crashes.sh

For each zip file, run:

```bash
./fuzz/triage_crashes.sh ./build-dbg/dragonfly <mode> /tmp/fuzz-repro-{run_id}/<artifact-name>.zip
```

Capture the full output.

### Step 6: Analyze and report

Parse the triage output for confirmed crashes. For each confirmed crash:

1. **Read the source** at the crash location — use the stack trace to identify
   the source file and line number, then read that code.
2. **Provide analysis**: likely root cause, what to investigate.

Print a structured report:

```
## Fuzz Crash Report

**Run**: {url}
**Artifacts**: {number} crash(es) found

---

### Crash NNNNNN

**Reproduced**: Yes / No (false positive)
**Signal**: SIGABRT (6) / SIGSEGV (11) / etc.

**Stack trace**:
\```
<stack trace from triage output>
\```

**Analysis**:
<1-3 sentences explaining the likely root cause based on the stack trace,
the assertion message, and the crash input. Identify the source file and
line number. Suggest what to investigate.>
```

## Important Notes

- The triage script uses port **6379** (resp) or **11211** (memcache).
  Ensure no other Dragonfly or Redis instance is using these ports.
- The script adds `--rename_command` flags to avoid false positives from
  commands like DEBUG SLEEP that the fuzzer might generate.
- Some crashes are non-deterministic (thread timing). The script reports
  these as "FALSE POSITIVE" — note this clearly, it doesn't mean the bug
  is invalid, just that it didn't reproduce on this run.
- The script handles its own cleanup of Dragonfly processes.
- Do NOT delete `/tmp/fuzz-repro-{run_id}/` — the user may want to inspect it.
- If `gh run download` fails with permissions, suggest the user authenticate
  with `gh auth login`.
