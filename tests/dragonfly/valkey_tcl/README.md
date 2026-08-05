# Valkey TCL stream tests on Dragonfly

Runs the upstream [Valkey](https://github.com/valkey-io/valkey) TCL test suite (the
classic Redis-era tests) against Dragonfly in **external-server mode** — the harness
connects to an already-running Dragonfly over `--host/--port` instead of spawning a
server. Currently scoped to the two stream suites:
`unit/type/stream` and `unit/type/stream-cgroups`.

## Layout

```
tests/dragonfly/valkey_tcl/
├── sync-valkey-tcl-tests.sh   # clone Valkey @ pinned SHA → copy harness + stream files (committed)
├── run-stream-tests.sh        # start Dragonfly + run the harness with the skiplist (committed)
├── skiplist.txt               # documented, tracked list of known-failing tests (committed)
├── README.md                  # this file (committed)
└── upstream/                  # synced harness + tests — GITIGNORED, not committed
```

The test code is never committed; it is synced from upstream at a pinned revision,
mirroring `tests/dragonfly/valkey_search/`.

## Run locally

```bash
cd tests/dragonfly/valkey_tcl
./sync-valkey-tcl-tests.sh                       # sync harness + stream tests (pinned SHA)
./run-stream-tests.sh ../../../build-dbg/dragonfly   # start DF + run suites
```

Requirements: `tclsh` 8.6, and (optionally) `redis-cli` for the readiness probe.
The run script exits non-zero if any non-skipped test fails.

## How it works

- **External mode**: `run-stream-tests.sh` starts Dragonfly (`--dbfilename= --dbnum=16`),
  waits for `PING`, then runs `tclsh test_helper.tcl --host … --port …`. In external mode
  the harness runs `FLUSHALL`/`FUNCTION FLUSH` before each block and applies overrides via
  `CONFIG SET`; it never spawns or kills the server.
- **Binary stub**: the harness's `set_executable_path.tcl` aborts unless
  `src/valkey-server` exists and is executable — even though external mode never runs it.
  The run script symlinks `upstream/src/valkey-server` to the Dragonfly binary to satisfy
  the check.
- **Tag skiplist**: `--tags "-needs:debug -needs:repl -external:skip -large-memory"` drops
  the AOF/`debug loadaof` blocks (Dragonfly has no AOF), replication blocks, and
  process-control blocks.
- **Test skiplist**: `skiplist.txt` lists individual known-failing tests (each a tracked
  Dragonfly gap) so the suite stays green over the passing subset. Remove an entry once the
  underlying gap is fixed.

Each skiplist bucket carries a one-line reason inline; the categorized baseline lives in
the bucket comments of `skiplist.txt` itself.

## CI

Runs as the `Run Valkey TCL stream tests` step of the `build` job in
`.github/workflows/ci.yml`, right after the Python regression tests and under the same
gating (`ubuntu-dev:24` + `NoSanitizers`, both Debug and Release), against the just-built
binary. The step installs `tcl`/`redis-tools`, syncs the harness, and runs the suites.
The step is a non-blocking canary (`continue-on-error`) until it proves stable in CI;
flip it to blocking to make the suite a required regression gate.
