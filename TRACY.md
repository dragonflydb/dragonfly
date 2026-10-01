# Tracy Profiling for Dragonfly

[Tracy](https://github.com/wolfpld/tracy) profiling infrastructure for Dragonfly.
This document is the single source of truth for building the tools, capturing traces, reading them,
and extending the instrumentation. Its commands use the V2 I/O loop
(`--enable_resp_io_loop_v2`) as a concrete example, but the SDK and workflow apply to any
instrumented Dragonfly code path.

- **Official Tracy manual (PDF):** <https://github.com/wolfpld/tracy/releases/latest/download/tracy.pdf>
- **Tracy repo:** <https://github.com/wolfpld/tracy> (we pin **v0.14.1**)

> **Zero overhead when off.** All instrumentation is compiled out unless you build with
> `-DWITH_TRACY=ON`. The default build has no Tracy code at all.

---

## TL;DR (the reliable workflow)

```bash
# 1. Build Dragonfly with selected Tracy zones (on-demand client).
./helio/blaze.sh -release -DUSE_MOLD=ON -DWITH_AWS=OFF -DWITH_TRACY=ON \
  -DDFLY_TRACY_SCOPES=connection,dispatch,squasher,reply
cd build-opt && ninja dragonfly && cd ..

# Sampling and context switches are opt-in; see §6 when either is needed.

# 2. Run the server (it listens for a profiler on :8086, but records nothing until one connects)
taskset -c 0,1 ./build-opt/dragonfly --proactor_threads=2 --enable_resp_io_loop_v2=true --port=6379

# 3. Capture 15 s to a file while load runs (headless, rock solid).
#    $TRACY = your Tracy tools checkout (see §2: `git clone …; export TRACY=$PWD`).
$TRACY/capture/build/tracy-capture -o /tmp/df.tracy -a 127.0.0.1 -p 8086 -f -s 15
memtier_benchmark -s127.0.0.1 -p6379 -t2 -c20 --pipeline=30 --ratio=1:1 --test-time=15

# 4a. Open the FILE in the GUI (stable — avoid a live connect):
$TRACY/profiler/build/tracy-profiler /tmp/df.tracy
# 4b. …or dump stats headless (no GUI):
$TRACY/csvexport/build/tracy-csvexport -e /tmp/df.tracy > /tmp/zones.csv
```

## Choosing Tracy Data and Overhead

Tracy has two layers of control: CMake decides which instrumentation exists in the binary, then
server-startup flags decide which compiled instrumentation emits for one capture.

### Terminology

- A **zone** is one numbered or named, timed instrumentation site. Each registered selectable zone
  has a stable numeric ID and a display name.
- A **named scope** is a build-time and runtime selection label for a collection of zones. Scope
  labels are Dragonfly instrumentation policy, not fixed Tracy SDK concepts.
- The current V2 I/O-loop instrumentation uses the named scopes `connection`, `dispatch`,
  `squasher`, `reply`, and `memory`. Future subsystems do not need to use those labels.
- A **manual zone selector** selects exact registered zones by numeric ID or display name. It is
  independent of named-scope selection.
- A **zone value** is optional numeric metadata attached to one occurrence of the current zone,
  such as a batch size or counter, and shown with that zone in the profiler. When its zone is
  enabled, its zone value is enabled too. It does not create a plot or appear in aggregate zone
  statistics CSV; use a plot when the value itself needs time-series export or analysis.
- A **plot** is a timestamped numeric series, separate from zone timing.

### Basic profiling configuration

This build makes every named scope and every registered manual zone available. Runtime flags select
which of those sites emit, without sampling, context-switch tracing, or forensic detail:

```bash
./helio/blaze.sh -release -DUSE_MOLD=ON -DWITH_AWS=OFF -DWITH_GCP=OFF \
  -DWITH_TRACY=ON -DWITH_TRACY_PLOTS=ON \
  -DWITH_TRACY_SAMPLING=OFF -DWITH_TRACY_CONTEXT_SWITCH=OFF \
  -DDFLY_TRACY_SCOPES=all -DDFLY_TRACY_MANUAL_ZONES=all
```

For an exact-zone-only capture, disable named scopes and select the desired manual zones (tracy_scopes default's is `all`):

```bash
./build-opt/dragonfly --tracy_scopes= --tracy_manual_zones=89,97,99,100
```

`-release` profiles optimized server behavior. `-DUSE_MOLD=ON` speeds linking when Mold is
available, but has no profiling-semantic effect. `-DWITH_AWS=OFF` and `-DWITH_GCP=OFF` keep
unrelated cloud dependencies out of a local profiling build; enable them only when profiling those
integrations.


### Build-time controls (CMake)

Changing these values requires reconfiguring and rebuilding Dragonfly.

| CMake option | Default | Allowed values | Effect and relationships |
|---|---|---|---|
| `WITH_TRACY` | `OFF` | `ON`, `OFF` | Includes the Tracy client. With `OFF`, there is no Tracy client or manual instrumentation code. All other Tracy CMake controls require this option to be `ON`. |
| `WITH_TRACY_FIBERS` | `OFF` | `ON`, `OFF` | Enables fiber-lane attribution. Keep it off for focused captures: when enabled, every scheduled fiber is registered with Tracy, including fibers with no selected zones. |
| `DFLY_TRACY_SCOPES` | empty | Current V2 labels: comma-separated `connection`, `dispatch`, `squasher`, `reply`, `memory`, or `all` | Compiles named scope groups only. `all` compiles every named scope. |
| `DFLY_TRACY_MANUAL_ZONES` | empty | `all` alone, or comma-separated stable numeric IDs / case-insensitive display names | Independently compiles exact manual zones so startup can narrow the emitted set. `all` compiles every registered manual zone; a shorter list produces a leaner profiling binary. |
| `WITH_TRACY_PLOTS` | `OFF` | `ON`, `OFF` | Includes arbitrary numeric application telemetry as timestamped plots. Keep it on for the focused default; turn it off for zone-only captures to avoid plot call sites, state, and trace volume. |
| `WITH_TRACY_SAMPLING` | `OFF` | `ON`, `OFF` | Enables Tracy statistical call-stack sampling. Leave it off by default: it needs Linux `perf` access, adds sampling overhead, and increases trace volume. |
| `WITH_TRACY_CONTEXT_SWITCH` | `OFF` | `ON`, `OFF` | Enables Tracy context-switch tracing. Leave it off by default; enable it only for scheduler or blocking analysis because it needs additional system tracing data and permissions. |
| `WITH_TRACY_FORENSIC` | `OFF` | `ON`, `OFF` | Adds high-volume nested per-command detail within the selected compiled groups. Leave it off for ordinary captures because it can substantially increase trace size and perturb hot paths. |

### Server-startup controls (runtime)

These flags select data from the instrumentation already compiled into the binary; they do not add
new sites without a rebuild.

| Server flag | Default | Effect and relationships |
|---|---|---|
| `--tracy_scopes=...` | `all` | Selects named scope groups only. `all` enables every compiled named scope. Pass an empty value to disable named scopes. |
| `--tracy_manual_zones=...` | empty | Independently selects exact manual IDs or names. `all` selects every compiled manual zone. The requested list is intersected with the build-time list. An explicit empty value disables manual zones; an invalid value aborts startup. |
| `--tracy_sampled_scopes=...` | empty | Selects compiled named scopes that emit only during a fiber-switch sample window. A sampled selection takes precedence over the same always-on scope. |
| `--tracy_sampled_manual_zones=...` | empty | Selects exact manual zones that emit only during a fiber-switch sample window. The list is intersected with the build-time manual list. |
| `--tracy_sample_every_fiber_switches=N` | `0` | Opens a window every $N$ scheduler fiber switches per proactor. It and `--tracy_sample_window_fiber_switches` must both be positive when either sampled selector is used. |
| `--tracy_sample_window_fiber_switches=N` | `0` | Number of fiber switches in each sample window. It must not exceed `--tracy_sample_every_fiber_switches`. |
| `--tracy_fibers=...` | empty | Selects registered fiber lanes and zones. Entries can be exact names, numeric connection aliases (`6` selects `DflyConn_6`), or known groups: `connections`, `dispatchers`, `l2_workers`, `shard_workers`, `periodic_workers`, `workers`, `acceptors`, `watchers`. It is **ORed** with `--tracy_proactors`. |
| `--tracy_proactors=...` | empty | Selects every fiber lane and Tracy zone on numeric proactor IDs, such as `1,2`. It is **ORed** with `--tracy_fibers`. |
| `--tracy_queue_connections=...` | empty | With `WITH_TRACY_PLOTS=ON`, further restricts V2 queue/lifecycle plots to client IDs. Empty selects all clients allowed by `--tracy_proactors`. When both queue filters are set, both must match. |
| `--tracy_queue_proactors=...` | empty | With `WITH_TRACY_PLOTS=ON`, further restricts V2 queue/lifecycle plots to proactor IDs. Empty selects all proactors allowed by `--tracy_proactors`. When both queue filters are set, both must match. |
| `--tracy_backpressure_plots` | `false` | With `WITH_TRACY_PLOTS=ON`, emits per-proactor pipeline bytes and configured limits only when a selected V2 connection parks on backpressure. |

In `DFLY_TRACY_SCOPES` and `--tracy_scopes`, `all` means every named scope. In
`DFLY_TRACY_MANUAL_ZONES`, `all` means every registered manual zone; in
`--tracy_manual_zones`, it means every compiled manual zone. For both manual-zone selectors,
and for both named-scope selectors, `all` must be used alone.

`--tracy_fibers` groups expand to these stable names:

| Group | Selected names |
|---|---|
| `connections` | `DflyConn_<id>` |
| `dispatchers` | `_dispatch_p<N>` |
| `l2_workers` | `l2_queue_p<proactor>/<worker>` |
| `l2_workers` | `l2_queue_p<proactor>/<worker>` |
| `shard_workers` | `shard_queue_p<proactor>/<worker>` |
| `periodic_workers` | `heartbeat_periodic<N>`, `shard_handler_periodic<N>` |
| `workers` | Every `l2_workers`, `shard_workers`, and `periodic_workers` lane |
| `acceptors` | `AcceptLoop_p<N>` |
| `watchers` | `ConnectionsWatcher_p<N>` |

Groups are fixed names, not wildcard expressions. An unrecognized entry is treated as an exact
fiber name, so `_dispatch_p0` and `l2_queue_p0/1` remain valid precise selectors.

### Manual-zone selection

Build-time and runtime selection are separate. `DFLY_TRACY_MANUAL_ZONES` decides which manual
zones exist in the binary; `--tracy_manual_zones` decides which of those zones emit in one server
run.

| Build | Runtime flags | Result |
|---|---|---|
| `-DDFLY_TRACY_MANUAL_ZONES=all` | `--tracy_manual_zones=all` | Emits every registered manual zone. |
| `-DDFLY_TRACY_MANUAL_ZONES=all` | `--tracy_manual_zones=89,97,99,100` | Emits only those four zones. |
| Any non-empty build list | `--tracy_manual_zones=all` | Emits every manual zone compiled by that build, not the whole registry. |
| Any build list | `--tracy_manual_zones=` or omitted | Emits no manual zones. |

For an exact-zone capture, always pass `--tracy_manual_zones=<list>` explicitly. Do not rely on
the `all` default.

### Runtime Targeting and Sampling

The runtime decisions are composed in this order:

1. A trace site must match a target. `--tracy_fibers=7,_dispatch_p0,l2_queue_p0/1` retains only
  those fiber lanes and their zones; `7` is expanded to `DflyConn_7`. `dispatchers` selects every
  `_dispatch_p<N>` lane and `l2_workers` every `l2_queue_p<proactor>/<worker>` lane. `workers` selects
  both queue worker groups and periodic shard workers. `--tracy_proactors=1,2` adds every fiber
  lane and zone on proactors `1` and `2`; it does not exclude explicitly selected fibers. With
  both filters empty, every lane and target matches.
2. The site must match the requested named scope or manual zone.
3. If that site was selected by a sampled scope or sampled manual zone, its proactor-local sample
   window must be active.

Proactor inputs are numeric comma-separated IDs. Fiber inputs are exact names or numeric
connection aliases: `6` means `DflyConn_6`. They are not checked against the running server, so a
name or ID that does not exist simply matches no events.

Helio emits a Tracy fiber-enter event on every fiber switch so zones can be attributed to their
fiber lanes. `--tracy_fibers` limits registration to selected names, while `--tracy_proactors`
adds every lane on selected proactors. Plot filters affect plots but not lane registration.

Sampled selection has precedence over always-on selection for the same site. For example,
`--tracy_manual_zones=45 --tracy_sampled_manual_zones=45` samples zone `45`; it does not emit it
continuously. Sample windows use each proactor's scheduler switch epoch, not wall-clock time or a
per-connection clock. A narrow window can therefore contain no work from a particular connection.

```bash
# Record selected manual zones for one connection fiber, with named scopes disabled.
./build-opt/dragonfly --tracy_scopes= --tracy_manual_zones=45,30,31 \
  --tracy_fibers=6

# Sample all compiled named scopes only on Proactor0: 200 scheduler switches per 1000-switch cycle.
./build-opt/dragonfly --tracy_sampled_scopes=all \
  --tracy_sample_every_fiber_switches=1000 \
  --tracy_sample_window_fiber_switches=200 --tracy_proactors=0

# Record selected connections plus every dispatcher and shard worker.
./build-opt/dragonfly --tracy_scopes= --tracy_manual_zones=45 \
  --tracy_fibers=6,12,dispatchers,workers
```

### Exact Manual Zones

The authoritative registry is [src/facade/tracy_manual_zones.h](src/facade/tracy_manual_zones.h):
it assigns stable IDs by category: timing and wait zones are `1-107`; plots are `500-519`.
The gap is reserved so the categories remain visually distinct. Do not renumber or reuse an ID;
append new zones in the appropriate range. Grouped source macros use its symbolic token, and Tracy
display names come only from the registry, so exact selection never needs a runtime zone-name lookup.

Both manual-zone selectors accept `all` alone, or comma-separated IDs and exact names
(case-insensitive):

```bash
# Build a binary where only these exact zones are available.
./helio/blaze.sh -release -DWITH_TRACY=ON \
  -DDFLY_TRACY_MANUAL_ZONES=72,Squasher.Hop.Callback

# Emit exactly those compiled zones. Named scopes are not enabled in this run.
./build-opt/dragonfly --tracy_scopes= \
  --tracy_manual_zones=InvokeCmd.Handler,97
```

The build and runtime lists are intersected. For example, a binary built with `72,97` cannot emit
another zone, even if that other zone is named at startup. Named scopes and manual zones are
independent, so `--tracy_scopes=dispatch --tracy_manual_zones=97` emits all compiled dispatch zones
plus zone `97`. An empty or omitted manual-zone selector emits no manual zones.

The IDs most useful for the V1/V2 squashing investigation are `72` (`InvokeCmd.Handler`), `73`
(`Squash.DispatchBatch`), `77` (`Squash.Dispatch.Command`), `89` (`Squasher.Hop.Work`), and `97`
(`Squasher.Hop.Callback`). They are ordinary registry entries, not a separate comparison scope.

### Current V2 Example Scopes

This is the current I/O-loop V2 layout, not a generic SDK scope taxonomy.

| Scope | Main zones and data | Source owner |
|---|---|---|
| `connection` | input, parse loops, idle/backpressure waits, control handling, migration, and V2 queue-state plots when `WITH_TRACY_PLOTS=ON` | `src/facade/dragonfly_connection.cc` |
| `dispatch` | command dispatch and execution, including `Dispatch.*`, `Squash.Dispatch.*`, `InvokeCmd.Handler`, and `V2.ExecuteBatch` | `src/server/main_service.cc`, `src/facade/dragonfly_connection.cc` |
| `squasher` | pipeline squash structure, shard hops, scheduling, merge, and squasher wait zones | `src/server/multi_command_squasher.cc`, `src/facade/dragonfly_connection.cc` |
| `reply` | reply batching, send/release/flush, plus `ReplyBuilder.*` when forensic detail is compiled | `src/facade/dragonfly_connection.cc`, `src/facade/reply_builder.cc` |
| `memory` | connection memory-usage refresh, computation, and application | `src/facade/dragonfly_connection.cc` |
`ReplyBuilder.*` zones specifically require both `reply` in `DFLY_TRACY_SCOPES` and
`-DWITH_TRACY_FORENSIC=ON` at build time. They can then be enabled or disabled with the runtime
`reply` scope like any other compiled group.

### Build a focused binary

This binary contains dispatch and pipeline-squasher instrumentation for exploratory captures:

```bash
./helio/blaze.sh -release -DUSE_MOLD=ON -DWITH_AWS=OFF -DWITH_TRACY=ON \
  -DDFLY_TRACY_SCOPES=dispatch,squasher
```

To have every current group available for later runtime selection, use
`-DDFLY_TRACY_SCOPES=all`. To collect nested, high-volume detail as well, add
`-DWITH_TRACY_FORENSIC=ON` to the build command. `DFLY_TRACY_SCOPES` is a CMake cache value, so
pass it explicitly on later reconfigures when changing the selected set.

### Select groups at server startup

The default `--tracy_scopes=all` emits every group compiled into the binary. Select a subset for
one run without rebuilding:

```bash
# From a binary built with dispatch,squasher available.
./build-opt/dragonfly --tracy_scopes=dispatch

# Emit both compiled groups.
./build-opt/dragonfly --tracy_scopes=dispatch,squasher

# Emit no grouped manual zones. Useful for measuring the enabled Tracy client itself.
./build-opt/dragonfly --tracy_scopes=
```

Requesting a group that was excluded by `DFLY_TRACY_SCOPES` is rejected at startup. Rebuild with
that group included when it is needed. For an unbiased performance baseline, use a separate binary
built with `-DWITH_TRACY=OFF`; an enabled Tracy build remains an attribution tool, even when its
manual groups are disabled.

### Five Common Configurations

Assume the Tracy-enabled binary was built with
`-DDFLY_TRACY_SCOPES=dispatch,squasher`. The runtime flag can remove or restore either compiled
group, but it cannot add `connection`, `reply`, or `memory` until a rebuild includes them.

| # | Build and server configuration | Manual zone result | Expected relative cost |
|---|---|---|---|
| 1 | `-DWITH_TRACY=OFF` | No Tracy client, fibers, manual zones, or scope checks | lowest |
| 2 | `-DWITH_TRACY=ON`, then default runtime setting | `dispatch` and `squasher` emit because the default is `--tracy_scopes=all` | higher than 1 |
| 3 | Same binary, `--tracy_scopes=` | No grouped manual zones emit | lower than 2; still above 1 |
| 4 | Same binary, `--tracy_scopes=dispatch` | Only `dispatch` emits; removes `squasher` | normally between 2 and 3 |
| 5 | Same binary, `--tracy_scopes=all` | Same result as 2: every compiled group emits | same as 2 |

The largest profiling cost is an `all` build with `--tracy_scopes=all`,
`-DWITH_TRACY_FORENSIC=ON`, and a Tracy viewer or capture client connected. With no collector
attached, on-demand Tracy does not record a capture, but the Tracy-enabled binary still is not a
clean replacement for configuration 1.

---

## 1. Why so few `DFLY_TRACY_ZONE`s? (read this first)

Tracy is a **hybrid** profiler. You do **not** need a zone on every function:

- **Manual zones** (`DFLY_TRACY_ZONE`) give you *structure* — named, exact‑bounded spans for the
  phases you care about (parse, dispatch, squash, flush, …).
- **Call‑stack sampling** (§6) fills in *everything else* automatically — every function, including
  system libraries and the kernel, on every thread, with **no code changes**. This is how you see
  the fine‑grained CPU cost inside a phase, and **what the proactor is doing**.

So the philosophy is: a *handful* of well‑placed structural zones on the hot loop, then let
sampling do the rest. Adding a zone to every function would bloat the trace and slow the client for
little benefit. (Tracy manual §"Sampling profiler" / §"Call stack sampling".)

### Zone vs. scope

A **zone** is one named individual Tracy event, such as `V2.Squash.Pipeline` or
`Squasher.Hop.Callback`. It measures one lexical region of code, beginning at its macro and ending
when its C++ scope exits. A **scope** is a named group of related zones, selected at build time by
`DFLY_TRACY_SCOPES` and at startup by `--tracy_scopes`. In the current V2 I/O-loop example,
enabling `squasher` enables its compiled `V1.Squash.*`, `V2.Squash.*`, and `Squasher.*` zones.

**Color convention:** zones that are **pure wait** (fiber parked, no CPU — `await`/`yield`/cond-wait/
blocking-recv/join) are colored **red** via `DFLY_TRACY_WAIT(...)`. Everything else is normal-colored
"work that may internally preempt" — its fiber-lane gaps still reveal any preemption.

V2 queue plots include Dragonfly's client ID as `v2.conn_<id>.*`, so samples from different
connections are never merged. A capture with $N$ active connections therefore has up to $16N$ queue plots.
Use `--tracy_queue_connections=11,12` and/or `--tracy_queue_proactors=0,3` to reduce capture
volume; when both flags are set, a connection must match both filters. Empty filters select all.
For a backpressure investigation, add `--tracy_backpressure_plots`; its per-proactor plots are
emitted only when a selected V2 connection actually parks above the pipeline limit.

**Deliberately NOT zoned** (cold / not on the loop): connection-close cleanup waits
(`ClearPipelinedMessages`/`DestroyParsedQueue` `Blocker()->Wait()`), and the publisher-side
`QueueBackpressure::EnsureBelowLimit` `pubsub_ec.await` (belongs to the pub/sub subsystem, not the
connection loop). Add them only if you're chasing a shutdown/pubsub-publisher stall.

---

## 2. The Tracy tools (clone + build once)

The **client** (inside `dragonfly`) is built automatically by `-DWITH_TRACY=ON`. The three
**server-side tools** are separate binaries you build once from a Tracy checkout.

Get the source (match our pinned **v0.14.1**) and remember its root:

```bash
git clone https://github.com/wolfpld/tracy
cd tracy && git checkout v0.14.1
export TRACY=$PWD          # repo root — every tool path in this doc is written as $TRACY/…
```

> Set `TRACY` once per shell (or point it at an existing checkout). All commands below use
> `$TRACY/…`, so the doc carries **no absolute paths**.

> ⚠️ **Don't build inside `build-opt/_deps/tracy-src`.** A Dragonfly reconfigure re‑clones (and
> deletes) that directory via `FetchContent`. Keep the tools in a standalone checkout like above.

**What each tool does:**

| Tool | Role | GPU |
|---|---|---|
| **tracy-capture** | Headless recorder — connects to the on‑demand client and writes a `.tracy` file. The rock‑solid way to capture (§3). | no |
| **tracy-csvexport** | Dumps per‑zone stats from a `.tracy` file to CSV text — for scripts / AI / v1‑vs‑v2 diffs (§6.3). | no |
| **tracy-profiler** | The GUI viewer — timeline, Statistics, Sampling, source view. For human exploration (§4). | yes (OpenGL) |

**Build them** (each is an independent CMake project under the repo root):

```bash
# tracy-capture — record a trace to a file
cd $TRACY/capture   && mkdir -p build && cd build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DNO_ISA_EXTENSIONS=ON .. && make -j"$(nproc)"

# tracy-csvexport — zone stats → CSV
cd $TRACY/csvexport && mkdir -p build && cd build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DNO_ISA_EXTENSIONS=ON .. && make -j"$(nproc)"

# tracy-profiler — GUI viewer.  -DLEGACY=ON => X11/GLFW backend (this box is an X11 session; the
#   default Wayland backend aborts with "Cannot establish wayland display connection!")
cd $TRACY/profiler  && mkdir -p build && cd build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DNO_ISA_EXTENSIONS=ON -DLEGACY=ON .. && make -j"$(nproc)"
```

Resulting binaries (relative to `$TRACY`):

- `capture/build/tracy-capture`
- `csvexport/build/tracy-csvexport`
- `profiler/build/tracy-profiler`

`-DNO_ISA_EXTENSIONS=ON` avoids `-march=native`, so the binaries stay portable across machines.

---

## 3. Capturing a trace

Dragonfly uses **on‑demand** mode: the client opens port **8086** and starts recording only once a
viewer or capture tool connects. Nothing is recorded (and there is no overhead beyond the idle
listener) until you connect.

### 3a. Headless capture → file (recommended)

```bash
tracy-capture -o /tmp/df.tracy -a 127.0.0.1 -p 8086 -f -s 15
```

| Flag | Meaning |
|---|---|
| `-o <file>` | output trace file |
| `-a <host>` | client address (use the server host; `127.0.0.1` locally) |
| `-p 8086` | client port |
| `-f` | overwrite the output file |
| `-s <sec>` | capture for N seconds then save and exit |
| `-m <MB>` | memory limit |

Run your load (e.g. `memtier_benchmark …`) during those N seconds so the trace has activity.

### 3b. GUI, live connect

```bash
tracy-profiler            # opens the connection dialog; pick the discovered "dragonfly" and Connect
```

Live connect is more fragile than opening a file (any hiccup takes the window down). Prefer 3a +
then open the file (§4).

---

## 4. Reading the trace (GUI)

Open a **file** (stable): `tracy-profiler /tmp/df.tracy`.

- **Top strip**: frame markers + a **CPU usage** overview graph. This is only a summary — your data
  is in the rows below it.
- **Timeline (main area)**: one **row per OS thread** and one **row per fiber** (fibers are the
  green "green‑thread" tracks). At full zoom, microsecond zones are invisible — **zoom in**
  (mouse‑wheel over a busy spot, e.g. the CPU spike) until zones appear as colored boxes. Left‑drag
  to pan; drag on the time ruler to zoom to a range. Hover a zone for its exact time; click it for
  the source location.
- **Statistics** (toolbar): a sortable table of every zone (total/mean/median/min/max, count). The
  fastest way to see per‑phase cost — start here.
- **Find zone** (toolbar): type `V2.` to list our zones; click one for its histogram + all
  instances.
- **Info** (toolbar): the authoritative summary of the loaded trace (zone count, threads, etc.).
  If Statistics looks empty, check Info to confirm the file actually has zones (an idle/empty
  capture will legitimately show 0).

**Headless alternative (no GUI):**

```bash
tracy-csvexport -e /tmp/df.tracy > zones.csv   # -e = self time; omit for inclusive
```

Columns: `name, src_file, src_line, total_ns, total_perc, counts, mean_ns, min_ns, max_ns, std_ns`.

---

## 5. Seeing **WAIT** vs **WORK** separately

This is the crux of a fiber server: most "time" in a zone over a suspending function is the fiber
**parked**, not CPU. Three complementary ways to see it:

1. **The `V2.IdleWait` / `V2.Squash` / `V2.Flush` zones** are the waits, named. In **csvexport**
   they show large `mean_ns` (hundreds of µs) because they include park time. That is *by design* —
   they answer "how long between doing useful work."
2. **Fiber yield regions.** In the timeline, when a fiber is parked its lane shows a distinct
   *yield* state (drawn like a context‑switch region — Tracy manual §"Fiber work and yield
   states"). You literally see the gaps where the fiber is asleep.
3. **Wait stacks** (needs sampling, §6). Tracy captures a call stack at every context switch, so it
   can tell you *why* a fiber/thread was suspended and what it was doing when it resumed
   (Tracy manual §"Wait stacks"). This turns "it's waiting" into "it's waiting **here**, on **this**
   call."

**Rule of thumb:** the true CPU cost is the *sum of the non‑suspending zones* (`V2.Parse`,
`V2.ExecuteBatch`, `V2.SendReply`, `V2.ReadInput`) plus whatever sampling attributes inside
`V2.Squash`. The big `Squash`/`Flush`/`IdleWait` totals are wait, not CPU.

---

## 6. Seeing what the **proactor** (and the kernel) is doing — sampling

You do **not** instrument the proactor by hand. Turn on **call‑stack sampling** and Tracy shows what
every thread — including the proactor thread doing `io_uring_enter` / `epoll_wait` and its
callbacks — was executing, sampled at ~10 kHz, plus kernel time. This is the "state‑of‑the‑art"
part and needs **no code changes**.

On Linux, sampling and context‑switch capture are automatic **if the kernel allows it**. See
§6.1 for the exact kernel knobs, why they matter, and the tradeoffs.

Then in the profiler:

- The **Statistics** window has an **Instrumentation ⟷ Sampling** toggle — switch to **Sampling**
  to see a flat/￼hierarchical profile of sampled call stacks (where CPU actually goes, including the
  proactor and syscalls).
- Right‑click a thread/fiber row → sampling & "wait stacks" context menus.
- Ghost zones (grey) on the timeline are sampled frames shown where you have no manual zone.

Notes (Tracy manual §"Call stack sampling"):
- Frequency: `TRACY_SAMPLING_HZ`. Disable: `TRACY_NO_SAMPLING`. Disable system tracing:
  `TRACY_NO_SYSTEM_TRACING` (compile) or env var `=1` (runtime).
- If `dmesg` shows *"perf: interrupt took too long, lowering …max_sample_rate"*, raise
  `kernel.perf_event_max_sample_rate` or sampling goes silently off.

### 6.1 Kernel setup for sampling (3 knobs, per boot)

Call-stack sampling is an opt-in build capability: configure with
`-DWITH_TRACY_SAMPLING=ON`. Whether an enabled build actually *gets* stacks is decided at
**capture time** by the Linux kernel's `perf` permissions. If these aren't set, you get the
half‑granted signature seen in an early capture: **Info → `Hardware samples: 304,893` but
`Call stack samples: 0`** (counters sampled, stacks refused). Set these **three** sysctls **once per
boot** *before* capturing:

```bash
# 1) allow call-stack (callchain) sampling + context switches. <=1 needed; -1 = everything.
echo 1     | sudo tee /proc/sys/kernel/perf_event_paranoid
# 2) (optional) expose kernel symbols so kernel frames aren't 0x0000 in stacks.
echo 0     | sudo tee /proc/sys/kernel/kptr_restrict
# 3) raise the max sample rate so the kernel doesn't silently throttle sampling off.
echo 100000 | sudo tee /proc/sys/kernel/perf_event_max_sample_rate

# verify:
cat /proc/sys/kernel/perf_event_paranoid        # -> 1 (or lower)
cat /proc/sys/kernel/kptr_restrict              # -> 0
cat /proc/sys/kernel/perf_event_max_sample_rate # -> comfortably above TRACY_SAMPLING_HZ (~10k)
```

| # | sysctl | Set to | What it unlocks |
|---|---|---|---|
| 1 | `kernel.perf_event_paranoid` | `1` (or `-1`) | The gate. `>=2` (common default) permits hardware counters but **blocks stack collection** → `Call stack samples: 0`. `<=1` lets Tracy record call stacks + context switches. |
| 2 | `kernel.kptr_restrict` | `0` | Optional. Reveals kernel symbol addresses so **kernel frames** in stacks resolve to names instead of `0x0`. User‑space stacks work without it. |
| 3 | `kernel.perf_event_max_sample_rate` | high (e.g. `100000`) | Safety valve. If too low, the kernel logs *"perf: interrupt took too long, lowering max_sample_rate"* and **sampling quietly stops** mid‑capture. Keep it well above `TRACY_SAMPLING_HZ` (~10 kHz). |

**Why enable it (the payoff).** Manual `DFLY_TRACY_ZONE`s are *black boxes* — e.g. instrumentation
shows ~100 % of commands flow through `V2.Squash` but can't say **what inside it** costs CPU.
Sampling opens the box with **no code changes**:
- breaks `V2.Squash` CPU into real functions (command handlers, DashTable lookups, hashing,
  `mimalloc`, `memcpy`);
- shows the **proactor** thread (`io_uring_enter`/`epoll_wait` + callbacks) and kernel time;
- **wait stacks**: *why* a fiber parked and *where* it resumed;
- grey **ghost zones** fill the un‑instrumented gaps on the timeline.

**Tradeoffs (why it's off by default).**
- **Security.** Lowering `perf_event_paranoid` (and `kptr_restrict`) system‑wide exposes kernel
  addresses and `perf` side‑channels to unprivileged processes. Fine on a dev laptop; **do not** do
  this on shared/prod hosts.
- **Overhead / perturbation.** Sampling interrupts every thread ~10 kHz and unwinds a stack each
  time — inflates trace size and slightly nudges timings. Take **clean latency/throughput numbers
  from a non‑sampled run**; use sampling for attribution.
- **Statistical, not exact.** It's a probabilistic profile: functions appear proportionally to how
  often they run, not with exact per‑call times. Great for "where does the bulk of CPU go," not for
  "this one call took exactly X."

**Persist across reboots** (optional — only on a machine where the security relaxation is acceptable):

```bash
sudo tee /etc/sysctl.d/99-tracy.conf >/dev/null <<'EOF'
kernel.perf_event_paranoid = 1
kernel.kptr_restrict = 0
kernel.perf_event_max_sample_rate = 100000
EOF
sudo sysctl --system
```

**Gotcha.** These must be set *before* the capture — an existing `.tracy` file **cannot** be
retrofitted with sampling. If stacks come back shallow / `[unknown]` even with the sysctls set, that's
**unwinding**, not permission: ensure the `-release` binary kept symbols, or rebuild with
`-DCMAKE_CXX_FLAGS=-fno-omit-frame-pointer`.

### 6.2 Wait stacks (context-switch call stacks) — the `-1` level

The three knobs above give you **CPU sampling** and context-switch *regions* (when each thread is
on/off CPU). They do **not** by themselves give **wait stacks** — the call stack captured *at each
context switch* that answers "**what** is this fiber/thread blocked **on** (futex / `epoll_wait` /
`io_uring_enter`) and where did it resume." Signature of the gap: **Info → `Context switch regions`
> 0 but `Context switch samples: 0`**, and no "Wait stacks" menu.

To record them you need the **most permissive** perf level (`-1`, not `1`), and usually a **root**
capture:

```bash
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid      # -1 = also allow ctx-switch stacks
# capture as root for the fullest kernel wait stacks:
sudo $TRACY/capture/build/tracy-capture \
  -o /tmp/df_waitstacks.tracy -a 127.0.0.1 -p 8086 -f -s 15
```

Verify **Info → `Context switch samples` > 0**, then **right-click a fiber/thread row → "Wait
stacks"** (also available as a mode in Statistics).

**Worth it for Dragonfly?** *Occasionally.* Dragonfly is wait-dominated, so wait stacks are valuable
when chasing a **specific stall** (why a fiber parks in a hot path). For everyday CPU-cost work the
CPU sampling from §6.1 already covers most of the value — treat `-1` + wait stacks as a deep-dive
tool, not the default (it's also the biggest security relaxation and needs root).

### 6.3 Headless analysis with `tracy-csvexport` (scripts / AI, no GUI)

The GUI is for **human** exploration; for **scripted or AI‑assisted** analysis, distill the
(multi‑MB) binary into a tiny text table with `tracy-csvexport` (built in §2) and analyze *that*.

```bash
CSV=$TRACY/csvexport/build/tracy-csvexport

# basic: aggregated per-zone stats (inclusive time) → CSV on stdout
"$CSV" /tmp/df.tracy > /tmp/zones.csv

# self time instead of inclusive (subtracts child-zone elapsed time)
"$CSV" -e /tmp/df.tracy > /tmp/zones_self.csv

# only the V2 loop zones (name filter)
"$CSV" -e -f V2. /tmp/df.tracy

# per-instance rows (NOT aggregated) — one line per zone occurrence; big output
"$CSV" -u /tmp/df.tracy > /tmp/every_zone.csv
```

| Flag | Meaning |
|---|---|
| *(none)* | aggregated per‑zone, **inclusive** time |
| `-e` / `--self` | use **self** elapsed time (child-zone duration subtracted) |
| `-f <name>` / `--filter` | only zones whose name contains `<name>` (e.g. `V2.`) |
| `-c` / `--case` | make the `-f` filter case‑sensitive |
| `-u` / `--unwrap` | emit **one row per zone instance** instead of aggregates (large) |
| `-s <sep>` / `--sep` | CSV separator (default `,`) |
| `-m` / `--messages` | export timeline **messages** instead of zones |

Run `"$CSV" -h` for the full list. Output columns (aggregated mode):
`name, src_file, src_line, total_ns, total_perc, counts, mean_ns, min_ns, max_ns, std_ns`.

**Why this is the AI‑friendly path.** Each aggregated export is a handful of rows — a **57 MB trace
becomes ~14 lines**, which an agent can read and diff directly (no GUI, no binary parsing). Typical
flow: export v1 and v2, hand both CSVs to the agent, ask for the diff.

```bash
"$CSV" -e /tmp/df_v1.tracy > /tmp/v1.csv
"$CSV" -e /tmp/df_v2.tracy > /tmp/v2.csv
```

**Two caveats.**
- For a meaningful **v1‑vs‑v2 diff, capture both under identical load and duration** — otherwise only
  the *structure* (which zones exist, relative per‑call means) is comparable, not absolute totals.
- **Self time is not CPU time.** `--self` removes nested child-zone duration, but the remaining
  elapsed time can still include a parked fiber, including inside `V2.Squash` or `V2.Flush`. Use
  **Sampling** to attribute on-CPU work; use zones to explain phase latency and wait structure.
- **csvexport covers instrumentation zones only.** The **Sampling** flat profile (per‑function CPU,
  ghost zones, wait stacks) has **no** clean headless export in v0.14.1 — that part still needs the
  GUI (or paste the Sampling table).

### 6.4 Scheduler timeline exporter

`tools/tracy_scheduler_timeline/` is the headless scheduler exporter for saved Tracy **v0.14.1**
captures. Unlike `tracy-csvexport`, it reads persisted `ContextSwitchData` directly and emits a
single globally time-ordered long-format CSV of scheduled fiber intervals. It resolves saved
thread names, so rows identify the executing proactor and fiber lane (for example, `Proactor2` and
`l2_queue_p2/1`). It also reads the CPU-zone timeline for each fiber and splits scheduler
intervals at zone boundaries. The CSV columns are `start_time`, `end_time`, `duration`, `proactor`,
`fiber`, `zone_level`, `zone`, and `zone_metadata`.

Output rows are globally ordered by `start_time` across all proactors and fibers. Rows tied at the
same start time use a deterministic ordering.

By default, `zone` includes only outermost CPU zones, preserving the original root-only timeline
segmentation. Root rows use `zone_level=1`; scheduler gaps have `zone_level=0` with empty `zone`
and `zone_metadata`. `zone_metadata` is the persisted formatted Tracy zone text for that row, such
as `conn=11 batch=1 shard=2 count=3` for `Squasher.Hop.Callback`.

Pass `--include-subzones` to export every nested CPU zone. Children use `zone_level=depth+1`, each
scheduler interval is divided at every overlapping zone start and end, and each resulting segment
has one row for every active zone ordered by ascending `zone_level`. Parent and child rows therefore
overlap intentionally; a segment with no active zone has one blank level-0 row. The exporter spools
fixed-size fiber intervals and zone records, then externally merges bounded sort chunks, so peak
memory is controlled by `--memory-records` rather than capture zone volume. A capture-relative time
window is decoded sequentially in full so saved time state remains valid, but out-of-window intervals
and zones are discarded before interval sorting and merging. It supports only Tracy v0.14.1; it
rejects every other saved-format version. Fibers that Tracy did not persist as context-switch records
cannot be recovered.

The standalone CMake target takes a local Tracy checkout explicitly; it does not modify or build
the checkout itself:

```bash
cmake -S tools/tracy_scheduler_timeline -B /tmp/tracy-scheduler-build \
  -DTRACY_ROOT=/path/to/tracy-v0.14.1
cmake --build /tmp/tracy-scheduler-build
/tmp/tracy-scheduler-build/tracy_scheduler_timeline /tmp/df.tracy /tmp/scheduler.csv \
  --memory-records 250000 --work-dir /tmp/df-scheduler-work
```

`--memory-records` bounds the number of raw 32-byte intervals held in one sort chunk; `250000` is
about 8 MiB before vector/sort overhead and is a conservative default for a busy capture.
`--work-dir` holds the temporary binary spool and external-sort chunks. It defaults to
`<timeline.csv>.tracy-scheduler-work`, is removed after success, and should be placed on a fast local
disk when the capture is large.

Use a capture-relative nanosecond window to reduce the output while preserving the full duration of
every overlapping scheduler interval:

```bash
# Intervals overlapping [1.9 s, 2.0 s).
/tmp/tracy-scheduler-build/tracy_scheduler_timeline /tmp/df.tracy /tmp/scheduler_1.9s.csv \
  --start-ns 1900000000 --end-ns 2000000000

# Intervals overlapping 100 ms beginning at 1.9 s.
/tmp/tracy-scheduler-build/tracy_scheduler_timeline /tmp/df.tracy /tmp/scheduler_1.9s_100ms.csv \
  --start-ns 1900000000 --duration-ns 100000000

# Include nested CPU zones. Parent and child rows intentionally overlap.
/tmp/tracy-scheduler-build/tracy_scheduler_timeline /tmp/df.tracy /tmp/scheduler_nested.csv \
  --include-subzones --start-ns 1900000000 --duration-ns 100000000
```

Run `tracy_scheduler_timeline --help` for all options and output columns.

The current native reader is staged around the pinned serialization format.

### Tracy SDK overview

Use these reusable infrastructure files when adding Tracy support to a new Dragonfly subsystem:

| File | Role |
|---|---|
| `CMakeLists.txt` | Defines Dragonfly's Tracy options, compiles the selected scopes/manual zones, and fetches the pinned Tracy client. |
| `src/facade/CMakeLists.txt` | Links `Tracy::TracyClient` into the facade target when `WITH_TRACY` is enabled. |
| `src/facade/tracy_support.h` | Public SDK header. It exposes generic and scoped wrapper macros, while compiling to no-ops when Tracy is disabled. Include this file from application code. |
| `src/facade/tracy_support.cc` | Owns runtime flag parsing, exact-zone selection, sampling windows, and target filters. |
| `src/facade/tracy_manual_zones.h` | Authoritative stable registry of selectable manual-zone IDs, symbols, and display names. Append new IDs; never renumber or reuse one. |

To make an application zone runtime-selectable, add its stable registry entry, use the matching
scoped macro from `tracy_support.h`, and include it in the profiling build's selected manual zones.
For short-lived local diagnostics, the generic `DFLY_TRACY_ZONE` and related generic macros are
also available.

## 7. Adding your **own** app‑level zones (no helio changes)

You never need to touch `helio/` to profile application code. The fiber lanes already exist; you
can add a zone to the relevant Dragonfly `.cc` file. Add it to a scope so build-time and runtime
selection continue to work:

```cpp
#include "facade/tracy_support.h"      // the zero-cost wrapper

void MyFunc() {
  DFLY_TRACY_ZONE("MyFunc");  // scoped span, name must be a string literal
  // ...
}
```

The currently available scoped wrappers use the V2-oriented labels `connection`, `dispatch`,
`squasher`, `reply`, and `memory`. Use the corresponding `DFLY_TRACY_<GROUP>_ZONE` or
`DFLY_TRACY_<GROUP>_WAIT` macro when one fits the subsystem; otherwise use a generic macro or
extend the SDK with the new grouping. Groups with dynamic metadata also provide matching `_VALUE`,
`_TEXT`, `_TEXT_F`, or `_PLOT` forms; keep metadata in the same lexical scope as a zone from that
group. Scoped macros take a `TracyManualZone` registry symbol such as `kDispatchCommand`, not a
string literal.

`DFLY_TRACY_ZONE`, `DFLY_TRACY_WAIT`, `DFLY_TRACY_PLOT`, and the other ungrouped macros bypass
`DFLY_TRACY_SCOPES` and `--tracy_scopes`. They are reserved for deliberate, temporary always-on
debugging in a Tracy-enabled build, not for normal Dragonfly instrumentation.

The generic macros below are no‑ops unless `-DWITH_TRACY=ON`:

| Macro | Use |
|---|---|
| `DFLY_TRACY_ZONE("name")` | scoped timing zone (RAII; ends at scope) |
| `DFLY_TRACY_ZONE_TEXT(ptr, len)` | attach text to the current zone |
| `DFLY_TRACY_ZONE_TEXT_SV(string_view)` | same, from a `string_view` |
| `DFLY_TRACY_PLOT("name", value)` | plot a number over time (e.g. queue depth) |
| `DFLY_TRACY_MESSAGE(ptr, len)` | drop a marker message on the timeline |
| `DFLY_TRACY_FRAME_MARK()` | mark a frame boundary |
| `DFLY_TRACY_THREAD_NAME("name")` | name the current OS thread |

Rules that matter:
- **One grouped zone per `{}` scope.** Grouped zone macros use a fixed variable name, so two in the
  *same* scope collide — wrap extra zones in their own `{ … }` block.
- **Names must be string literals** (they're pooled by pointer). Dynamic text goes through
  `..._TEXT` / `..._TEXT_SV`.
- Zones you add in **other libraries** (e.g. `dragonfly_lib`) work because `-DWITH_TRACY=ON`
  defines `TRACY_ENABLE`/`TRACY_FIBERS` **globally** and links the client — see §10.
- Want per‑command detail inside execution? Add `DFLY_TRACY_ZONE`s in the command handlers /
  `DispatchSquashedBatch` path. They'll nest under `V2.Squash`.

---

## 8. Build / flag reference

| Where | Flag | Effect |
|---|---|---|
| CMake | `-DWITH_TRACY=ON` | fetch + link Tracy client, define `TRACY_ENABLE`+`TRACY_FIBERS` globally, on‑demand mode |
| CMake | `-DDFLY_TRACY_SCOPES=all` | compile all current named scope groups; use a comma-separated subset for a narrower binary |
| CMake | `-DWITH_TRACY_FORENSIC=ON` | compile high-volume nested per-command detail for the selected groups |
| server | `--tracy_scopes=all` | emit all groups compiled into this binary; pass a comma-separated subset or an empty value to emit none |
| tools | `-DLEGACY=ON` | profiler uses X11/GLFW backend (needed on X11 sessions) |
| tools | `-DNO_ISA_EXTENSIONS=ON` | don't pass `-march=native` (portable tool binaries) |
| env | `TRACY_SAMPLING_HZ=N` | sampling frequency |
| env | `TRACY_NO_SAMPLING` / `TRACY_NO_SYSTEM_TRACING` | disable sampling / context switches |
| sysctl | `kernel.perf_event_paranoid<=1` | enable call‑stack sampling + context switches (§6.1) |
| sysctl | `kernel.kptr_restrict=0` | resolve kernel frames in stacks (§6.1) |
| sysctl | `kernel.perf_event_max_sample_rate` high | stop the kernel silently throttling sampling off (§6.1) |

Turn Tracy off again for clean perf numbers: `cd build-opt && cmake -DWITH_TRACY=OFF . && ninja dragonfly`.

---

## 9. Gotchas (learned the hard way)

- **Blank timeline / "Total zone count: 0"** → you almost certainly opened an **empty/idle capture**
  (check the file size and **Info** window). Capture *while load runs*.
- **Viewer/capture SEGFAULT on connect** → historically caused by emitting `TracyFiberLeave` for a
  fiber the server never saw (on‑demand). Fixed: we are **Enter‑only** (§10). If it ever returns,
  suspect an unbalanced fiber event.
- **"Cannot establish wayland display connection!"** → build the profiler with `-DLEGACY=ON` (X11).
- **`csvexport` prints only a header** → build it with statistics on (default `NO_STATISTICS=OFF`);
  and make sure the trace isn't empty.
- **Tools vanished after a rebuild** → you built them inside `_deps`; rebuild in a standalone
  `$TRACY` checkout (§2).
- Version skew: client and tools **must** be the same Tracy version (we use v0.14.1).

---

## 10. Implementation overview (handoff for another agent) - This is a draft section which should be removed when this becomes official

Everything below is **uncommitted, local** by request (including the `helio` submodule edits).

### What was changed

**Root `CMakeLists.txt`** — `WITH_TRACY` option (default OFF). When ON:
- `set(TRACY_ENABLE/TRACY_ON_DEMAND/TRACY_FIBERS ON CACHE ... FORCE)`
- `add_compile_definitions(TRACY_ENABLE TRACY_FIBERS)` — **global** defines so the guards fire in
  every TU (dragonfly + helio) regardless of how INTERFACE defs propagate across helio's custom
  `cxx_link` boundary. (We do **not** define `WITH_TRACY` as a C macro — nothing `#ifdef`s it.)
- `FetchContent` Tracy `v0.14.1`.

**`src/facade/tracy_support.h`** (new) — thin wrapper. Real Tracy macros when `TRACY_ENABLE` is
defined (propagated by linking `Tracy::TracyClient`), otherwise zero‑cost no‑ops that leave args
unevaluated via `sizeof`. Exposes `DFLY_TRACY_ZONE[_TEXT[_SV]]`, `_PLOT`, `_MESSAGE`, `_FRAME_MARK`,
`_THREAD_NAME`.

**`src/facade/CMakeLists.txt`** — `target_link_libraries(dfly_facade Tracy::TracyClient)` (plain
signature — `cxx_link` uses plain; mixing plain+keyword is a CMake error).

**`src/facade/dragonfly_connection.cc`** — the V2 zones from §1 (parse/execute/squash/reply/flush/
read/idle‑wait), per-connection queue plots, and `V2.Dispatch` command‑verb text.

**helio (submodule) — fiber awareness:**
- `helio/util/fibers/CMakeLists.txt` — link `Tracy::TracyClient` to `fibers2` when `WITH_TRACY`.
- `helio/util/fibers/detail/fiber_interface.cc` — the core piece. **Enter‑only** instrumentation:
  `#define HELIO_TRACY_FIBER_ENTER(nm) TracyFiberEnter(nm)` and **no `TracyFiberLeave` at all**.
  `HELIO_TRACY_FIBER_ENTER(name())` is called inside the `resume_with` lambda of **both**
  `SwitchTo()` and `SwitchToAndExecute()` (the only two functions that call `SwitchSetup()`, i.e.
  the only places the logical active fiber changes). The lambda runs on the *target* fiber's stack,
  so the enter names the fiber being resumed.

### Why Enter‑only (the crash fix — do not regress)

Tracy's server `Worker::ProcessFiberLeave` looks up the thread with `RetrieveThread()` (which does
**not** auto‑create) and dereferences it. Under **on‑demand**, a proactor thread that entered a
fiber *before* the viewer connected would later emit a `FiberLeave` the server never saw a matching
`FiberEnter` for → **null deref → viewer/capture SEGFAULT**. `ProcessFiberEnter` uses
`NoticeThread()` (auto‑creates) and guards its "close previous span" on a non‑null fiber, so
**Enter‑only is crash‑safe and complete**. The Tracy manual explicitly blesses Enter‑without‑Leave
for direct fiber‑to‑fiber switching, which is exactly helio's model (the dispatcher and main
context are themselves fibers; control never returns to non‑fiber code).

### Switch‑coverage proof (why 2 functions is enough)

All logical fiber switches funnel through `SwitchTo` / `SwitchToAndExecute` (the only callers of
`SwitchSetup`). `Scheduler::Preempt` → `SwitchTo`, so yield/preempt/terminate/dispatch are covered.
Other `resume()` sites are either `BOOST_USE_UCONTEXT`‑only (compiled out on our fcontext build) or
synchronous stack‑borrows (`ExecuteOnFiberStack`, dying‑fiber cleanup in `intrusive_ptr_release`)
that return to the *same* logical fiber and so need no enter/leave.

### Fiber naming

Tracy keys fiber lanes by the **pointer** value passed to `TracyFiberEnter` and lazily reads the
string via `ServerQueryFiberName` from `(const char*)ptr`. helio fibers carry a stable, per‑fiber,
null‑terminated `char name_[24]` (`FiberInterface::name()`), which satisfies Tracy's
"unique pointer" rule. (Pointer reuse across a long capture could stale a name — irrelevant for the
short captures used here.)

### Tools

Built from a standalone Tracy checkout at `$TRACY/{capture,csvexport,profiler}/build` (outside
`_deps`). Profiler built with `-DLEGACY=ON` for X11.

### Current data (2t / 20c / pipeline=30 / 2 shards, ~12 s)

`V2.Dispatch` never fires (100 % of commands go through `V2.Squash`). Wait‑dominated:
`V2.Squash` ~711 µs/call, `V2.Flush` ~458 µs/call, `V2.IdleWait` similar (fiber parked). Real CPU:
`V2.Parse` ~12.6 µs/batch (~420 ns/cmd), `V2.ExecuteBatch` ~275 ns, `V2.SendReply` ~109 ns,
`V2.ReadInput` ~76 ns. The residual ~5–7 % V2‑vs‑V1 gap is diffuse per‑command CPU; next step is
sampling inside the squash/dispatch path.

### Open ideas / next steps

- ~~Turn on sampling (§6) and read the **Sampling** stats + **wait stacks** to attribute the residual
  gap and the proactor time.~~ Done — sampling is fully operational; kernel knobs documented in §6.1–6.2.
- ~~Add `kSimpleHop` (issue #260) to extend parse-in-proactor to `DispatchCommandSimple` hops.~~
  Done — see commit message. `kSimpleHop` and `kSendReply` are now both wired. `V2.ProactorParse`
  only fires at `kSimpleHop` when new socket data arrives while the fiber is blocked on a
  single-command dispatch (`dispatch_waiting_count_ == 1`); in practice this is bursty/low-pipeline
  traffic, not bulk-pipelined load.
- Add zones inside `DispatchSquashedBatch` / command handlers to break down `V2.Squash`.
- Consider naming connection fibers uniquely if the lanes get noisy.

### V1 vs V2 performance summary (laptop, 4 proactors, SET only, pipeline-depth sweep)

*(single run, noisy laptop baseline — confirm on a remote server pair)*

| Mode | pipeline | V1 ops/s | V2 ops/s | V2/V1 |
|---|---|---|---|---|
| multi_conn | 1 | 238,429 | 217,139 | 91% |
| multi_conn | 10 | 632,417 | 594,352 | 94% |
| multi_conn | 50 | 840,149 | 817,722 | 97% |
| multi_conn | 100 | 737,841 | 715,853 | 97% |
| single_conn | 1 | 41,573 | 39,118 | 94% |
| single_conn | 10 | 140,021 | 140,479 | 100% |
| single_conn | 50 | 270,377 | 234,901 | 87% |
| single_conn | 100 | 332,731 | 269,779 | 81% |

**Root cause of V2 gap:** V1 runs 2 fibers/connection (recv + async-dispatch) that overlap
read↔execute↔flush. V2 uses 1 fiber/connection and serializes them. The `V2.Flush` zone
(~35% of V2 fiber budget at 100 conns, ~1 ms/call) is the primary bottleneck that V1 hides
behind its recv fiber. Next steps: async/double-buffered flush, and verify on a remote pair
where network RTT amplifies the overlap benefit.

- (Cleanliness) none of the V2 loop work is committed upstream; helio changes must not be pushed upstream.
