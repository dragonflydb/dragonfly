 # Tracy Profiling for Dragonfly

Fiber‑aware [Tracy](https://github.com/wolfpld/tracy) profiling for the V2 I/O loop
(`--enable_resp_io_loop_v2`). This document is the single source of truth for building the tools,
capturing traces, reading them, and extending the instrumentation.

- **Full Tracy manual (PDF):** <https://github.com/wolfpld/tracy/releases/latest/download/tracy.pdf>
- **Tracy repo:** <https://github.com/wolfpld/tracy> (we pin **v0.11.1**, protocol 69)

> **Zero overhead when off.** All instrumentation is compiled out unless you build with
> `-DWITH_TRACY=ON`. The default build has no Tracy code at all.

---

## TL;DR (the reliable workflow)

```bash
# 1. Build Dragonfly with Tracy (on-demand client, fiber-aware)
./helio/blaze.sh -release -DUSE_MOLD=ON -DWITH_AWS=OFF -DWITH_TRACY=ON
cd build-opt && ninja dragonfly && cd ..

# 2. (once) allow call-stack sampling + context switches — see §6
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid

# 3. Run the server (it listens for a profiler on :8086, but records nothing until one connects)
taskset -c 0,1 ./build-opt/dragonfly --proactor_threads=2 --enable_resp_io_loop_v2=true --port=6379

# 4. Capture 15 s to a file while load runs (headless, rock solid).
#    $TRACY = your Tracy tools checkout (see §2: `git clone …; export TRACY=$PWD`).
$TRACY/capture/build/tracy-capture -o /tmp/df.tracy -a 127.0.0.1 -p 8086 -f -s 15
memtier_benchmark -s127.0.0.1 -p6379 -t2 -c20 --pipeline=30 --ratio=1:1 --test-time=15

# 5a. Open the FILE in the GUI (stable — avoid a live connect):
$TRACY/profiler/build/tracy-profiler /tmp/df.tracy
# 5b. …or dump stats headless (no GUI):
$TRACY/csvexport/build/tracy-csvexport -e /tmp/df.tracy > /tmp/zones.csv
```

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

The V2 loop currently has these zones (all in `src/facade/dragonfly_connection.cc`):

**Color convention:** zones that are **pure wait** (fiber parked, no CPU — `await`/`yield`/cond-wait/
blocking-recv/join) are colored **red** via `DFLY_TRACY_WAIT(...)`. Everything else is normal-colored
"work that may internally preempt" — its fiber-lane gaps still reveal any preemption.

| Zone | Loop | Meaning | Kind |
|---|---|---|---|
| `V2.RunParsePath` / `V2.ParseLoop` | V2 | one parse→execute→reply pass / cycle | work |
| `V2.Parse` | V2 | RESP protocol parsing | work |
| `ParseYield` | V1+V2 | yield mid-parse (busy-read cap) | **wait** |
| `V2.ExecuteBatch` | V2 | per-command execution loop | work |
| `V2.Dispatch` (+verb) | V2 | one `DispatchCommandSimple` | work/preempt |
| `V2.Squash` | V2 | vectorized cross-shard squash (blocks) | work/block |
| `V2.ReplyBatch` / `V2.SendReply` | V2 | reply assembly / one (coroutine) reply | work/preempt |
| `V2.ReadInput` | V2 | copy bytes from socket (non-blocking `TryRecv`) | work |
| `V2.Flush` | V2 | `sendmsg` / socket write | work/block |
| `V2.IdleWait` | V2 | parked: nothing to do | **wait** |
| `V2.Backpressure` | V2 | parked: pipeline over memory limit | **wait** |
| `V2.Control` | V2 | drain admin/pubsub (`ProcessControlMessages`) | work/preempt |
| `V2.ProactorParse` | V2 | parse-in-proactor (runs on the proactor lane) | work |
| `Migrate` | V1+V2 | cross-thread hop (cold) | **wait** |
| `V1.Recv` | V1 | blocking socket read | **wait** |
| `V1.Parse` | V1 | parse + inline sync dispatch | work/preempt |
| `V1.CondWait` | V1 | `AsyncFiber` parked on the condvar | **wait** |
| `V1.BatchYield` / `V1.QuotaYield` | V1 | `AsyncFiber` yields to the producer | **wait** |
| `V1.Backpressure` | V1 | `DispatchSingle` parked over limit | **wait** |
| `V1.Squash` | V1 | `SquashPipeline` (blocks on shards) | work/block |
| `V1.Dispatch` | V1 | sync `DispatchCommand` | work/preempt |
| `V1.Admin` | V1 | admin/pubsub dispatch | work/preempt |
| `v2.parsed_q_len` (plot) | V2 | pipeline depth over time | plot |

**Deliberately NOT zoned** (cold / not on the loop): connection-close cleanup waits
(`ClearPipelinedMessages`/`DestroyParsedQueue` `Blocker()->Wait()`), and the publisher-side
`QueueBackpressure::EnsureBelowLimit` `pubsub_ec.await` (belongs to the pub/sub subsystem, not the
connection loop). Add them only if you're chasing a shutdown/pubsub-publisher stall.

---

## 2. The Tracy tools (clone + build once)

The **client** (inside `dragonfly`) is built automatically by `-DWITH_TRACY=ON`. The three
**server-side tools** are separate binaries you build once from a Tracy checkout.

Get the source (match our pinned **v0.11.1**) and remember its root:

```bash
git clone https://github.com/wolfpld/tracy
cd tracy && git checkout v0.11.1
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

Call‑stack sampling is **not a build flag** — the Tracy client is always compiled with sampling
enabled (`-DWITH_TRACY=ON`, Tracy default). Whether you actually *get* stacks is decided at
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
  ghost zones, wait stacks) has **no** clean headless export in v0.11.1 — that part still needs the
  GUI (or paste the Sampling table).

---

## 7. Adding your **own** app‑level zones (no helio changes)

You never need to touch `helio/` to profile application code. The fiber lanes already exist; you
just drop zones into whatever Dragonfly `.cc` you want to measure.

```cpp
#include "facade/tracy_support.h"      // the zero-cost wrapper

void MyFunc() {
  DFLY_TRACY_ZONE("MyFunc");           // scoped span, name must be a string literal
  DFLY_TRACY_ZONE_TEXT_SV(some_view);  // attach dynamic text (e.g. a key/command)
  // ...
}
```

Available macros (all no‑ops unless `-DWITH_TRACY=ON`):

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
- **One zone per `{}` scope.** `DFLY_TRACY_ZONE` uses a fixed variable name, so two in the *same*
  scope collide — wrap extra zones in their own `{ … }` block.
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
- Version skew: client and tools **must** be the same Tracy version (we use v0.11.1 / protocol 69).

---

## 10. Implementation overview (handoff for another agent)

Everything below is **uncommitted, local** by request (including the `helio` submodule edits).

### What was changed

**Root `CMakeLists.txt`** — `WITH_TRACY` option (default OFF). When ON:
- `set(TRACY_ENABLE/TRACY_ON_DEMAND/TRACY_FIBERS ON CACHE ... FORCE)`
- `add_compile_definitions(TRACY_ENABLE TRACY_FIBERS)` — **global** defines so the guards fire in
  every TU (dragonfly + helio) regardless of how INTERFACE defs propagate across helio's custom
  `cxx_link` boundary. (We do **not** define `WITH_TRACY` as a C macro — nothing `#ifdef`s it.)
- `FetchContent` Tracy `v0.11.1`.

**`src/facade/tracy_support.h`** (new) — thin wrapper. Real Tracy macros when `TRACY_ENABLE` is
defined (propagated by linking `Tracy::TracyClient`), otherwise zero‑cost no‑ops that leave args
unevaluated via `sizeof`. Exposes `DFLY_TRACY_ZONE[_TEXT[_SV]]`, `_PLOT`, `_MESSAGE`, `_FRAME_MARK`,
`_THREAD_NAME`.

**`src/facade/CMakeLists.txt`** — `target_link_libraries(dfly_facade Tracy::TracyClient)` (plain
signature — `cxx_link` uses plain; mixing plain+keyword is a CMake error).

**`src/facade/dragonfly_connection.cc`** — the V2 zones from §1 (parse/execute/squash/reply/flush/
read/idle‑wait) plus the `v2.parsed_q_len` plot and `V2.Dispatch` command‑verb text.

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
