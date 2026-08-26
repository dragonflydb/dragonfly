# Tracy Integration Plan for Dragonfly and Helio

## Purpose

This document records the design rationale, verified current state, and proposed roadmap for
fiber-aware Tracy profiling in Dragonfly and its Helio runtime. It complements [TRACY.md](TRACY.md),
which is the operational guide for building, capturing, and reading traces. This document owns the
integration architecture, invariants, extension SDK, and rollout decisions; `TRACY.md` owns the
linear user workflow and troubleshooting guide.

The goal is not to replace `perf`. The goal is to give Dragonfly engineers a reproducible,
fiber-aware view that connects infrastructure mechanics with database work:

```text
Connection / command phase
  -> Helio scheduler, queue, synchronization, and I/O activity
  -> shard / transaction execution
  -> reply and socket write
```

`perf` remains valuable for quick CPU surveys and production-safe sampling. Tracy adds a timeline,
logical fiber lanes, explicit phase boundaries, and the ability to correlate CPU sampling, waiting,
queueing, and application work in one capture.

## Current Branch State

The Dragonfly branch `glevkovich/tracy_profiler` contains these related commits:

| Commit | Content |
|---|---|
| `c78769952` | IoLoopV2 parse-in-proactor work and park statistics. |
| `b284c240f` | Tracy build integration, facade instrumentation, `TRACY.md`, and sample CSV. |
| `06be78170` | Helio submodule update and documentation correction: CSV self time is not CPU time. |

The Helio submodule points at `7cc8da6` on `glevkovich/tracy_profiler`, containing the fiber-aware
Tracy integration. The Dragonfly submodule pointer is required: without it, a clean Dragonfly
checkout will not get the Helio fiber lanes that make the trace trustworthy.

### What Is Already Implemented

- `WITH_TRACY=OFF` is the default. The Dragonfly wrappers compile out and leave arguments
  unevaluated, so the default build has no Tracy instrumentation cost.
- `WITH_TRACY=ON` fetches pinned Tracy `v0.11.1`, enables `TRACY_ENABLE`, `TRACY_FIBERS`, and
  `TRACY_ON_DEMAND`, and links the Tracy client to Dragonfly facade and Helio fibers.
- The on-demand client starts recording only after a viewer or `tracy-capture` connects.
- Helio calls `TracyFiberEnter` on every logical fiber switch. Helio's scheduler is fiber-to-fiber,
  so entering the next fiber implicitly closes the previous one. It intentionally does not emit
  `TracyFiberLeave`: an unmatched Leave after a viewer attaches can crash the Tracy server in
  on-demand mode.
- Dragonfly has structural V1 and V2 zones for parsing, dispatch, batching/squash, replies, reads,
  flushes, idle/backpressure waits, migration, and proactor parsing.
- Wait-only zones use a distinct red color. The V2 parsed-command queue length is exported as a
  Tracy plot.
- Linux call-stack sampling and context-switch/wait-stack setup are documented in [TRACY.md](TRACY.md).
- Headless capture and `tracy-csvexport` provide scriptable zone exports.

### Design Invariants

These are deliberate integration contracts. Changing one requires a design review and a focused
regression capture, not a local simplification.

| Invariant | Reason |
|---|---|
| Use one exact, pinned Tracy version for the client and all server-side tools. | Tracy trace protocol compatibility is version-sensitive. |
| Keep `TRACY_ON_DEMAND` enabled. | A server should not retain its entire lifetime of profiling events before a collector attaches. |
| Define `TRACY_ENABLE` and `TRACY_FIBERS` consistently across Dragonfly and Helio translation units. | The fiber lanes and application zones must agree on one client integration. |
| Emit `TracyFiberEnter` at Helio's two logical fiber-switch funnels. | Zones must be attributed to the logical fiber rather than only its OS thread. |
| Do not add `TracyFiberLeave` to Helio's fiber-to-fiber switch path. | In on-demand mode an unmatched Leave can reach the server after attachment and crash it; Enter of the next fiber closes the prior span in this model. |
| Build capture/viewer/export tools from a standalone Tracy checkout. | A Dragonfly CMake reconfigure can delete `_deps/tracy-src`. |
| Keep hooks compiled out when `WITH_TRACY=OFF`. | The normal Dragonfly binary must pay no Tracy cost. |

The Enter-only rule is specific to Helio's logical fiber-switch path. It is not a blanket ban on
all `TracyFiberLeave` usage in every possible future integration.

### Evidence It Is Already Useful

The initial V1/V2 capture produced a concrete architectural finding. Under the tested workload,
V2's single connection fiber serializes reading, execution, and flushing; V1's receive and async
dispatch fibers overlap those phases. `V2.Flush` was a prominent wall-time cost. This is a design
finding, not merely a list of hot functions, and is exactly the type of question that is difficult
to answer from aggregate thread-level samples alone.

## Important Interpretation Rules

### Zones, wall time, and CPU time are different

An instrumentation zone measures elapsed time in a logical operation. A fiber can be parked inside
that interval. `tracy-csvexport --self` subtracts nested child-zone elapsed time; it does not turn
wall time into on-CPU time.

Use each source for its proper question:

| Evidence | Answers |
|---|---|
| Structural zones | Which named Dragonfly or Helio phase consumed elapsed time? |
| Fiber lanes | When did a logical operation run, yield, resume, or wait? |
| CPU sampling | Which functions, libraries, and kernel paths used CPU? |
| Wait stacks | Where did a thread or fiber block and resume? |
| Queue plots / delay events | Is work building up or waiting to be scheduled? |

For example, a long `V2.Squash` zone may be mostly remote-shard wait, not expensive command CPU.
Sampling and Helio queue/scheduler facts are needed to distinguish the two.

### Production and security boundaries

- `WITH_TRACY=OFF` is the only zero-overhead configuration.
- `TRACY_ON_DEMAND` avoids collecting a trace until a client connects, but a Tracy-enabled binary
  still performs per-event bookkeeping. It is not a zero-cost production switch.
- Treat the Tracy listener and trace contents as sensitive. Do not expose the listener publicly,
  and do not add keys, values, credentials, or other sensitive payloads as dynamic zone text.
- Lowering `perf_event_paranoid` or `kptr_restrict` is appropriate only on a controlled development
  or diagnostic host, never as a broad production default.
- A Tracy diagnostic run can perturb throughput, latency, cache behavior, scheduling, and trace
  size. Obtain benchmark numbers from a non-instrumented measurement run; use Tracy for attribution.

### Recommended Profiling Modes

| Mode | Configuration | Intended result |
|---|---|---|
| `baseline` | `WITH_TRACY=OFF` | Clean throughput and latency measurements. |
| `structure` | Tracy zones and fiber lanes; sampling disabled or kernel defaults retained | Phase, fiber, and explicit-wait structure without sampling overhead. |
| `full-analysis` | Zones, sampling, and optionally wait stacks on a controlled diagnostic host | CPU attribution, kernel/proactor activity, and deep stall explanation. |

Never present a `structure` or `full-analysis` throughput number as a clean benchmark result. Pair
an attribution trace with a separate `baseline` measurement under the same workload.

## Design Direction: Two Correlated Layers

The correct direction is Helio-first infrastructure visibility plus Dragonfly semantic visibility.
Neither layer alone is sufficient.

| Layer | Explains | Examples |
|---|---|---|
| Helio | How work moved or stalled | fiber resume/yield, runnable work, queue delay, condvar/mutex wait, proactor poll/completion |
| Dragonfly | Why that work existed | connection parsing, command execution, cross-shard operation, transaction, replication, snapshot |

The desired trace can show a causal chain such as:

```text
Conn.V2.Squash
  -> Helio.Queue.Delay
  -> Shard.Execute
  -> Helio.Fiber.Resume
  -> Conn.V2.SendReply
```

For a synchronization investigation, it should be possible to see both sides:

```text
Fiber A: Helio.Mutex.Wait      420 us
Fiber B: Helio.Mutex.Held      415 us
```

Dragonfly is largely shared-nothing, so the most common high-value infrastructure signals may be
scheduler delay, queue delay, cross-proactor completion, I/O wakeups, and fiber starvation rather
than mutex contention. That is an argument for the infrastructure layer, not against it.

## Broad, Optional Instrumentation

The design should support broad source coverage. A developer should be able to add a stable hook
once and later choose whether that hook exists in a binary and whether it emits in a capture.

This needs three independent levels of control:

```text
Source coverage:       hooks may be present throughout Helio and Dragonfly
Build-time selection:  only selected scope groups are compiled into a binary
Runtime selection:     selected compiled groups emit for this capture
```

This makes a fully instrumented forensic build possible without imposing its cost or trace volume
on ordinary profiling builds.

### Proposed Scope Groups

Use stable, prefixed names in the trace and a corresponding bitmask in the SDK:

```cpp
enum class TracyScope : uint32_t {
  kConnection = 1U << 0,
  kScheduler  = 1U << 1,
  kSync       = 1U << 2,
  kQueue      = 1U << 3,
  kIo         = 1U << 4,
  kShard      = 1U << 5,
  kTransaction = 1U << 6,
  kMemory     = 1U << 7,
  kReplication = 1U << 8,
  kSnapshot   = 1U << 9,
  kSearch     = 1U << 10,
};
```

Suggested trace prefixes are `Conn.`, `Helio.Scheduler.`, `Helio.Sync.`, `Helio.Queue.`,
`Helio.Io.`, `Shard.`, `Tx.`, `Memory.`, `Replication.`, `Snapshot.`, and `Search.`. Prefixes let
users filter in the GUI and `csvexport` even when many groups are compiled.

### Build-Time and Runtime Selection

The ergonomic CMake interface should accept names, not numeric masks:

```bash
-DWITH_TRACY=ON -DDFLY_TRACY_SCOPES=connection,scheduler,sync,queue
```

CMake converts the list to generated compile definitions such as
`DFLY_TRACY_BUILD_SYNC=1` and `DFLY_TRACY_BUILD_QUEUE=1`. A numeric mask can exist internally, but
the C++ preprocessor cannot directly evaluate an enum expression in `#if`.

Start with `WITH_TRACY` plus `DFLY_TRACY_SCOPES`; do not split the top-level CMake interface into
`WITH_TRACY_CLIENT`, `WITH_TRACY_FIBERS`, and `WITH_TRACY_APP_ZONES` until a tested use case needs
one of those combinations. The existing global client definitions solve a real Dragonfly/Helio
target-boundary issue. If split controls are added later, they must preserve that consistency.

`TRACY_SAMPLING_HZ` is already a runtime environment setting in the current pinned release. Keep it
as the primary sampling-rate control; a CMake cache default is optional only after users need a
repeatable build-level default.

At runtime, a flag can activate a subset already present in the binary:

```text
--tracy-scopes=connection,sync
--tracy-scopes=all
--tracy-min-wait-us=20
--tracy-min-queue-delay-us=50
--tracy-min-fiber-runtime-us=100
```

The intended behavior is:

| State | Result |
|---|---|
| `WITH_TRACY=OFF` | All hooks compile out completely. |
| Tracy on, scope excluded at build time | That scope's hook compiles out completely. |
| Scope compiled but disabled at runtime | No Tracy event is emitted; dynamic formatting and expensive state lookup must also be skipped. |
| Scope compiled and enabled at runtime | The hook emits its zone, plot, message, or event. |

### Profiles

Named profiles keep the flexible system approachable:

| Profile | Typical compiled scopes | Use |
|---|---|---|
| `light` | connection, scheduler, I/O | routine phase and latency investigation |
| `deep` | light plus sync, queue, shard, transaction | focused cross-layer diagnosis |
| `forensic` | all | reproduce a rare stall, starvation event, deadlock, or unexplained backlog |

`forensic` is a deliberate diagnostic configuration, not a benchmark configuration. It should be
available, documented, and easy to select, but its data must not be used as an unqualified
throughput or p99 baseline.

## Instrumentation SDK Principles

Create common Dragonfly and Helio headers rather than letting every caller use raw Tracy macros.
The first version should support scope-aware zones, wait zones, plots, messages, and safe dynamic
annotations. Example call sites:

```cpp
HELIO_TRACY_ZONE(TracyScope::kScheduler, "Helio.Scheduler.Resume");
HELIO_TRACY_WAIT(TracyScope::kSync, "Helio.Mutex.Wait");
HELIO_TRACY_PLOT(TracyScope::kQueue, "Helio.Queue.Depth", depth);

DFLY_TRACY_ZONE(TracyScope::kShard, "Shard.Execute");
DFLY_TRACY_ZONE(TracyScope::kTransaction, "Tx.Commit");
```

SDK rules:

- Use literal static names for normal zones; use dynamic text only for bounded, non-sensitive
  context such as command verb, arity, shard count, queue identity, or key length.
- A disabled runtime scope must not allocate, concatenate strings, format text, acquire a lock, or
  inspect expensive state merely to decide not to emit an event.
- Prefer a plot, sampled counter, or thresholded event for high-frequency operations. Do not emit a
  detailed event for every queue operation by default.
- Use wait zones only for known parked/blocking operations. A function that does work and may yield
  remains a normal work zone; fiber-lane gaps expose preemption.
- Keep a source location and static identity stable so captures can be compared between revisions.
- Retain the existing `DFLY_TRACY_*` compatibility wrappers while introducing scope-aware variants
  incrementally.

### Backend Boundary

Helio should expose a compile-time adapter such as `HELIO_FIBER_SWITCH_HOOK(name)` and map it to
Tracy today. This makes the profiler backend replaceable without imposing a function-pointer or
virtual dispatch on every hot fiber switch. The adapter must inline to nothing when its build scope
is absent.

### Macro Cookbook Requirements

Add a short cookbook to [TRACY.md](TRACY.md) when the scoped API lands. It must include these
patterns, using examples from distinct modules:

```cpp
// A normal work phase. Names are static literals.
DFLY_TRACY_ZONE(TracyScope::kShard, "Shard.Execute");

// A known parked/blocking operation, not a function that merely may yield.
HELIO_TRACY_WAIT(TracyScope::kSync, "Helio.Mutex.Wait");

// Bounded, non-sensitive context, emitted only after the scope is active.
DFLY_TRACY_ZONE_TEXT_SV(command_verb);

// A numeric trend rather than an event for every queue operation.
HELIO_TRACY_PLOT(TracyScope::kQueue, "Helio.Queue.Depth", depth);
```

The cookbook must say explicitly not to attach keys, values, credentials, or unbounded user data;
not to construct dynamic strings before checking scope activation; and not to use zone self time as
CPU time.

## Helio-First MVP

Start with a small, trustworthy infrastructure substrate. The existing fiber-switch integration is
the prerequisite and should remain in place.

### Scheduler Scope

- Resume/yield/preemption transitions where the reason is already cheaply known.
- Runnable or ready-queue depth plots.
- Long fiber-running event, aligned with the existing fiber runtime warning mechanism.
- Optional count of runnable, sleeping, and active fibers per proactor.

### Synchronization Scope

- Fiber-aware mutex and condition-variable wait duration.
- Held-duration zones only where ownership and unlock semantics are unambiguous.
- Stable primitive identity and source location; richer owner context only when inexpensive.
- Thresholds to suppress routine short waits.

Do not introduce `std::mutex`, `std::thread`, or other standard threading primitives for this work.
Helio's fiber-aware synchronization model remains mandatory.

### Queue Scope

- Queue-depth plots at useful boundaries or when thresholds are crossed.
- Enqueue-to-dequeue delay for queues that can safely retain an enqueue timestamp.
- Thresholded delayed-work events rather than per-operation timeline spam.
- Queue and proactor/shard identity when available without costly dynamic work.

### I/O Scope

Sampling normally explains the proactor and kernel well. Add explicit I/O zones only at meaningful
boundaries that sampling cannot explain clearly, such as poll, submission, completion, wakeup, or
cross-proactor handoff.

## Dragonfly Instrumentation Priorities

Do not instrument every function simply because the SDK permits it. Broad optional coverage is the
goal; the first active scopes should answer the most valuable questions.

1. Break down `V2.Squash`: scheduling, remote-shard wait, command execution, and reply collection.
2. Instrument transaction boundaries: shard acquisition, scheduling, execution, commit/unlock.
3. Instrument shard execution boundaries and cross-shard hops.
4. Add selected plots: in-flight work, backpressure state, pipeline memory, queue depth, and active
   fibers per proactor.
5. Add replication, snapshot, memory, and search scopes after actual investigations identify a need.

For data structures and allocators, start with sampling plus meaningful high-level boundaries. Add
fine-grained zones or allocation hooks only when a hypothesis requires them; memory events and
per-operation data-structure events can create a very large trace.

## Tracy Version Upgrade: v0.11.1 to v0.14.0

The current branch is validated with `v0.11.1`. Tracy `v0.14.0` is a worthwhile upgrade candidate,
but it is a separate compatibility change, not a prerequisite for the Helio scope SDK.

Relevant verified v0.14 features include:

- sections with categories and range filtering, useful for benchmark warmup, steady-state, snapshot,
  or reproduction phases;
- heuristic call-stack reconstruction for sampled zones and improved sampling-data consistency;
- improved flame graphs and trace comparison statistics;
- a local MCP server for loading, analyzing, and comparing `.tracy` captures;
- `tracy-capture-daemon` for unattended discovery/capture of clients;
- `tracy-merge` for multi-process traces.

`tracy-merge` currently preserves zones, messages, and plots but loses call stacks, memory events,
and GPU events. It is therefore not suitable for Dragonfly's primary CPU/fiber attribution flow.

### Upgrade Gate

Do not state compatibility as "Tracy >= v0.14". Pin one exact validated version. Before changing
the pin, perform this focused validation:

1. Change only the `FetchContent` tag to `v0.14.0`.
2. Rebuild the client and every standalone capture, viewer, and CSV tool from `v0.14.0`.
3. Reproduce one known V1/V2 workload and capture.
4. Verify on-demand attach stability, fiber-lane attribution, CSV export, sampling, and wait stacks.
5. Confirm the client and tools report compatible protocol/version information.
6. Only then update [TRACY.md](TRACY.md), this plan, and any scripts to name `v0.14.0`.

The v0.14 MCP server and capture daemon are promising follow-up experiments once the basic capture
workflow is repeatable. They do not replace a Dragonfly-native report or a human review of a trace.

## Capture, Analysis, and Automation

### Make the Existing Workflow Usable by Others

Before broad rollout, provide a one-command helper that:

1. Checks that the binary has the requested Tracy scope profile.
2. Explains required kernel permissions without modifying the system implicitly.
3. Starts or targets Dragonfly, performs a bounded headless capture, and stores the `.tracy` file.
4. Produces inclusive and self-elapsed CSV exports.
5. Writes a concise report with workload metadata and links to the raw artifacts.

The success criterion is practical: an engineer who only knows `perf` can produce a useful focused
trace and report in under 30 minutes without assistance.

### Dragonfly-Native Report

Build a small analyzer over `tracy-csvexport` output and benchmark results. It should report facts,
not overclaim CPU attribution:

- phase wall-time and self-elapsed time;
- explicit wait and backpressure durations;
- zone counts, throughput, benchmark p50/p99 latency, and capture duration;
- meaningful before/after deltas under identical workload;
- likely next investigation view, such as sampling inside `V2.Squash` or a queue-delay trace.

The analyzer may identify refactor candidates, but must state whether a signal is wall time, wait
time, or sampled CPU.

### CI Regression Diffs

Automated trace-derived regression analysis is high value, but should begin as a nightly artifact,
not a per-PR blocking gate.

1. Use a dedicated stable host, pinned CPU configuration, workload, capture duration, and versions.
2. Run repeated samples to establish variance.
3. Upload raw benchmark output, CSV reports, and selected `.tracy` files as artifacts.
4. Compare medians/distributions and p50/p99 benchmark latency, not one noisy zone mean or a fixed
   five-percent threshold.
5. Promote stable checks to advisory PR reporting, then consider a blocking gate only after the
   baseline demonstrates low variance and useful signal.

The strongest early value is a shared, reviewable performance report. The full trace remains
available for a human deep dive when a report changes.

## Rollout and Presentation

### Present Now: a Focused Preview

Do not wait to become a universal Tracy expert or to instrument the entire datastore. The current
work is ready for a short technical preview once the branch/submodule state is clean and reproducible.

Show:

- the fiber-aware timeline and V1/V2 connection zones;
- the concrete V2 flush/serialized-pipeline finding;
- the difference between wall-time zones and sampled CPU;
- a short next-step proposal: Helio scheduler/queue/sync scopes plus a capture/report helper.

Frame it as a companion to `perf`, not a claim that the team must abandon its existing workflow.

### Broader Adoption Gate

Ask for wider team adoption after all of the following are true:

- a clean checkout includes the Helio fiber instrumentation;
- another engineer can capture and interpret a trace independently;
- at least two real performance investigations or PRs used the tool to make a decision;
- the helper/report workflow has a documented, repeatable workload;
- overhead and diagnostic limitations are clearly communicated.

The best evidence of value is not the number of macros or scopes. It is performance decisions made
from a shared trace and report rather than guesswork or aggregate samples alone.

## Phased Roadmap

| Phase | Deliverable | Completion evidence |
|---|---|---|
| 0: Make it reproducible | Commit/submodule consistency, current guide, one-command capture/report | Clean checkout produces a known-good V2 trace. |
| 1: Helio substrate | Scope-aware SDK; scheduler, queue, and synchronization MVP; thresholds | One trace explains a connection-to-shard delay through Helio. |
| 2: Data path | `V2.Squash`, shard, and transaction boundaries | An investigation attributes a previously opaque squash or multi-shard delay. |
| 3: Productize | Profiles, workload recipes, report/diff tool, onboarding | Another engineer uses it unaided. |
| 4: Automation | Nightly repeated benchmarks and CSV artifacts | Stable historical signal, with no premature CI gate. |
| 5: Expand by evidence | Replication, snapshot, memory, search, forensic hooks | Each new scope answers a demonstrated investigation need. |

## Non-Goals

- Replacing `perf` or claiming Tracy is the source of truth for every production incident.
- Treating `csvexport --self` as CPU time.
- Enabling permissive kernel profiling settings or an exposed Tracy listener on production hosts.
- Adding unbounded dynamic text, secrets, keys, or values to traces.
- Making all instrumentation active in every build by default.
- Using a forensic trace as an absolute performance benchmark.

## North-Star Metric

Track the number of performance decisions or regressions that the team explains with a reproducible
Dragonfly Tracy phase budget and fiber-aware timeline. If the tool repeatedly answers questions that
would otherwise require conjecture or several disconnected profiling runs, it has paid for itself.
