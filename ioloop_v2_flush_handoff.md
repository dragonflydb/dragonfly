# IoLoopV2 Flush Investigation Handoff

## Scope and Current Decision

Investigate whether V2's synchronous reply flush is a material contributor to the V1/V2
performance gap. Do not implement asynchronous output yet. First add a low-level measurement
gate that separates socket-writability parking from inline send work and scheduler preemption.

The concern is credible. A locally reproduced saturated V2 trace has long, frequent `V2.Flush`
intervals. However, the current `V2.Flush` Tracy scope spans the whole synchronous
`FlushReplies()` call, so it is elapsed wall time, not proof of `EAGAIN` or kernel socket
contention.

## Required Branch Setup

Use the existing independent Tracy branches in both repositories. The branch name is
`glevkovich/tracy_profiler` in both repositories; it is not `glevkovich/tracy_profile`.

## Required Tracy Documents

Before continuing, read both Markdown documents in the Tracy profiling worktree:

- `~/workspaces/dragonfly_worktrees/tracy_profiler/TRACY.md` -- operational guide for building,
  capturing, opening, exporting, sampling, and collecting wait stacks.
- `~/workspaces/dragonfly_worktrees/tracy_profiler/tracy_integration_plan.md` -- architecture and
  integration plan for Dragonfly/Helio Tracy zones, fiber attribution, scope design, and
  interpretation rules.

Also read the original investigation note:

- `/home/gil/notes/tasks/ioloop_v2/overlap_flushing_v2.md`

Dragonfly base:

```text
glevkovich/tracy_profiler
1bb68ee7d6a7728d889a29eaa63330af84c7a6b0
```

Helio base:

```text
glevkovich/tracy_profiler
2e6e1d00f2661a54576169d78249291fb9d3d40c
```

Create the same continuation branch in each repository:

```bash
# Dragonfly repository.
cd ~/workspaces/dragonfly_worktrees/tracy_profiler
git checkout -b glevkovich/ioloop_v2_improve_flush glevkovich/tracy_profiler

# Independent Helio repository used by this Dragonfly worktree.
cd helio
git checkout -b glevkovich/ioloop_v2_improve_flush glevkovich/tracy_profiler
```

The parent Dragonfly worktree will show `M helio` because the Helio branch is intentionally
independent of the submodule SHA recorded in Dragonfly. Do not change the Dragonfly submodule
pointer as incidental cleanup. Do not edit unrelated V2 work.

## Verified Profiling Setup

- Dragonfly profiling client is enabled by `-DWITH_TRACY=ON` and uses on-demand Tracy.
- `TRACY.md` in the Dragonfly profiling worktree documents capture, GUI, sampling, and wait
  stacks.
- Standalone Tracy v0.11.1 tools exist at:
  - `~/workspaces/3rd_party/tracy/capture/build/tracy-capture`
  - `~/workspaces/3rd_party/tracy/csvexport/build/tracy-csvexport`
  - `/usr/local/bin/tracy-profiler`
- The current implementation has V2 zones including `V2.Flush`, `V2.IdleWait`,
  `V2.ExecuteBatch`, `V2.Squash`, `V2.ReplyBatch`, `V2.SendReply`, and parsing zones.
- Helio's fiber attribution is active on the independent Helio Tracy branch.

## Key Reproduced Evidence

Trace and workload artifacts:

```text
~/tmp/tracy_runs/repro_now/df_v2_repro.tracy
~/tmp/tracy_runs/repro_now/zones_v2_repro.csv
~/tmp/tracy_runs/repro_now/memtier_v2_repro.log
```

Reproduced workload:

```text
Dragonfly: V2, 2 proactor threads, loopback
Client:    memtier, 2 threads x 20 connections = 40 connections
Pipeline:  30
Traffic:   1:1 SET/GET, 32-byte values, mostly GET misses
Duration:  15 seconds
Result:    873,400 total operations/sec, average latency 1.368 ms
```

`V2.Flush` aggregate from that capture:

```text
calls:       410,116
total:       132.573 s summed fiber elapsed time
mean:        323.256 us
minimum:     3.364 us
maximum:     7.969 ms
```

Duration distribution:

```text
>= 100 us: 382,212 calls (93.2%), 98.6% of V2.Flush elapsed time
>= 500 us:  57,122 calls (13.9%), 26.5% of V2.Flush elapsed time
>= 1 ms:        644 calls (0.16%), 0.8% of V2.Flush elapsed time
```

Approximately one flush occurs per pipeline batch:

```text
(873,400 ops/s * 15 s) / 410,116 flushes ~= 32 commands/flush
```

This is reproducible from the original capture set in `~/tmp/tracy_runs/`:

```text
df_v2.tracy:   V2.Flush mean 702 us, max 8.06 ms
df_v2ms.tracy: V2.Flush mean 314 us, max 6.34 ms
df_v1.tracy:   V1 baseline
```

The separate single-connection, pipeline-100 GET probe did not show this behavior:

```text
V2.Flush count 14, mean 264 ns, max 1.905 us
```

This difference is expected: 40 active connection fibers create far more scheduler, proactor, and
kernel/network activity than one loopback client. It does not, by itself, prove kernel contention.

## Exact Local Reproduction Commands

These commands produced `~/tmp/tracy_runs/repro_now/df_v2_repro.tracy` and the measurements above.
They require the existing Tracy-enabled binary in the `tracy_profiler` worktree. Use three terminals.
The fixed output path is intentional: shell variables from one terminal are not available in another.

Terminal 1: start the V2 server and leave it running.

```bash
cd ~/workspaces/dragonfly_worktrees/tracy_profiler

./build-opt/dragonfly \
  --proactor_threads=2 \
  --enable_resp_io_loop_v2=true \
  --port=6379 \
  --admin_port=6380 \
  --dbfilename="" \
  --alsologtostderr
```

Terminal 2: first clear the database, then wait for the capture in Terminal 3 to report that it is
connected, and immediately run the 15-second saturated workload.

```bash
redis-cli -p 6379 FLUSHALL
```

```bash
memtier_benchmark \
  -s 127.0.0.1 \
  -p 6379 \
  -t 2 \
  -c 20 \
  --pipeline=30 \
  --ratio=1:1 \
  -d 32 \
  --key-pattern=R:R \
  --key-minimum=1 \
  --key-maximum=10000000 \
  --test-time=15 \
  --hide-histogram \
  | tee "$HOME/tmp/tracy_runs/repro_now/memtier_v2_repro.log"
```

Terminal 3: create the fixed output directory, then begin the 15-second capture. Start Terminal 2's
memtier command immediately after this command prints `Connecting to 127.0.0.1:8086...`.

```bash
mkdir -p "$HOME/tmp/tracy_runs/repro_now"
```

```bash
"$HOME/workspaces/3rd_party/tracy/capture/build/tracy-capture" \
  -o "$HOME/tmp/tracy_runs/repro_now/df_v2_repro.tracy" \
  -a 127.0.0.1 \
  -p 8086 \
  -f \
  -s 15
```

After capture and memtier both exit, run these in any terminal:

```bash
"$HOME/workspaces/3rd_party/tracy/csvexport/build/tracy-csvexport" \
  -f V2. \
  "$HOME/tmp/tracy_runs/repro_now/df_v2_repro.tracy" \
  | tee "$HOME/tmp/tracy_runs/repro_now/zones_v2_repro.csv"
```

```bash
"$HOME/workspaces/3rd_party/tracy/csvexport/build/tracy-csvexport" \
  -f V2.Flush \
  "$HOME/tmp/tracy_runs/repro_now/df_v2_repro.tracy"
```

```bash
/usr/local/bin/tracy-profiler \
  "$HOME/tmp/tracy_runs/repro_now/df_v2_repro.tracy"
```

## Deep Laptop Capture: CPU Sampling and Wait Stacks

Run this only on the developer laptop. It temporarily relaxes system-wide perf permissions and
increases sampling overhead. Do not apply it to a shared or production host. This run is for
attribution, not a clean throughput number.

The objective is to inspect representative long `V2.Flush` intervals and distinguish:

- CPU in `sendmsg`, socket-copy paths, or application code;
- a parked proactor/fiber waiting in `epoll_wait` or `io_uring_enter`;
- generic scheduler preemption.

### 1. Enable Deep Sampling Before Starting the Capture

In a terminal, run the following commands manually. They require the local sudo password and take
effect until reboot unless separately persisted. `-1` is required for context-switch wait stacks;
`1` is enough for CPU samples but does not provide the full wait-stack data.

```bash
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
echo 100000 | sudo tee /proc/sys/kernel/perf_event_max_sample_rate
```

Confirm the values:

```bash
cat /proc/sys/kernel/perf_event_paranoid
cat /proc/sys/kernel/kptr_restrict
cat /proc/sys/kernel/perf_event_max_sample_rate
```

Expected output is respectively `-1`, `0`, and `100000`.

### 2. Run the Same Workload With a Root Collector

Use the same Terminal 1 server command and Terminal 2 `FLUSHALL` plus memtier command from the
previous section. In a new Terminal 3, create a separate output directory:

```bash
mkdir -p "$HOME/tmp/tracy_runs/repro_deep"
```

Start the collector as root, then start memtier immediately after it prints
`Connecting to 127.0.0.1:8086...`:

```bash
sudo "$HOME/workspaces/3rd_party/tracy/capture/build/tracy-capture" \
  -o "$HOME/tmp/tracy_runs/repro_deep/df_v2_repro_deep.tracy" \
  -a 127.0.0.1 \
  -p 8086 \
  -f \
  -s 15
```

The output file will be root-owned because the collector is root. After it completes, return
ownership so the normal user can export and open it:

```bash
sudo chown "$USER":"$(id -gn)" \
  "$HOME/tmp/tracy_runs/repro_deep/df_v2_repro_deep.tracy"
```

Then export zones and open the trace:

```bash
"$HOME/workspaces/3rd_party/tracy/csvexport/build/tracy-csvexport" \
  -f V2. \
  "$HOME/tmp/tracy_runs/repro_deep/df_v2_repro_deep.tracy" \
  | tee "$HOME/tmp/tracy_runs/repro_deep/zones_v2_repro_deep.csv"
```

```bash
/usr/local/bin/tracy-profiler \
  "$HOME/tmp/tracy_runs/repro_deep/df_v2_repro_deep.tracy"
```

### 3. Verify Sampling Before Interpreting It

In Tracy, open **Info**. Do not interpret the trace as a deep capture unless both values are
nonzero:

```text
Call stack samples:       > 0
Context switch samples:   > 0
```

Then inspect a long `V2.Flush` instance:

1. Open **Statistics**, filter `V2.Flush`, and double-click a millisecond-scale instance.
2. In **Sampling** statistics, inspect stacks on the corresponding proactor thread for `sendmsg`,
   socket-copy functions, `epoll_wait`, `io_uring_enter`, and application functions.
3. Right-click the relevant fiber/proactor row and use **Wait stacks**. A wait stack rooted in an
   output-writability wait supports the async-output hypothesis; a stack showing generic scheduler
   preemption or CPU work does not.

Sampling is still statistical. Keep the explicit partial-write, `EAGAIN`, byte, and wait-duration
instrumentation described below as the authoritative quantitative measurement.

## What `V2.Flush` Currently Means

The V2 scope surrounds `Connection::FlushReplies()` in
`src/facade/dragonfly_connection.cc`. It eventually reaches
`SinkReplyBuilder::Send()` in `src/facade/reply_builder.cc`:

```cpp
if (auto ec = sink_->Write(vecs_.data(), vecs_.size()); ec)
  ec_ = ec;
```

`io::Sink::Write()` calls `WriteSome()` repeatedly until the whole iovec batch has been written or
an error occurs. The scope therefore establishes an important scheduling fact: while it is open,
the V2 connection fiber cannot make progress on the next input, parse, execution, reply build,
control message, or migration action for that connection.

It does not distinguish any of these causes:

1. Inline `sendmsg` / iovec / copying / syscall CPU.
2. A partial send followed by additional immediate write attempts.
3. Socket `EAGAIN`, then a fiber park awaiting writability.
4. Fiber preemption or another scheduler delay while synchronous write remains on the call stack.

Do not call the current result "kernel contention" without further evidence. It is a plausible
hypothesis, especially with 40 connections, but not a demonstrated cause.

## What Tracy Sampling Can and Cannot Answer

Yes: with the Linux settings documented in `TRACY.md`, Tracy can show the proactor and kernel-side
CPU stacks. Use it to determine whether sampled CPU in/near the flush interval belongs to
`sendmsg`, socket send/copy paths, `epoll_wait`, `io_uring_enter`, or application code.

For CPU sampling and context-switch regions, capture after setting on a controlled development host:

```bash
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
echo 100000 | sudo tee /proc/sys/kernel/perf_event_max_sample_rate
```

For wait stacks, use the more permissive setting and usually run the collector as root:

```bash
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
sudo ~/workspaces/3rd_party/tracy/capture/build/tracy-capture \
  -o /tmp/df_waitstacks.tracy -a 127.0.0.1 -p 8086 -f -s 15
```

Verify in Tracy's Info view that `Call stack samples` and, for the second capture,
`Context switch samples` are nonzero. Sampling is statistical and can explain CPU and parked call
stacks, but it cannot provide exact counts for `EAGAIN`, partial sends, WriteSome calls, or bytes
accepted before the first block. Add explicit counters for those facts.

## Next Implementation: Measurement Gate Only

Make the smallest instrumented change first. Do not add `AsyncWrite()` output overlap until the
following data says it can help.

Required facts per synchronous reply batch:

1. Total batch bytes and iovec count.
2. Number of `WriteSome()` attempts.
3. Number of partial writes.
4. `EAGAIN` / would-block count.
5. Bytes accepted before the first yield or `EAGAIN`.
6. Wall/cycle duration from first would-block until the final completion.
7. Flush count and total flush duration, preserving existing reply send metrics.

Use a normal Tracy work zone for the whole flush, and add a distinct red wait zone only around the
actual socket-writability await. Do not label the whole `V2.Flush` scope as a pure wait zone because
it can contain real work and preemption.

Instrumentation ownership:

- Dragonfly reply layer should record batch size, reply/connection attribution, and high-level
  observability near `SinkReplyBuilder::Send()`.
- Helio socket implementations should expose or record partial sends, `EAGAIN`, and the exact
  wait-after-`EAGAIN` interval. The generic `io::Sink::Write()` wrapper alone cannot know why a
  concrete socket did not write.
- Preserve Dragonfly's shared-nothing/fiber model. Do not introduce `std::mutex`, `std::thread`,
  or global mutable aggregation state.

Run the measurement in both epoll and io_uring modes, and under the exact reproduced 40-connection
pipeline-30 scenario before drawing conclusions.

## Decision Gate for Async Output

Proceed to a V2-only ordered asynchronous writer only when the measured data shows a meaningful
fraction of flush wall time is wait after partial output or `EAGAIN`:

```text
High EAGAIN / substantial parked-after-EAGAIN time:
  Async output can overlap a prior reply drain with later connection work.

Low EAGAIN / mostly inline send CPU or scheduler preemption:
  AsyncWrite alone will not remove the cost.
  Focus on batching, syscall/iovec reduction, reply construction, or V2 scheduling instead.
```

If async output is justified, follow the design constraints in the task note
`/home/gil/notes/tasks/ioloop_v2/overlap_flushing_v2.md`:

- One active socket write per connection. Concurrent pending writes can crash epoll and violate
  RESP ordering.
- Use `io::AsyncSink::AsyncWrite()`, never facade-level `AsyncWriteSome()`.
- Keep submitted payload bytes alive through completion.
- Start with owned/batched replies; retain the synchronous policy for large borrowed references
  unless a safe explicit ownership model exists.
- Bound active plus in-flight output memory and preserve send observability.
- Prevent migration and teardown from racing a pending completion.
- Route all V2 producers through one ordered writer.

## Benchmark Context

Issue #279 (`dragonflydb/dataplane-private#279`) demonstrates V1/V2 gaps on separate AWS client
and server instances with four proactors and multiple pipeline depths. The local 2-proactor trace
does not prove flush serialization is the sole cause of every issue cell. It does make output flush
a high-priority candidate to measure in a matching remote benchmark cell once the low-level facts
exist.

## Validation Order

1. Build the profiling branches only with explicit user approval.
2. Repeat the 40-connection/pipeline-30 trace with new low-level observability.
3. Inspect Tracy sampling and wait stacks for a representative long flush.
4. Repeat under epoll and io_uring.
5. Reproduce one remote Issue #279 V1/V2-regression cell with the same counters.
6. Choose async ordered output only if parked socket-write time is material.

No async flush implementation has been started in this handoff.
