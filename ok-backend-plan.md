## ok-backend-benchmarking Plan

### Stage 0.1 — Hardware & SSH sanity check

| Check | How |
|-------|-----|
| Both AWS machines reachable | `ssh client_ip true; ssh server_ip true` |
| Memtier installed on client | `ssh client which memtier_benchmark` |
| Valkey installed on server | `which valkey-server` (install if missing) |
| Performance governor | `cpupower frequency-set -g performance` |
| Turbo boost off (optional, for cleaner numbers) | echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo |

### Stage 0.2 — Modify bench_v2.sh

Three additions, all in one PR-sized change:

**(a) Valkey support**
```bash
VALKEY_MODE=${VALKEY_MODE:-0}  # set to 1 to launch valkey-server instead
VALKEY_BIN=${VALKEY_BIN:-valkey-server}
VALKEY_IO_THREADS=${VALKEY_IO_THREADS:-1}
```

In `start_dragonfly()`, branch on `$VALKEY_MODE`:
- If 1, exec `$VALKEY_BIN --port $PORT --io-threads $VALKEY_IO_THREADS --protected-mode no --save "" --appendonly no --daemonize no`
- Skip `verify_metrics_endpoint`, set `get_send_count() { echo 0; }` (or compute density from memtier output only)
- Skip `--admin_port`, `--proactor_threads`, V1/V2 flags

**(b) Tail latency parsing**
Remove `--hide-histogram` from default memtier args; add columns to RESULTS_TMP:
```
PIPELINE\tRPS\tAVG_LAT(ms)\tP50(ms)\tP95(ms)\tP99(ms)\tSEND_SYSCALLS\tBATCH_DENSITY
```

**(c) Multishot flag for V2 runs**
Already supported via `EXTRA_DFLY_FLAGS="--uring_recv_buffer_cnt=128"`. Just document it.

### Stage 0.3 — Instrumentation prep (Dragonfly + ok_backend)

Your commit 699ab8d gives fiber-level counters. Cheap additions before re-running:

**Histograms** (~20 lines in dragonfly_connection.h):
```cpp
// Per-phase distribution, not just averages
uint64_t wake_latency_hist[6] = {};   // [<1µs, 1-2, 2-5, 5-10, 10-50, >50]
uint64_t cmds_between_idle_hist[6] = {};  // bucket on idle-await
uint64_t flush_qdepth_hist[6] = {};   // at real flush sites
```

Plus the bucketing helper:
```cpp
inline unsigned BucketIdx(uint64_t val, std::initializer_list<uint64_t> thresholds) {
  unsigned i = 0;
  for (auto t : thresholds) { if (val < t) return i; ++i; }
  return i;
}
```

**Per-command TSC stamps (defer to Stage 3 if Stage 2 is insufficient).** Don't add upfront — wait until you know they're needed.

**VLOG aggregation in `OnClose`**: extend the existing per-connection `[v2stats conn=N]` log line with the new histograms so they show up in `$DFLY_LOG`.

### Stage 0.4 — Confirm ok_backend works in the harness

```bash
# Local smoke test
./build-opt/ok_backend --proactor_threads=1 --enable_resp_io_loop_v2=true \
  --bind=0.0.0.0 --port=6379 --admin_port=8099 --dbfilename ""
# In another shell: redis-cli -p 6379 PING ; curl http://localhost:8099/metrics
```

If both succeed, `bench_v2.sh ./build-opt/ok_backend ...` will work.

---

### Stage 1 — Baseline Matrix on AWS

> **Results:** See [ok-backend-stage1-results.md](ok-backend-stage1-results.md) for validated data, insights, and corrections from peer review.

**Goal:** Decompose the V2 vs V1 regression into three named gaps before writing any optimization code.

| Gap to measure | How measured | Question answered |
|----------------|--------------|-------------------|
| Facade overhead | Valkey vs ok-backend V1 @1t | How much does Dragonfly's fiber/IO loop architecture cost vs a monolithic loop? |
| V2 loop regression — single connection | ok-backend V1 vs V2 @1t, single_conn | Does IoLoopV2 add overhead even with no cross-shard hops? |
| V2 loop regression — multi connection | ok-backend V1 vs V2 @4t, multi_conn | How much regression comes from multi-shard scheduling overhead in V2? |
| Engine cost | ok-backend V2 vs Dragonfly V2 @4t | How much do DashTable, transactions, and barriers add on top of the loop? |
| Multishot impact | ok-backend V2 multishot on vs off @4t | Does multishot recv help or hurt, and is the effect loop-only or engine-coupled? |

**Why `single_conn` and `multi_conn` are separate (not "both modes"):**
- `single_conn` (1t × 1c × pipeline=P): one connection, no cross-shard hops. Tests parser efficiency, reply batching, idle-flush. V2 is close to V1 here at 1 thread — shows the loop itself is not catastrophically slow.
- `multi_conn` (2t × 25c = 50 connections × pipeline=P): keys spread across all shards, full cross-thread FiberQueue traffic. This is where the 25–41% V2 regression lives. Shows the multi-shard scheduling overhead that squashing (Task 13) targets.
- **Pubsub is excluded** from Stage 1 — it's a different workload axis and the V1/V2 pubsub regressions are already fixed. Add it only if you have a specific pubsub question.

**Runs** (all on AWS cross-machine, `SERVER_HOST=<server_ip> CLIENT_HOST=<client_ip>` prepended):

| # | Server | Threads | V1/V2 | Multishot | Mode | Command |
|---|--------|---------|-------|-----------|------|---------|
| 1 | Valkey | 1 (io-threads=1) | n/a | n/a | single_conn | `SERVER_TYPE=valkey ./bench_v2.sh ../valkey/src/valkey-server single_conn 3 valkey_1t` |
| 2 | Valkey | 1 (io-threads=1) | n/a | n/a | multi_conn | `SERVER_TYPE=valkey ./bench_v2.sh ../valkey/src/valkey-server multi_conn 3 valkey_1t` |
| 3 | ok_backend | 1 | V1 + V2 | off | single_conn | `./bench_v2.sh ./build-opt/ok_backend single_conn 3 ok_v1v2_1t both 1` |
| 4 | ok_backend | 4 | V1 + V2 | off | single_conn | `./bench_v2.sh ./build-opt/ok_backend single_conn 3 ok_v1v2_4t both 4` |
| 5 | ok_backend | 4 | V1 + V2 | off | multi_conn | `./bench_v2.sh ./build-opt/ok_backend multi_conn 3 ok_v1v2_4t both 4` |
| 6 | ok_backend | 4 | V2 only | on (128) | multi_conn | `EXTRA_SERVER_FLAGS="--uring_recv_buffer_cnt=128" ./bench_v2.sh ./build-opt/ok_backend multi_conn 3 ok_v2_ms_4t v2 4` |
| 7 | Dragonfly | 4 | V1 + V2 | off | single_conn | `./bench_v2.sh ./build-opt/dragonfly single_conn 3 dfly_4t both 4` |
| 8 | Dragonfly | 4 | V1 + V2 | off | multi_conn | `./bench_v2.sh ./build-opt/dragonfly multi_conn 3 dfly_4t both 4` |
| 9 | Dragonfly | 4 | V2 only | on (128) | multi_conn | `EXTRA_SERVER_FLAGS="--uring_recv_buffer_cnt=128" ./bench_v2.sh ./build-opt/dragonfly multi_conn 3 dfly_v2_ms_4t v2 4` |

Notes on the current script state:
- `BENCH_DURATION` defaults to **15 seconds** per pipeline depth — total per run is ~4×15s = 60s; 3 runs = ~3 min per invocation; 9 invocations = ~27 min total.
- `EXTRA_SERVER_FLAGS` (not `EXTRA_DFLY_FLAGS`) — renamed for all server types.
- `SERVER_TYPE=valkey` — uses the binary path as the first arg; `VALKEY_IO_THREADS=1` is the default.
- Valkey binary path `../valkey/src/valkey-server` matches your AWS setup — adjust if installed elsewhere.
- All commands should be run from the Dragonfly repo root on the server machine.

**What to read from the results:**

| Comparison | Expected finding | If unexpected |
|------------|-----------------|---------------|
| Valkey vs ok-V1 @1t, p=100 | ok-backend within 20–30% of Valkey | Gap > 30%: facade architecture itself is the bottleneck |
| ok-V1 vs ok-V2 @1t, p=100 | V2 within 5% of V1 (no cross-shard hops) | V2 much slower: the loop itself has per-command overhead beyond scheduling |
| ok-V1 vs ok-V2 @4t, p=100 | V2 ~25% slower than V1 (matches prior data) | Larger gap: scheduling overhead grew; smaller: prior runs were noisy |
| ok-V2 vs Dragonfly V2 @4t | Dragonfly ~15% slower than ok-backend | Gap > 20%: engine is a bigger factor than Roman's phase data suggests |
| ok-V2 multishot on vs off @4t | ≤5% difference (Task 17 baseline) | Big win: promote Task 9; big loss: multishot is buggy at this concurrency |


### Stage 2 — Analyze with the existing instrumentation

For every Stage 1 run, parse the saved Dragonfly log for `v2stats` lines. Compute per-config:

- **Average wake latency**: `wakeup_latency_cycles / wakeup_latency_count`
- **Wake latency distribution**: from the new histogram
- **Cmds between idle awaits distribution**: from the new histogram
- **Real flush rate**: `flush_syscalls / flush_calls` (how many calls are no-ops)
- **Idle-flush attribution**: `flush_reason_idle / flush_reason_*` split
- **Fragment starvation evidence**: `idle_flush_with_pending_input`, `idle_flush_kernel_had_data` (already in your commit)
- **Cross-shard barrier rate**: `executebatch_fiber_yields`

Build a derived table per config: what fraction of time goes to each phase, what fraction of flushes are spurious, where does V2 differ from V1 most.

**Decision point:** If Stage 2 gives a clear winner explanation (e.g., "V2 wake latency is 5x higher" or "V2 has 2x more spurious flushes"), go to Stage 4 or Stage 5. If it's ambiguous, do Stage 3.

### Stage 3 — Per-command TSC tracing (conditional)

Only if Stage 2 doesn't explain the regression. Add 4 stamps to `CommandContext`:
- `ts_dispatch_entry` (top of `Service::DispatchCommand`)
- `ts_after_find_extended` (after `registry_.FindExtended` — Roman's 5.9% phase)
- `ts_before_invoke` (right before `InvokeCmd`)
- `ts_after_invoke` (right after)

Sum + count per phase per thread, expose as new `/metrics` lines. Rerun a subset of Stage 1 cells.

> **Implementation reminder (conditional — do NOT add upfront):**
> Only implement if Stage 2 data is ambiguous (does not clearly identify which phase dominates).
> If Stage 2 shows a clear signal (e.g. `wake_latency_hist` heavily skewed to >10µs, or
> `cmds_between_idle_hist` bimodal), skip Stage 3 and go directly to Stage 4 or Stage 5.
>
> When implementing:
> - Add `uint64_t ts_dispatch_entry, ts_after_find, ts_before_invoke, ts_after_invoke` to
>   `CommandContext` (in `src/server/command_registry.h` or equivalent)
> - Use `base::CycleClock::Now()` (already used in this codebase — NOT raw `__rdtsc()`)
> - Aggregate sums + counts per thread (not per-command log — too noisy under load)
> - Expose as new `/metrics` lines: `phase_us_find_avg`, `phase_us_invoke_avg`, etc.
> - The same stamps apply to ok_backend's `CmdContext` for the dispatch-only measurement

### Stage 4 — SPIN sweep (MID priority, restored from earlier demotion)

Add `SPIN <µs>` command to ok_backend (~20 lines):
```cpp
if (absl::EqualsIgnoreCase(cmd_name, "SPIN")) {
    uint64_t us = 0;
    if (cmd->size() >= 2) absl::SimpleAtoi(cmd->at(1), &us);
    return SpinAsync(cmd_ctx, pool_, us);
}
```

The shard lambda busy-waits via `CycleClock::Now()` for `us` microseconds, then `blocker.Dec()`.

Run bench_v2.sh with a custom memtier command via `MEMTIER_ARGS`:
```bash
MEMTIER_ARGS='--command=SPIN 0 -d 2 -n 50000 -t 2 -c 25 --hide-histogram' \
  ./bench_v2.sh ./build-opt/ok_backend multi_conn 3 ok_spin0 both 4
# Repeat for SPIN 500, 1000, 2000, 5000
```

**What it tells you:** the crossover point where V2 dispatch overhead stops mattering. Roman didn't measure this.

### Stage 5 — Squashing prototype (Option B — modify `ExecuteBatch()`)

**This is the V2 squashing experiment you actually want.** Modify dragonfly_connection.cc `ExecuteBatch()` to bucket queued `ParsedCommand`s by target shard before the per-command dispatch loop. Per shard, issue ONE `DispatchBrief` carrying the batch.

Why ok_backend is the testbed:
- The engine work is `flat_hash_map::insert_or_assign` (~50 ns) — squashing's impact on dispatch overhead is unmasked.
- No transactions, no DashTable, no `CapturingReplyBuilder` complexity.
- The same dragonfly_connection.cc code runs in real Dragonfly, so a successful prototype directly informs Task 13.

Workflow:
1. Branch from main, modify `ExecuteBatch()` with the squashing logic.
2. Rebuild ok_backend.
3. Re-run Stage 1 cells for ok_backend V2 only.
4. Compare RPS / batch density / wake latency before vs after.
5. If positive, run the same modified binary as real Dragonfly to confirm the change transfers.

If V2 squashing on ok_backend doesn't beat V2 baseline, **the projection in Task 13 is wrong** and you've saved multi-week wasted effort.

Estimated effort: 1–2 days (most of the time is iterating on edge cases like single-shard pipelines).

---

### Summary of Priority Changes from Earlier Plan

| Item | Earlier | Now | Why |
|------|---------|-----|-----|
| Use bench_v2.sh | implicit | **explicit Stage 0.2** | You called this out |
| Valkey support in script | absent | **Stage 0.2(a)** | You called this out |
| p99 parsing | absent | **Stage 0.2(b)** | You called this out |
| Histogram additions | absent | **Stage 0.3** | You called this out |
| Rerun all baselines | optional | **mandatory** | Methodology risk |
| Stage 5 squashing target | V1 path (option A) | **V2 path via `ExecuteBatch()`** (option B) | You correctly pushed back |
| Per-command TSC | upfront | **conditional on Stage 2** | Use existing instrumentation first |
| Drop noop-RPS-to-µs arithmetic | kept | **dropped from any new analysis** | Roman is right, methodology is unsound |
| SPIN priority | LOW | **MID** | Sweeps a dimension Roman didn't |
