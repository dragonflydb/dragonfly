# Stage 1 Baseline Results — IoLoop V2 Analysis

## Test Environment

- **Server:** AWS Graviton ARM `172.31.30.209` (Neoverse-N1, 4 CPUs)
- **Client:** AWS Graviton ARM `172.31.20.3` (Neoverse-V1, 4 CPUs)
- **Valkey:** 9.1.0
- **Dragonfly / ok_backend:** branch `glevkovich/ioloopv2_main`, forked from `main` at `4464d117881fd0c9ce94e9f4163c7247dcb9a45c`
- **Per-row commits:** rows 1–7 = `a3312b78`, row 8 = `8883425c`, row 9 = `1a35aacd`
- **Benchmark params:** `BENCH_DURATION=15s`, `NUM_RUNS=3`, `-d 2048` (2 KB SETs), `--ratio=1:0`
- **SConn** = memtier `-t 1 -c 1` (1 connection)
- **MConn** = memtier `-t 2 -c 25` (50 connections)

### Shorthand

| Short | Meaning |
|-------|---------|
| **VK** | Valkey 9.1.0 |
| **OKB** | ok_backend (minimal Redis-compat, helio + flat hash map, no engine) |
| **DF** | Dragonfly (DashTable, transactions, shared-nothing) |
| **V1** | Original `RespIoLoop` (blocking fiber dispatch) |
| **V2** | New `RespIoLoop` (event-driven, async dispatch) |
| **MS** | Multishot recv (`--uring_recv_buffer_cnt=16000`) |

---

## Workflow Definitions

| # | Name | Binary | Loop | Threads | Conns | Purpose |
|--:|------|--------|------|--------:|------:|---------|
| 1 | VK-1t-SConn | VK | — | 1 | 1 | Industry baseline |
| 2 | VK-1t-MConn | VK | — | 1 | 50 | VK saturated |
| 3 | OKB-1t-SConn (V1+V2) | OKB | V1, V2 | 1 | 1 | Pure loop overhead, no fan-out |
| 4 | OKB-4t-SConn (V1+V2) | OKB | V1, V2 | 4 | 1 | Cross-shard on 1 socket |
| 5 | OKB-4t-MConn (V1+V2) | OKB | V1, V2 | 4 | 50 | Production-shape, thin engine |
| 6 | OKB-4t-MConn+MS | OKB | V2+MS | 4 | 50 | Multishot on thin engine |
| 7 | DF-4t-SConn (V1+V2) | DF | V1, V2 | 4 | 1 | Engine cost at single conn |
| 8 | DF-4t-MConn (V1+V2) | DF | V1, V2 | 4 | 50 | **THE V2 regression row** |
| 9 | DF-4t-MConn+MS | DF | V2+MS | 4 | 50 | Multishot on DF |

Rows 6, 9: multishot deadlocks at p≥100 → only p=1, 10, 50 collected.

---

## Raw Averages (kops/s, 3 runs each)

| # | Workflow | p=1 | p=10 | p=50 | p=100 |
|--:|---------|----:|-----:|-----:|------:|
| 1 | VK-1t-SConn | 13.8 | 85.3 | — | 247.1 |
| 2 | VK-1t-MConn | 120.1 | 280.6 | — | 338.9 |
| 3 | OKB-1t-SConn **V1** | 14.6 | 89.7 | — | 281.5 |
| 3 | OKB-1t-SConn **V2** | 13.3 | 69.6 | — | 168.6 |
| 4 | OKB-4t-SConn **V1** | 12.3 | 42.3 | — | 68.0 |
| 4 | OKB-4t-SConn **V2** | 11.4 | 60.9 | — | 157.6 |
| 5 | OKB-4t-MConn **V1** | 206.6 | 485.1 | — | 557.9 |
| 5 | OKB-4t-MConn **V2** | 184.3 | 498.1 | — | 593.6 |
| 6 | OKB-4t-MConn **V2+MS** | 143.8 | 402.2 | 514.4 | — |
| 7 | DF-4t-SConn **V1** | 11.2 | 46.7 | — | 138.0 |
| 7 | DF-4t-SConn **V2** | 10.3 | 56.0 | — | 162.3 |
| 8 | DF-4t-MConn **V1** | 179.6 | 429.2 | 544.3 | 563.6 |
| 8 | DF-4t-MConn **V2** | 151.2 | 405.2 | 463.1 | 445.2 |
| 9 | DF-4t-MConn **V2+MS** | 172.2 | 424.4 | 491.6 | — |

## Tail Latency (ms, at p=100 unless noted)

| # | Workflow | avg | p99 | p99.9 |
|--:|---------|----:|----:|------:|
| 1 | VK-1t-SConn | 0.40 | 0.66 | 0.79 |
| 2 | VK-1t-MConn | 14.7 | 16.28 | 16.62 |
| 3 | OKB-1t-SConn V1 | 0.35 | 0.48 | 0.55 |
| 3 | OKB-1t-SConn V2 | 0.57 | 0.67 | 0.79 |
| 5 | OKB-4t-MConn V1 | 8.94 | 11.43 | 16.36 |
| 5 | OKB-4t-MConn V2 | 8.41 | 21.33 | **95.49** |
| 8 | DF-4t-MConn V1 | 8.78 | 13.00 | 16.00 |
| 8 | DF-4t-MConn V2 | 11.20 | 32.00 | **42.00** |

---

## Insights

### A. V2 loop regression on OKB (Row 3, 1 thread / 1 connection)

Cleanest signal — no fan-out, no engine, just the loop.

| p | OKB V1 | OKB V2 | V2/V1 |
|--:|-------:|-------:|------:|
| 1 | 14.6 | 13.3 | 0.91 (−9%) |
| 10 | 89.7 | 69.6 | 0.78 (−22%) |
| 100 | 281.5 | 168.6 | 0.60 (**−40%**) |

**Scope:** This regression is most clearly demonstrated at 1 thread / 1 connection. At 4 threads / 50 connections (Row 5) it is masked by connection-level parallelism where V2's async dispatch compensates.

### B. V2 wins when V1's per-conn dispatch serialises (Row 4, 4 threads / 1 connection)

Strictly the 4-thread × 1-connection shape. V1 serialises pipelined commands through one dispatch fiber; V2 fires them concurrently via `ONLY_ASYNC`.

| p | OKB V1 | OKB V2 | V2/V1 |
|--:|-------:|-------:|------:|
| 10 | 42.3 | 60.9 | 1.44 (+44%) |
| 100 | 68.0 | 157.6 | 2.32 (**+132%**) |

### C. V2 ≥ V1 on OKB MConn (Row 5, 4 threads / 50 connections)

| p | OKB V1 | OKB V2 | V2/V1 |
|--:|-------:|-------:|------:|
| 1 | 206.6 | 184.3 | 0.89 (−11%) |
| 10 | 485.1 | 498.1 | 1.03 (+3%) |
| 100 | 557.9 | 593.6 | 1.06 (+6%) |

**Important context (verified in code):** OKB V1's `DispatchManyCommands` does **NOT** implement squashing. The comment in `ok_main.cc:260` explicitly states: *"Currently does not implement squashing and is very naive."* Each command does an individual `AwaitBrief` cross-thread hop — meaning with 4 threads, ~75% of commands pay a full fiber yield + cross-thread round-trip, one at a time.

Despite this, OKB V1 still achieves 557.9k at p=100 because: (a) the engine work is ~50 ns (a flat_hash_map insert), and (b) with 50 parallel connections, the hop latency is amortised across fibers — while one fiber waits for its hop to return, the other 49 connections keep the thread busy.

**What this means for the comparison:** V2 "winning" by +6% at p=100 is NOT because V2 is efficient — it's because V1's baseline is artificially depressed by the lack of squashing. If OKB V1 implemented proper cross-shard batching (like DF V1 does via `MultiCommandSquasher`), V1 would likely score significantly higher, and the V2 regression would become visible even at 50 connections.

**Bottom line:** This row does not prove "V2 ≥ V1". It proves that with 50 parallel connections and a near-zero-cost engine, connection-level parallelism masks both V1's dispatch inefficiency and V2's loop overhead.

### D. V2 regresses on DF MConn (Row 8, 4 threads / 50 connections) — THE headline

| p | DF V1 | DF V2 | V2/V1 |
|--:|------:|------:|------:|
| 1 | 179.6 | 151.2 | 0.84 (**−16%**) |
| 10 | 429.2 | 405.2 | 0.94 (−6%) |
| 50 | 544.3 | 463.1 | 0.85 (**−15%**) |
| 100 | 563.6 | 445.2 | 0.79 (**−21%**) |

Tail latency: V2 p99.9 = 42 ms vs V1 = 16 ms at p=100.

### E. V2 has a severe tail-latency problem (Row 5 + Row 8)

**This is the strongest actionable signal in the dataset.**

OKB V2 at p=100 with 50 connections shows p99.9 = **95 ms** while the engine work (flat_hash_map insert) takes ~50 ns. This means some connections are stalled for almost 100 ms — a ~2,000,000× amplification of actual work time.

DF V2 shows the same pattern: p99.9 = 42 ms vs V1's 16 ms.

**Possible causes (not yet proven — profiling required):**

1. **Backpressure starvation:** V2's `IoLoopV2` reads aggressively from the socket, fills `parsed_cmd_q_len_` quickly, triggers `post_over_limit` → `io_event_.await()`, and the wake-up logic may be too slow or unfair under 50 concurrent connections.
2. **Fiber scheduling unfairness:** With 50 connections per thread, the proactor's fiber scheduler may starve some fibers while others monopolise the CPU — especially if V2's single-fiber-per-connection model doesn't yield often enough during batch execution.
3. **Reply flushing delays:** V2 defers flushing until idle-await. If the fiber never reaches idle (always has more commands to parse), replies pile up and the client blocks waiting, creating a feedback loop.
4. **Lock contention on shared structures:** The global `QueueBackpressure` notification (`NotifyPipelineWaiters`) wakes all connections on a thread simultaneously — thundering herd under high concurrency.

**Priority 1 action:** Profile which of these causes dominates. Start with tracing the time connections spend parked in `io_event_.await()` vs actively executing.

### F. Multishot HURTS OKB (Row 5 V2 vs Row 6)

| p | OKB V2 | OKB V2+MS | Δ |
|--:|-------:|-----------:|--:|
| 1 | 184.3 | 143.8 | **−22%** |
| 10 | 498.1 | 402.2 | **−19%** |

Because OKB processes commands so fast, multishot's provided-buffer overhead and ring-buffer replenishment outpaces the actual work. It also exacerbates the backpressure starvation (Insight E) by flooding the parser even faster.

### G. Multishot HELPS DF (Row 8 V2 vs Row 9)

| p | DF V2 | DF V2+MS | Δ |
|--:|------:|---------:|--:|
| 1 | 151.2 | 172.2 | **+14%** |
| 10 | 405.2 | 424.4 | +5% |
| 50 | 463.1 | 491.6 | +6% |

Because DF's engine is slower, the thread is busy in DashTable while the kernel continues pulling packets via multishot in the background — a net win.

### H. Multishot nearly closes the DF V2 gap (Row 8 V1 vs Row 9)

| p | DF V1 | DF V2+MS | gap |
|--:|------:|---------:|----:|
| 1 | 179.6 | 172.2 | −4% |
| 10 | 429.2 | 424.4 | −1% |
| 50 | 544.3 | 491.6 | −10% |

At p=1 and p=10, multishot recovers V2 to within 1–4% of V1. At p=50 there's still ~10% left. Note: multishot deadlocks at p≥100 so we cannot yet validate the full pipeline range.

### I. DF engine cost (Row 5 V2 vs Row 8 V2)

| p | OKB V2 | DF V2 | engine overhead |
|--:|-------:|------:|---------------:|
| 1 | 184.3 | 151.2 | −18% |
| 10 | 498.1 | 405.2 | −19% |
| 100 | 593.6 | 445.2 | −25% |

DashTable + transactions add 18–25% under load. At SConn it's negligible.

### J. OKB V1 is faster than Valkey — the helio I/O layer is not a handicap (Row 1 vs Row 3 V1)

OKB is built on **helio** — Dragonfly's I/O and fiber scheduling library. This comparison checks whether the helio architecture itself (fiber scheduler, io_uring proactor) costs performance vs Valkey's traditional event loop.

| p | VK | OKB V1 | OKB/VK |
|--:|---:|-------:|-------:|
| 1 | 13.8 | 14.6 | 1.06 |
| 10 | 85.3 | 89.7 | 1.05 |
| 100 | 247.1 | 281.5 | **1.14** |

---

## Conclusions

1. **V2 has a real loop-level regression** (Insight A: −40% at p=100 on 1 thread / 1 connection). Grows with pipeline depth, suggesting per-batch overhead.

2. **V2's architecture is correct for multi-thread fan-out** (Insight B: +132% over V1 at 4 threads / 1 connection where V1 serialises).

3. **The DF production regression is real: 16–21%** (Insight D) plus much worse tail latency (42 ms vs 16 ms p99.9).

4. **V2 has a severe tail-latency problem** (Insight E): p99.9 = 95 ms on OKB, 42 ms on DF. Root cause not yet proven — could be backpressure starvation, fiber scheduling unfairness, flush timing, or contention. Profiling is the #1 next step.

5. **Multishot's sign depends on engine weight** (Insight F/G): hurts OKB (−22%), helps DF (+14%).

6. **Multishot nearly closes the DF gap at low pipeline** (Insight H): −4% at p=1, −1% at p=10. Not yet testable at p≥100 due to deadlock.

7. **Insight C is invalidated (verified in code):** OKB V1 lacks squashing — every command does an individual cross-thread hop. The apparent V2 advantage at 50 connections is an artefact: connection-level parallelism hides both V1's dispatch inefficiency and V2's loop overhead. A proper comparison requires implementing squashing in OKB V1 first.

---

## Finding: Latency Distribution at pipeline=100 (OKB V2, 4t, multi-conn, SET 2048B)

Reproduced on 2026-06-12 with `memtier_benchmark -s 172.31.30.209 -p 6379 -t 2 -c 25 --pipeline=100 --ratio=1:0 -d 2048 --key-pattern=R:R --key-prefix=bench --test-time=15`:

- **593k ops/s**, AVG latency **8.4ms**, p50=6.9ms, p99=25.3ms, p99.9=213ms

Key percentiles from the full histogram:

| Percentile | Latency (ms) | Observation |
|---:|---:|---|
| 50% | 6.9 | Median per-command — dominated by position-in-batch |
| 90% | 14.5 | Still normal: last ~10 commands in a typical batch |
| 99% | 25.3 | 1% of commands > 25ms — moderately slow batches |
| 99.86% | 52.5 | End of the "normal" distribution |
| 99.87% | 205.8 | **JUMP** — bimodal: separate failure mode begins |
| 99.95% | 430-630 | Severe stalls (~0.05% of commands) |
| 100% | 835 | Worst case: single command saw 835ms latency |

**Key insight**: The distribution is bimodal. Commands up to p99.86 follow a smooth curve (normal V2 processing). Above that, latencies jump 4× to 200ms+ — a completely different failure mode affecting <0.15% of batches. These are rare catastrophic stalls, not accumulation of per-command overhead.

**Investigation approach**: Added timing instrumentation to identify the stall source:
1. `ParseLoop` batch timing (parse→execute→reply cycle) — catches slow batches
2. `IoLoopV2` idle-await timing — catches fiber sleeping too long between batches
3. `IoLoopV2` backpressure-await timing — catches memory-limit parking stalls

---

## Recommended Next Steps

| Priority | Action | Driven by |
|---:|--------|-----------|
| 1 | Run instrumented build to identify which mechanism causes the 200ms+ bimodal stalls (batch processing? idle-await? backpressure?) | Finding above |
| 2 | Profile what causes the 95ms / 42ms p99.9 tail latency in V2 (backpressure? scheduling? flush timing? contention?) | Insight E |
| 3 | Prototype squashing in OKB V1 `DispatchManyCommands` to get the true V1 baseline | Insight C: current V1 is crippled without it |

**Note**: Roman is already implementing V2 squashing on `origin/FixV3` branch (`feat: add vectorized pipeline squashing for V2 dispatch`). Priority 3 may be superseded by his work.

---

## Log Files

| # | File |
|--:|------|
| 1 | `valkey_1t_single_141259.log` |
| 2 | `valkey_1t_multi_141519.log` |
| 3 | `ok_v1v2_1t_single_141746.log` |
| 4 | `ok_v1v2_4t_single_142234.log` |
| 5 | `ok_v1v2_4t_multi_142718.log` |
| 6 | `ok_v2_ms_4t_multi_154803.log` |
| 7 | `dfly_4t_single_143353.log` |
| 8 | `dfly_4t_multi_155408.log` |
| 9 | `dfly_v2_ms_4t_multi_163031.log` |

## Scripts

- `run_stage1.sh` — orchestrator (9 rows)
- `bench_v2.sh` — benchmark harness
