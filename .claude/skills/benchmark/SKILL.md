---
name: benchmark
description: >
  Benchmark Dragonfly (and compare against Valkey/Redis) on local or remote
  cloud instances, then produce a performance + memory report with charts and a
  raw-data appendix. Use this whenever the user runs /benchmark, or asks to
  benchmark / load-test / measure throughput, QPS, latency, or memory efficiency
  of Dragonfly or another Redis-compatible server, to compare Dragonfly vs
  Valkey or Redis, or to generate a benchmark report. Trigger it even when the
  user just describes the goal ("see how fast Dragonfly is on this box",
  "compare memory per key vs Valkey") without saying the word "benchmark".
argument-hint: (optional) free-form spec, e.g. "server=10.0.0.1 client=10.0.0.2 strings vs-valkey"
allowed-tools: Bash, Read, Write, Edit, AskUserQuestion
---

# Benchmark Dragonfly

Drive a benchmark of Dragonfly (optionally side-by-side with Valkey or Redis) on
local or remote instances, collect throughput / latency / memory numbers, and
assemble a report that mirrors the team's published format: a narrative TLDR,
per-instance metric tables, charts, and an appendix with the exact commands and
raw output so anyone can reproduce it.

The whole point of the report is **trust**: a reader should be able to see the
headline numbers, understand the conditions they were measured under, and
re-run the exact commands themselves. Everything below serves that goal.

## When you start

If the user passed a free-form spec in `$ARGUMENTS`, parse what you can from it
(IPs, use-cases, comparison targets) and only ask about what's missing. The work
splits into clear phases — walk through them in order, but stay flexible: the
user may already have servers running, or may only want the report half (handed
raw output to format). Read what they're actually asking for.

Phases: (1) gather spec → (2) prepare instances → (3) network tuning →
(4) start monitoring → (5) run workloads → (6) extract metrics, charts, report.

The two reference files carry the operational detail — read them before acting,
not from memory:

- `references/running-benchmarks.md` — how to prep instances, start servers,
  drive `dfly_bench` and `memtier_benchmark`, run Valkey/Redis for comparison,
  apply network tuning, and measure memory. **Read this before Phase 2.**
- `references/report-template.md` — the exact report structure, with an example.
  **Read this before Phase 5.**

Bundled `scripts/` (reuse these instead of re-improvising the same loops — they
encode the safety discipline learned the hard way):

- `bench_server.sh start|stop|status <ssh> <binary> [maxmem] [port]` — launch a
  server tracking its PID; **stop kills by PID and verifies the process is gone +
  port free** (tmux kill-session does not reap it).
- `monitor_fill.sh <server_ssh> <client_ssh> [session] [abort_pct]` — the live
  RSS guard: polls `used_memory_rss`, prints a line per sample, aborts the write
  if RSS crosses the threshold (default 92%). Its output is also the input to
  `plot_fill.py`.
- `capture_result.sh <server_ssh> <client_ssh> <log> <outdir> [label]` — saves
  the run summary + INFO memory + **OS-level `free` snapshot** (`host_memory.txt`)
  and exits non-zero if there were any eviction errors (enforces the zero-error
  rule). The OS snapshot matters: `used_memory_rss` and host free can diverge
  significantly — e.g. with `MIMALLOC_ALLOW_LARGE_OS_PAGES=1` (default), mimalloc
  holds large-page arenas that the OS counts as used but INFO doesn't fully reflect,
  making the machine appear near-OOM while RSS looks fine.
- `collect_metrics.py` / `make_charts.py` — metrics + charts from dfly_bench
  JSON (GET/SET workloads).
- `plot_fill.py` — memory-fill chart from `monitor_fill.sh` output, for
  `--command` workloads (e.g. SETEX) that produce no JSON time series.

Every benchmark output directory must include a `commands_audit.log`
artifact with the exact command lines used for setup, server launch, writes,
reads, captures, diagnostics, and chart/report generation. Raw data without the
commands that produced it is not reproducible.

**Run-control scripts are mandatory for benchmark phases.** For Phase 5 runs,
use the bundled scripts as the default API:

- start/stop/status servers with `scripts/bench_server.sh`;
- monitor long write fills with `scripts/monitor_fill.sh`;
- capture write results and enforce the zero-error rule with
  `scripts/capture_result.sh`;
- generate metrics/charts with `scripts/collect_metrics.py`,
  `scripts/make_charts.py`, and `scripts/plot_fill.py`.

Inline SSH is fine for inspection, package setup, one-off diagnostics, and
bounded foreground probes, but not as a replacement for the run-control scripts
in the main benchmark path. If a script is not usable for a run, document the
reason in `commands_audit.log` and in the report appendix before using a manual
equivalent.

## Phase 1 — Gather the benchmark spec (interactive)

Use `AskUserQuestion` to pin down anything not already given. You need:

1. **Where it runs.** Local, or remote instances? For remote, get the **server**
   host/IP and the **client** host/IP (the load generator should run on a
   separate machine from the server — co-locating them steals CPU and corrupts
   the numbers). Get the SSH user / key if not obvious. On cloud instances,
   **always use the private/internal IP** (VPC-internal) for both SSH and as the
   load generator's `-h` target — the public IP routes through the internet
   gateway, adding latency and egress cost, and the result measures the gateway
   rather than the server.
2. **Use-cases.** Default menu: simple strings (SETEX writes + GET reads),
   pipelined / peak-throughput, memory efficiency. Always also ask whether there
   is a **specific focus** (a particular command, data type, key distribution,
   value size, or scenario) — the canned use-cases are a starting point, not a
   cage.
3. **Comparison targets.** Dragonfly only, or also Valkey / Redis? Which
   versions?
4. **Instance specs** (for the report's conditions section): instance type, CPU
   count, OS/kernel. Read these directly from the box at the start of Phase 2
   rather than asking the user.
5. **Load parameters** if the user has opinions: value size (`-d`), key space
   (`--key_maximum`), connections (`-c`), pipeline depth, test duration. Pick
   sensible defaults (see the reference) and tell the user what you chose.
   **Sizing the key count is the consequential decision** — see the reference's
   "Sizing the load" section. The dataset must fill a large fraction of instance
   RAM (benchmarking a near-empty instance is wrong) but stay clear of OOM
   (~60–75% of RAM, headroom to spare). `--key_maximum` bounds the footprint, so
   compute it from instance RAM ÷ estimated bytes-per-entry, calibrating with a
   short pilot write if unsure.

Summarize the resolved spec back to the user in a short block before running
anything, so a wrong assumption gets caught before machines spin.

## Phase 2 — Prepare the instances

- Record instance specs (`nproc`, `uname -r`, cloud metadata) — these go in the
  report's conditions section. Use the **private IP** when connecting between
  instances (see Phase 1).
- A remote instance usually already has `~/projects/dragonfly`. If not, clone it
  there. Build a **release** binary (`build-opt/dragonfly`, `build-opt/dfly_bench`)
  unless the user wants a published release downloaded instead — follow their
  instruction on build-vs-download.
- For comparison targets, pull the Valkey/Redis docker image on the server box.
- Verify everything launches and is reachable from the client box (a quick
  `redis-cli -h <server-private-ip> ping`) before committing to a long run.

Run each step individually over SSH — do not chain launch, polling, capture, and
the next phase into one command. If a tool call is interrupted, remote work keeps
running but goes uncaptured. Start the server; start the write; monitor it;
capture; start reads — one bounded step at a time.

## Phase 3 — Network tuning (optional, remote only)

High-throughput numbers on cloud hardware depend heavily on IRQ/SMP affinity
tuning. If the user wants maximal numbers, apply the tuning described in the
reference and **record exactly what you did** — it belongs in the appendix and
materially affects the results. If skipped, say so in the report; untuned and
tuned numbers are not comparable.

## Phase 4 — Start the monitoring stack (server machine)

Start the local Prometheus + Grafana stack on the **server** machine so you get
continuous, fine-grained time-series (server CPU, memory, command rates, host
metrics) spanning the whole benchmark — richer than the load generator's own
output and the source of the most convincing report charts.

```bash
ssh <server> 'cd ~/projects/dragonfly/tools/local/monitoring && docker compose up -d'
# when benchmarking Valkey/Redis, also bring up the redis exporter:
ssh <server> 'cd ~/projects/dragonfly/tools/local/monitoring && docker compose --profile redis up -d'
```

Prometheus scrapes at 1s. **Always run servers on port 6380** (Dragonfly,
Valkey, and Redis alike). The bundled `prometheus.yml` ships pointing at `6379`,
so before bringing the stack up, fix the scrape targets:

```bash
sed -i 's/:6379/:6380/g' tools/local/monitoring/prometheus/prometheus.yml
```

(This is a workaround for a misconfigured default — if you have repo write
access, commit the corrected file once so future runs start clean.)

Restart Prometheus if it was already running, then verify targets are `UP` at
`http://<server>:9090/targets` before starting the load — a silently-down scrape
job means no data for that window.

Start monitoring **before** the workloads and leave it running across all runs,
so a single continuous window covers everything. See the reference's monitoring
section for what to read from Grafana (dashboards are pre-provisioned) and how to
pull panels into the report. Record the benchmark start/end timestamps — they
let you crop the Grafana window to exactly the run.

## Phase 5 — Run the workloads

For each (use-case × datastore) combination, see the reference for the precise
commands. The shape is always: **write → memory snapshot → read**. Use the
private IP as the load generator's `-h` target.

Pass `--json_out_file` to `dfly_bench` (and `--json-out-file` to memtier) when
the workload supports it — charts and headline numbers come from these files.
`dfly_bench --command` workloads (e.g. `SETEX __key__ 10000 __data__`) do not
produce JSON; save full stdout and chart the `monitor_fill.sh` output instead.

Drive the run with the bundled scripts: `bench_server.sh start`, write in a
client tmux session, `monitor_fill.sh` alongside (live RSS guard), then
`capture_result.sh` to save results. `capture_result.sh` enforces the
**zero-error rule** — any eviction/OOM error response makes the run invalid;
discard it, cut `--key_maximum` ~10–15%, and re-run. See the preamble and the
reference's "Sizing the load" section for the full rule.

**Prefill writes must use `--pipeline 30`.** Without pipelining, the client
stalls waiting for each reply and becomes the bottleneck before the server is
saturated — especially at high key counts. Use `--pipeline 30` for all write
(fill) phases. Drop to `--pipeline 1` (or omit it) only for latency-honest
read benchmarks where you want honest per-request numbers.

Initialize `commands_audit.log` at the start and append each command as it runs
(see the preamble for the requirement).

## Phase 6 — Extract metrics, build charts, assemble the report

1. **Headline metrics + memory.** Run the bundled extractor over the JSON files
   to pull QPS / P99 and compute bytes-per-entry (both `used_memory`- and
   `used_memory_rss`-based, per the user's choice to report both):

   ```bash
   python3 .claude/skills/benchmark/scripts/collect_metrics.py \
       --bench-json <run.json> [--bench-json <run2.json> ...] \
       --info <info_memory.txt> --dbsize <N> \
       --label "Dragonfly m7g.2xlarge"
   ```

   It prints a metrics block (and JSON) you can drop straight into the tables.

2. **Charts.** Generate QPS-over-time PNGs from the time series (this is what
   visually shows Dragonfly's smooth throughput vs a competitor's degradation):

   ```bash
   python3 .claude/skills/benchmark/scripts/make_charts.py \
       --series "Dragonfly=df_write.json" --series "Valkey=valkey_write.json" \
       --metric qps --title "Write throughput, m7g.2xlarge" \
       --out charts/write_m7g2xl.png
   ```

   Embed the PNGs in the report with relative paths. **For `--command` workloads
   (e.g. SETEX) there is no dfly_bench JSON** — use `plot_fill.py` instead to chart
   memory-fill over time from the `monitor_fill.sh` output:

   ```bash
   python3 .claude/skills/benchmark/scripts/plot_fill.py \
       --series "prebuilt=prebuilt/poll.txt" --series "release=release/poll.txt" \
       --interval 10 --ram-gib 30 --out charts/memory_fill.png
   ```

3. **Report.** Follow `references/report-template.md` exactly. Write a TLDR with
   the 3–5 genuinely important takeaways (scaling behavior, memory efficiency,
   any hardware surprise), then per-instance tables, then the appendix. The TLDR
   is interpretation, not a restatement of the tables — say what the numbers
   *mean*. Only claim what the data supports.

Save the report where the user asks (default: `benchmark-<YYYY>-<MM>.md` in the
repo root) and tell them the path plus a one-line summary of the headline
result.

## Cleanup

If you started servers, the monitoring stack, or comparison containers on remote
instances, offer to stop them (`docker compose down` in the monitoring dir).
If you applied network tuning that changes machine state, tell the user what
would need reverting. Don't tear anything down without asking — they may want to
inspect or re-run.
