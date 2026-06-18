# Running benchmarks — operational reference

How to prepare instances, start servers, drive the load generators, and measure
memory. Read the section you need; you don't have to read top-to-bottom.

## Table of contents

- [Instance preparation](#instance-preparation)
- [Starting Dragonfly](#starting-dragonfly)
- [Starting Valkey / Redis for comparison](#starting-valkey--redis-for-comparison)
- [dfly_bench (default load generator)](#dfly_bench-default-load-generator)
- [memtier_benchmark (alternative)](#memtier_benchmark-alternative)
- [The standard strings workload](#the-standard-strings-workload)
- [Measuring memory](#measuring-memory)
- [Monitoring stack (Prometheus + Grafana)](#monitoring-stack-prometheus--grafana)
- [Network tuning](#network-tuning)
- [Sizing the load](#sizing-the-load)

---

## Instance preparation

The server and the load generator should live on **separate machines**. A
client co-located with the server competes for CPU and silently caps the
numbers — the report is then measuring the client, not the server.

Remote instances usually already have the repo at `~/projects/dragonfly`. Check
first; clone only if missing:

```bash
ssh <host> 'test -d ~/projects/dragonfly && echo present || echo missing'
# if missing:
ssh <host> 'git clone https://github.com/dragonflydb/dragonfly.git ~/projects/dragonfly'
```

**Look for prebuilt binaries first.** Before building or downloading, check the
repo's build output folders — the binary is often already there. Look in
`build-opt/` and `build-release/` (and `build-dbg/` only as a last resort — debug
builds give meaningless performance numbers):

```bash
ssh <host> 'ls -la ~/projects/dragonfly/build-opt/{dragonfly,dragonfly-aarch64,dfly_bench} 2>/dev/null; \
            ls -la ~/projects/dragonfly/build-release/{dragonfly,dfly_bench} 2>/dev/null'
```

A folder may hold several binaries — e.g. a locally-built `dragonfly` (dev/HEAD)
alongside a downloaded release `dragonfly-aarch64`; check `--version` on each to
know which is which, since "compare the prebuilt vs the release binary" is a
common request. The load generator (`dfly_bench`) lives in the same folder on the
client.

**Build vs download** — if no suitable prebuilt binary exists, follow the user's
instruction. To build a release binary from source on the instance:

```bash
ssh <host> 'cd ~/projects/dragonfly && ./helio/blaze.sh -release -DWITH_AWS=OFF -DWITH_GCP=OFF'
ssh <host> 'cd ~/projects/dragonfly/build-opt && ninja dragonfly dfly_bench'
```

This yields `build-opt/dragonfly` (the server) and `build-opt/dfly_bench` (the
load generator). Build `dfly_bench` on the **client** box and `dragonfly` on the
**server** box. Always benchmark release builds — a debug build's numbers are
meaningless for performance claims.

If the user prefers a published release, download the tarball from GitHub
releases and extract it instead of building.

Sanity-check reachability from the client before a long run:

```bash
redis-cli -h <server-ip> -p 6380 ping   # expect PONG
```

Collect environment facts for the report while you're on the box:

```bash
ssh <host> 'nproc; uname -r; cat /etc/os-release | head -2'
# cloud instance type, if on AWS:
ssh <host> 'curl -s http://169.254.169.254/latest/meta-data/instance-type'
```

---

## Starting Dragonfly

The published comparisons run Dragonfly with a minimal, persistence-free config
so the benchmark measures the data path, not disk:

```bash
./dragonfly --conn_use_incoming_cpu --dbfilename= --logtostderr --port 6380
```

- `--conn_use_incoming_cpu` — pin each connection's processing to the CPU that
  received it; improves locality under high connection counts.
- `--dbfilename=` — disable snapshot loading/saving.

Let Dragonfly use all cores by default (it auto-detects). Record the exact
launch command for the appendix.

**Start it so you can reliably stop it — capture the PID.** Launching under
`tmux`/`nohup` is fine, but `tmux kill-session` does **not** reliably reap the
server, and a stale server is a trap: a "fresh" server silently fails to bind the
port, your client reconnects to the *old* instance, and you measure stale data
(and waste the old dataset's RAM). Always record the PID and kill by PID:

```bash
ssh <server> 'cd ~/projects/dragonfly/build-opt; \
   nohup ./dragonfly --conn_use_incoming_cpu --dbfilename= --logtostderr --port 6380 \
       >/tmp/dfly.log 2>&1 & echo $! > /tmp/dfly.pid'
# ... benchmark ...
# Stop it and VERIFY it's gone before starting the next binary:
ssh <server> 'kill -9 $(cat /tmp/dfly.pid); sleep 3; \
   pgrep -af dragonfly | grep -v "pgrep\|bash -c" || echo "stopped"; \
   redis-cli -p 6380 ping 2>&1 | head -1   # expect Connection refused'
```

Between two binaries (e.g. prebuilt vs release), **fully stop and verify the
first is dead and the port is free before starting the second** — otherwise the
second never binds and you benchmark the first twice. Note that pattern-based
`pkill -f` can miss a process whose cmdline is the relative `./dragonfly`; killing
by the captured PID is the robust path.

---

## Starting Valkey / Redis for comparison

Run via docker on the server box. Valkey does not scale past ~8 io-threads on
current hardware, so more than that rarely helps — note the thread count you
used.

```bash
docker run --network=host --rm valkey/valkey:9.0 \
    --save "" --appendonly no --io-threads 8 --protected-mode no --port 6380
```

For Redis, substitute the `redis` image and equivalent flags. Run the comparison
target on the **same instance type** as Dragonfly, one at a time (not
simultaneously — they'd contend), so the comparison is apples-to-apples.

---

## dfly_bench (default load generator)

Source: `src/server/dfly_bench.cc`. Key flags:

| Flag | Meaning | Typical |
| --- | --- | --- |
| `-h` | server host | client→server IP |
| `-p` | server port | `6380` |
| `-c` | connections **per thread** | ~10; ≤25 max; 1–5 when pipelining |
| `-n` | requests per connection | sizes the run (omit if `--test_time`) |
| `--test_time` | run duration in seconds | e.g. `120` |
| `--qps` | target QPS per connection; `0` = unthrottled | `0` for peak |
| `-d` | value size in bytes | `64` for strings tests |
| `--key_maximum` | size of key space | tune to fill memory |
| `--key_dist` | `U` uniform, `N` normal, `Z` zipfian, `S` sequential | `U` default |
| `--ratio` | set:get ratio | `1:0` writes, `0:1` reads |
| `--command` | custom command template (`__key__`, `__data__`) | for non-string workloads |
| `--pipeline` | pending requests per connection | `>1` for peak throughput |
| `-P` | `memcache_text` for memcached protocol | empty for RESP |
| `--json_out_file` | **memtier-compatible JSON with per-second time series** | always set it |

`--json_out_file` only works with the built-in GET/SET workload (not with
`--command`, `--noreply`, or `--connect_only`). For custom-command workloads you
scrape the console summary instead.

**Console output.** Periodic lines look like:

```
12s: 40.0% done, RPS(now/agg): 815000/812340, errs: 0, hitrate: 99.8%, clients: 200
```

and the final summary:

```
Total time: 30s. Overall number of requests: 24370200, QPS: 812340, P99 lat: 787us
Latency summary, all times are in usec:
<histogram>
```

Save the full stdout — the histogram and final QPS go in the appendix; the
`collect_metrics.py` script prefers the JSON but can fall back to this.

Also save the command lines themselves under the run output directory, e.g.
`commands_audit.log`. Record setup, server launch, write/read load,
capture, diagnostics, and chart/report commands as they are run; stdout alone is
not raw data because it cannot reproduce the run. Initialize this file before
the first workload so planned script invocations are visible up front, then
append commands as they execute.

**Use the skill scripts for the main run path.** The main benchmark phases should
use:

```bash
.claude/skills/benchmark/scripts/bench_server.sh start|start-docker|stop|stop-docker|status ...
.claude/skills/benchmark/scripts/monitor_fill.sh ...
.claude/skills/benchmark/scripts/capture_result.sh ...
.claude/skills/benchmark/scripts/collect_metrics.py ...
.claude/skills/benchmark/scripts/make_charts.py ...
.claude/skills/benchmark/scripts/plot_fill.py ...
```

Inline SSH is acceptable for discovery, package/setup checks, bounded
foreground probes, and diagnostics. If you bypass a run-control script for a
main benchmark phase, record the reason in `commands_audit.log` and the appendix before
running the manual equivalent.

**Keep orchestration phase-separated.** Do not run a single local shell command
that starts a remote tmux benchmark, polls until completion, captures results,
and starts the next phase. If the local command is interrupted, the remote tmux
process can finish while the local capture logic is gone, leaving the run idle
and uncaptured. Prefer bounded foreground `dfly_bench` commands for known-length
runs. If you need tmux/nohup for a long fill, use separate steps: launch it,
monitor it, verify it has exited, then run the capture/read commands separately.

**JSON structure** (`--json_out_file`):

```json
{
  "configuration": { "server": "...", "port": 6380, "clients": 200,
                      "threads": 8, "pipeline": 1, "ratio": "1:0" },
  "ALL STATS": {
    "Runtime": { "Total duration": 30000 },
    "Sets": { "Count": ..., "Average Latency": ..., "Percentile ...": ...,
              "Time-Serie": { "0": {...}, "1": {...}, ... } },
    "Gets": { ... }
  }
}
```

The per-second `Time-Serie` buckets are what the charts plot.

---

## memtier_benchmark (alternative)

Industry-standard, available as a docker image (`redislabs/memtier_benchmark`).
Use it when the user wants cross-validation against a tool Dragonfly doesn't
ship, or for the pipelined peak-throughput use-case. Example (from the repo's
k8s job):

```bash
memtier_benchmark -s <server> -p 6380 \
    --pipeline=30 --key-maximum=100000 -c 10 -t 2 --test-time=120 \
    --distinct-client-seed --hide-histogram --json-out-file memtier_out.json
```

`--json-out-file` produces the same shape `collect_metrics.py` and
`make_charts.py` read.

---

## The standard strings workload

This reproduces the published "Simple Strings" comparison. Run write then read,
with a memory snapshot in between.

**Write phase** (load the keyspace):

```bash
./dfly_bench -h <server> -p 6380 --qps=0 -d 64 --key_maximum=$KMAX -c $CONN \
    --pipeline 30 -n $N --json_out_file df_write.json --command "setex __key__ 10000 __data__"
```

Note: with `--command`, `--json_out_file` is ignored — for the canonical strings
test use the built-in SET path (`--ratio 1:0`) instead if you need the JSON/time
series, and reserve `--command "setex ..."` for when TTLs matter. For SETEX,
save stdout and parse/chart the periodic `RPS(now/agg)` lines or
`monitor_fill.sh` output.

**Memory snapshot** (immediately after writes — see next section).

**Read phase**:

```bash
./dfly_bench -h <server> -p 6380 --qps=0 -d 64 --key_maximum=$KMAX -c $CONN \
    -n $N --ratio 0:1 --json_out_file df_read.json
```

Choose `KMAX`, `N`, and `CONN` to (a) saturate the server and (b) load enough
data that instance memory is well utilized — see [Sizing the load](#sizing-the-load).

---

## Measuring memory

"Bytes per entry" is the memory metric in the report. Capture it right after the
write phase, before reads:

```bash
redis-cli -h <server> -p 6380 INFO memory > info_memory.txt
redis-cli -h <server> -p 6380 DBSIZE
```

From `INFO memory` you need `used_memory` (logical) and `used_memory_rss`
(physical resident set). Report **both** per-entry figures:

- `used_memory / DBSIZE` — logical bytes per entry (matches the published
  "Memory Per Entry").
- `used_memory_rss / DBSIZE` — physical bytes per entry, the real footprint
  including allocator overhead and fragmentation.

`collect_metrics.py` computes both when given `--info` and `--dbsize`.

For Valkey/Redis the same `INFO memory` fields exist, so the comparison is
direct. Capture the snapshot at the **same keyspace size** for both, or the
per-entry comparison is unfair.

---

## Monitoring stack (Prometheus + Grafana)

Lives in `tools/local/monitoring/` and runs on the **server** machine via docker
compose. It gives continuous, 1-second-resolution time series for the whole
benchmark window — server-side command rates and memory, plus host CPU/network
from node-exporter — which is both a sanity check (was the server actually
saturated? did it swap?) and the source of the report's best charts.

```bash
cd ~/projects/dragonfly/tools/local/monitoring
docker compose up -d                      # Prometheus, Grafana, node-exporter
docker compose --profile redis up -d      # also start redis-exporter for Valkey/Redis
```

Endpoints (on the server box): Prometheus `:9090`, Grafana `:3000`,
node-exporter `:9100`, redis-exporter `:9121`.

**What gets scraped:**

- `dragonfly` job — Dragonfly exposes Prometheus metrics directly at
  `/metrics` on its main port. The bundled `prometheus.yml` targets
  `host.docker.internal:6379`.
- `node-exporter` job — host CPU, memory, network.
- `redis-exporter` job (profile `redis`) — Valkey/Redis stats, also via
  `host.docker.internal:6379`.

**Port — standardize on 6380.** Always run benchmarked servers (Dragonfly,
Valkey, Redis) on port `6380`. The shipped `prometheus.yml` targets `6379`, so
before bringing the stack up, edit the `dragonfly` and `redis-exporter` jobs in
`tools/local/monitoring/prometheus/prometheus.yml` to point at `:6380`:

```bash
sed -i 's/:6379/:6380/g' ~/projects/dragonfly/tools/local/monitoring/prometheus/prometheus.yml
# if Prometheus is already up, pick up the change:
docker compose restart prometheus
```

Then confirm targets are `UP` at `http://<server>:9090/targets` before starting
load — otherwise Prometheus silently collects nothing for the server.

**Dashboards** are pre-provisioned in Grafana (`dragonfly.json`, `redis.json`,
`node-exporter.json`) — no manual import. For the report, note the benchmark
start/end timestamps so you can crop the Grafana time range to the exact run,
then capture the relevant panels (throughput, memory, CPU) — via the provisioned
`grafana-image-renderer` (the compose file includes a `renderer` service) or a
screenshot — and embed them. You can also query Prometheus directly over HTTP
(`/api/v1/query_range`) if you want raw series for `make_charts.py`.

Leave the stack up across all runs so one continuous window covers everything.

## Network tuning

On cloud hardware, peak throughput is gated by how network-interrupt load is
spread across CPUs. The published runs tuned **SMP affinity for IRQs** so the
NIC's interrupts fan out across multiple cores instead of saturating one.

This is instance- and NIC-specific. The general approach: identify the NIC's IRQs
(`/proc/interrupts`), then write CPU masks to `/proc/irq/<N>/smp_affinity` to
spread them. Many setups use the kernel's `set_irq_affinity.sh` helper shipped
with the NIC driver. Because the exact steps depend on the instance, confirm the
approach with the user if they want tuning, and **record the exact commands you
ran** — tuned and untuned numbers are not comparable, and the appendix must say
which was used.

If you skip tuning, state that plainly in the report.

---

## Sizing the load

Two independent things to size: **how many keys** (sets the memory footprint)
and **how long / how many requests** (sets steady-state and how full memory
gets). Get these wrong in either direction and the benchmark is misleading.

### The memory budget — the most important (and most dangerous) call

A write benchmark must land in the **sweet spot between two failure modes**:

- **Too few keys → wrong-but-safe.** Benchmarking a nearly-empty instance is
  not representative. A small dataset sits in CPU cache and hides the real cost
  of a large hash table (cache misses, allocator behavior, fragmentation). The
  QPS looks great and means nothing. *Using available memory is the whole
  point.*
- **Too many keys → OOM or eviction.** Push too far and the run is ruined two
  different ways (see the two ceilings below). Either way the numbers are
  garbage.

**There are TWO ceilings, not one. Stay under both.**

1. **Dragonfly's `maxmemory`.** Dragonfly auto-sets `maxmemory` to a fraction of
   RAM (≈ 78–80% by default). When memory crosses it, Dragonfly **rejects or
   evicts** writes — the load generator reports *error responses* and the run is
   contaminated. **Check it before the run** (`redis-cli -p 6380 config get
   maxmemory`, or the `maxmemory:` line in `INFO memory`).

   **Rejections start with a margin *below* `maxmemory`, not at it.** During a
   write, dash-table resizes transiently spike memory, so Dragonfly begins
   rejecting while steady-state `used_memory` is still well under the cap —
   observed rejections at `used_memory` ≈ 90% of `maxmemory` and even lower at the
   moment of a big resize. So keep steady-state `used_memory` to **≤ ~80% of
   `maxmemory`** to leave room for these spikes. If you need to fill more, raise
   `--maxmemory` explicitly — but then ceiling #2 (host RAM) binds.

   **Zero errors is the pass/fail bar.** *Any* eviction/OOM error contaminates
   the experiment — it slows the run (rejection stalls inflate P99) and stops the
   keyspace from growing, so QPS, latency, and bytes-per-entry are all off. After
   every write, **verify the load generator reported zero errors**
   (`grep -i error <write.log>`; dfly_bench prints `Got N error responses!`). If
   N > 0, the run is **invalid** — discard it, cut `--key_maximum` (start with a
   ~10–15% reduction), and re-run until errors are zero. Don't report a run with
   any errors, and don't try to "subtract" them out.
2. **Host physical RAM, measured by RSS.** The accurate measure of real memory
   pressure is **resident set size — `used_memory_rss`** (equivalently the
   process RSS). True OOM happens when RSS approaches host RAM. Do **not** panic
   over `free`'s `used`/`available` columns during a write: a high-rate write
   inflates **page cache** (clean, reclaimable) and dirty pages, which makes
   `available` dip alarmingly even though the kernel will reclaim it under
   pressure. On a 30 GiB box a 195M-key dataset showed `free` available dropping
   to ~10% while `used_memory_rss` was only ~21 GiB (71% of RAM) — the dataset
   was nowhere near OOM; the rest was reclaimable cache. **Track RSS, keep it
   below host RAM with a ≥5% margin**, and ignore page-cache noise.

**So size against RSS.** Target the dataset's RSS up to your fill goal while
keeping a real margin, and stay under `maxmemory`:

```
target_rss     = host_RAM * fill_fraction      # e.g. 0.90 for "fill 90%", never > 0.95
rss_per_entry  = used_memory_rss / DBSIZE        # from calibration (accurate)
key_for_host   = target_rss / rss_per_entry
key_for_maxmem = 0.90 * maxmemory / used_memory_per_entry   # avoid eviction
target_keys    = min(key_for_host, key_for_maxmem)
--key_maximum  = target_keys
```

The usual binding constraint is **`maxmemory`**, since its default (~78% of RAM)
is below a 90% fill target. **To genuinely fill 90% of RAM, raise `--maxmemory`
explicitly** (e.g. `--maxmemory 28gb` on a 30 GiB box) so Dragonfly doesn't evict
before RSS reaches the target — then RSS, watched live, is the safety signal.
Filling memory is the goal; the failure modes to avoid are eviction (used_memory
> maxmemory) and RSS approaching host RAM.

`*_per_entry` (≈ `value_size + 60–100 B` overhead for small strings; more for
TTL'd keys and other types) is best **calibrated**, not guessed:

1. Run a short pilot write inserting a known, modest number of distinct keys
   (e.g. `--key_maximum=5000000 --key_dist S` with enough requests to write them
   all).
2. Read `used_memory` / `DBSIZE` → real `bytes_per_entry`. Note this slightly
   *under*-estimates at scale (fragmentation grows), so keep margin.
3. Plug into the formula above for the full run.

Calibrate **per datastore** — Dragonfly and Valkey have different per-entry
overhead, which is exactly what the report measures. Size each so its dataset
fills the same target fraction; comparing at the same `DBSIZE` keeps the
bytes-per-entry comparison fair (Dragonfly will simply use less RAM at that
key count, which is the finding, not a sizing error).

### How distinct keys actually accumulate

With uniform random keys over `--key_maximum`, distinct keys (and therefore
memory) climb fast at first, then asymptote toward `key_maximum` as repeats
start landing on existing keys. **Memory plateaus near `key_maximum` distinct
entries** — writing *longer* past that point just overwrites existing keys and
does **not** grow memory. That's the key safety property: **`--key_maximum`, not
run length, bounds the footprint.** Run length controls how *close* to that
plateau you get and how long you hold steady state.

So to reliably fill to the target without OOM:

- Set `--key_maximum` to the computed `target_keys`.
- Make the write phase long enough to insert essentially all of them — total
  requests (`-c` × threads × `-n`, or a `--test_time` that covers it) should be
  a small multiple of `key_maximum` (~2–3×) so coverage is near-complete.
- Use `--key_dist S` (sequential) if you want *exactly* `key_maximum` distinct
  keys with no repeats and a precise fill; uniform is fine when you just need to
  approach the plateau.

### Watch memory live — guard on RSS

Never run a long write blind. The accurate safety signal is **`used_memory_rss`**
(resident set), not `free`'s `available` (page cache makes it dip transiently and
is reclaimable). Poll RSS every few seconds and **abort the write if RSS exceeds
~90–95% of host RAM**, and separately watch `used_memory` against `maxmemory` to
catch approaching eviction. Example guard loop:

Run the guard as its own bounded step, not as part of a larger launch/poll/capture
mega-command. After the guard reports `DONE`, immediately run a separate capture
step (`DBSIZE`, `INFO memory`, stdout log, and zero-error check), then start the
read phase.

```bash
TOTAL=$(ssh <server> 'free -b | awk "/^Mem:/{print \$2}"')
for i in $(seq 1 60); do
  sleep 8
  read DB RSS USED < <(ssh <server> 'redis-cli -p 6380 dbsize | tr -d "\r"; \
     redis-cli -p 6380 info memory | grep -E "^used_memory_rss:|^used_memory:" | cut -d: -f2 | tr -d "\r"' | paste -sd" ")
  pct=$(awk "BEGIN{printf \"%.0f\", $RSS/$TOTAL*100}")
  echo "dbsize=$DB rss=$(awk "BEGIN{printf \"%.1f\",$RSS/2^30}")GiB (${pct}% of RAM)"
  if [ "$pct" -ge 92 ]; then echo "ABORT: RSS ≥92% of RAM"; \
     ssh <client> 'tmux kill-session -t bench'; break; fi
done
```

Ways to read current memory:

- **`INFO memory` → `used_memory_rss`** — the accurate measure; the guard's
  input. Also compare `used_memory` to `maxmemory` (eviction warning).
- **`/metrics` endpoint (Dragonfly only)** — `curl -s
  http://<server>:6380/metrics | grep -i mem` gives Dragonfly's live RSS/used
  gauges without a redis client. **Valkey/Redis do not serve `/metrics`** — for
  those, use `INFO memory` directly or scrape via the redis-exporter (see the
  monitoring section). `INFO memory` works for all three datastores, so it's the
  portable choice for the guard.
- **Monitoring stack** — Prometheus/Grafana plots these over time; the source of
  the memory-over-time chart. (`free`/node-exporter available is fine for the
  chart, just not the trigger.)

If RSS approaches the limit faster than expected, **stop the write early** —
you've already filled enough to measure; a partially-filled run with clean
numbers beats one that evicted or OOMed.

### The other knobs

- **Connections (`-c` is *per thread*):** keep `-c` modest — **never above 25
  connections per thread, usually around 10**; for pipelined runs drop it to
  **1–5 per thread** (each connection already has many requests in flight, so
  more connections just add contention without more throughput). Scale total
  load by adding **threads** (and, if needed, a second client box), not by
  cramming connections onto each thread. Raise the total until QPS stops rising;
  if it keeps climbing at your max, the client is the bottleneck — add threads or
  another client machine, not more `-c`.
- **Duration / steady state:** beyond filling memory, hold at least 30–60s of
  steady traffic so the reported QPS/P99 reflect steady state, not warm-up.
  Watch the periodic RPS line stabilize before trusting the number.
- **Pipeline:** Use `--pipeline 30` for all write/prefill phases — without it
  the client stalls on each reply and becomes the bottleneck before the server
  saturates. Use `--pipeline 1` (or omit) only for latency-honest read
  benchmarks, and label those numbers accordingly.

Always note in the report the key count, value size, resulting memory fill (%
of instance RAM), and whether the run was client-bound — an under-driven server
or a barely-filled instance both produce numbers that quietly mislead.
