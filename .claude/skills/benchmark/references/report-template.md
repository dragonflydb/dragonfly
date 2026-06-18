# Report template

The report mirrors the team's published benchmark format. Structure matters: a
reader scans the conditions, jumps to the TLDR for the takeaways, checks the
per-instance tables for numbers, and uses the appendix to reproduce. Follow this
shape.

Use Markdown tables. Center-align numeric columns (`:---:`). Bold the winning
datastore in comparison tables when there's a clear winner.

## Structure

```markdown
# <Title> — e.g. "Comparing Dragonfly vs Valkey on AWS"

<1–2 sentences: what was compared, on what hardware, what workloads.>

## Conditions

Dragonfly launch command (identical across all configs):

    ./dragonfly --conn_use_incoming_cpu --dbfilename= --logtostderr --port 6380

<Comparison target> launch command:

    docker run --network=host --rm valkey/valkey:9.0 --save "" --appendonly no \
        --io-threads 8 --protected-mode no --port 6380

Instances: <server types + CPU counts>, client on <client instance type>.
OS: <distro + kernel>. Network tuning: <applied / none — one line on what>.

## <Use-case name> — e.g. "Simple Strings"

<1–2 sentences describing the workload and why it matters.>

Write command:

    ./dfly_bench -n $N -p 6380 --qps=0 -d 64 --key_maximum=$KMAX -c $CONN \
        --pipeline 30 --ratio 1:0 --json_out_file df_write.json

Read command:

    ./dfly_bench -n $N -p 6380 --qps=0 -d 64 --key_maximum=$KMAX -c $CONN --ratio 0:1

### TLDR

* **<Datastore>** <the genuinely important takeaway — scaling, efficiency, a surprise>.
* <3–5 bullets total. Interpretation, not a restatement of the tables.>

## <Instance type> — e.g. "m7g.2xlarge"

<Optional: one sentence on anything notable, e.g. throughput stability over time.>

![Write throughput over time](charts/write_m7g2xl.png)

### QPS and memory

| Metric | Dragonfly | Valkey |
| ----- | :---: | :---: |
| Write QPS (k) | 816 | 548 |
| Read QPS (k) | 848 | 811 |
| Memory / entry — logical (bytes) | 127 | 149 |
| Memory / entry — RSS (bytes) | 138 | 162 |

### Latency

| Backend | Write P99 (us) | Read P99 (us) |
| :---: | :---: | :---: |
| **Dragonfly** | 787 | 887 |
| **Valkey** | 3283 | 951 |

<Repeat the per-instance block for each instance type tested.>

## Appendix — raw data

### Environment

| Property | Server | Client |
| --- | --- | --- |
| Instance type | m7g.2xlarge | c7gn.4xlarge |
| vCPUs | 8 | 16 |
| OS / kernel | Ubuntu 24.04.2 / 6.8.0 | ... |
| Dragonfly version | <git hash / release> | — |
| Network tuning | IRQ SMP affinity spread across CPUs | — |

### Exact commands

<Every command line actually run, per phase and per datastore, verbatim —
including the launch commands, write/read load commands, and any tuning steps.>

### Raw output

<Per run: the final dfly_bench/memtier summary + latency histogram, and a
reference to the saved JSON file (df_write.json, etc.). Include INFO memory
snapshots used for the per-entry figures.>
```

## Notes on writing the TLDR

The TLDR is the part people actually read. Make each bullet a claim the data
supports, phrased as a takeaway:

- Good: "Dragonfly scales vertically with cores and sustains up to 10x the
  throughput of a single Valkey process on large instances."
- Weak: "Dragonfly had higher QPS." (just restates the table)

Call out hardware surprises honestly — e.g. an instance type that didn't scale
linearly — because they affect how a reader should interpret the numbers. If a
run was client-bound or otherwise caveated, say so here rather than burying it.

## Charts

One chart per instance per phase is usually enough: QPS-over-time, with one line
per datastore. The visual story is throughput *stability* — Dragonfly holding a
flat line while a competitor degrades as its table grows is more convincing than
a single aggregate number. Generate with `scripts/make_charts.py` from the load
generator's time-series JSON, save under a `charts/` dir next to the report, and
embed with relative paths.

When the monitoring stack ran (Phase 4), the Grafana dashboards add server-side
panels that the load generator can't show — memory growth over the write phase,
per-shard CPU, host network saturation. Crop the Grafana time range to the run
window and pull the relevant panels (rendered PNG or screenshot) into the
per-instance section. These are especially good for the memory-efficiency story
and for proving the server (not the client) was the bottleneck. Note in the
appendix that the data came from the monitoring stack, and which scrape port was
used.
