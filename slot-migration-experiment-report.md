# Dragonfly 1.40.8 — local slot migration experiments

Four primary processes, one proactor and one database shard each. Continuous cluster GETs run before, during and after migration.

**Decision: no tested combination met the goal of faster migration with unchanged GET-only P95/P99.** Every completed run increased P99 on at least one node relative to that run's own pre-migration baseline. The two profiles below improved migration time and read tails compared with default migration settings, but neither qualifies for a strict no-increase requirement.

For the simultaneous ring, the measured improvement is **18.34 → 13.36 seconds (27.1% less time)**. For three sources sending to D, it is **11.81 → 10.70 seconds (9.4% less time)**. Use these as measured tradeoffs and starting points for further tuning; an unchanged-tail profile has not been established.

## Conditions

- Release build from source tag `i1.40.8`, commit `c13712901762329149b13bd50090a2f18f210493`; server implementation unchanged.
- AMD Ryzen AI 7 350, 30 GiB RAM, Linux 7.0.0-38-generic, io_uring, loopback networking.
- Nodes A/B/C/D pinned to CPUs 0/1/2/3; cluster clients pinned to CPUs 4/5/6/7. No SMT siblings shared between these eight processes.
- Each node uses `--proactor_threads=1 --num_shards=1 --maxmemory=6gb`. Every server thread's affinity is checked at launch.
- 1,048,576 migrating string keys × 1,024 value bytes = 1 GiB per job; 16,384 resident GET keys per node outside migrating ranges. No TTLs or workload writes.
- Ten GET connections per client, pipeline one, go-redis/v9 v9.12.1; every returned value is validated. Three-significant-figure HDR histograms record every GET.
- CPU calibration: A 47.0%, B 46.4%, C 46.8%, D 46.6%. Calibrated client settings are unchanged during and across migration runs.
- Ten-second GET-only windows before and after migration; fresh original snapshots loaded before every run, outside measurement windows. Automatic snapshots disabled.
- Concurrency 1: A→B, B→C, C→D, D→A simultaneously (4 GiB total). Concurrency 2: A/B/C→D simultaneously (3 GiB total).

## Results

Recorded 129 completed runs and 29 failed runs, including default repetitions, the combination screen and finalist repetitions. Failed runs are excluded from speed rankings and retained in the raw data.

Times below are medians of completed repetitions; latency numbers are medians of each run's independently merged phase histogram percentiles, not averages of per-second percentiles.

Both tuned configurations lower P99 and shorten migration time compared with defaults. “Ring” means simultaneous A→B, B→C, C→D, D→A migrations. A/B/C→D means three simultaneous migrations into D.

| Parameter / result | Ring default | Ring tuned | A/B/C→D default | A/B/C→D tuned |
|---|---:|---:|---:|---:|
| `migration_buckets_cpu_budget` | 0.2 | 0.2 | 0.2 | 0.2 |
| `migration_buckets_serialization_threshold` | 10 | **25** | 10 | 10 |
| `migration_buckets_sleep_usec` | 500 | **250** | 500 | **250** |
| `slot_migration_throttle_us` | 0 | **20** | 0 | **20** |
| `snapshot_egress_limit_bytes` | 0 | **100mb** | 0 | 0 |
| `replication_stream_output_limit` | 1048576 | 1048576 | 1048576 | 1048576 |
| **Migration time (s)** | **18.34** | **13.36** | **11.81** | **10.70** |
| **GET P95 during migration (µs)** | **86** | **85** | **102** | **87** |
| **GET P99 during migration (µs)** | **265** | **178** | **163** | **140** |

Times in this comparison are medians of three runs. P95/P99 show the highest per-node median across A–D during migration, rather than a combined cluster percentile. The node determining that maximum can differ between profiles. Egress `0` means unlimited; output buffer `1048576` means 1 MiB.

- **Ring tuned:** 27.1% less migration time and 32.8% lower P99.
- **A/B/C→D tuned:** 9.4% less migration time and 14.1% lower P99.

Use the following startup flags on all four instances, alongside the existing one-proactor settings and CPU pinning. The source flags act where data is sent; the incoming throttle acts where it is received. Applying the same profile to all nodes covers both roles in the ring.

| Parameter | Default | Ring profile | A/B/C→D profile | What it controls |
| --- | :---: | :---: | :---: | --- |
| `migration_buckets_cpu_budget` | 0.2 | 0.2 | 0.2 | Source serialization CPU budget; a smaller value makes serialization less aggressive. It is not a cap on total node CPU. |
| `migration_buckets_serialization_threshold` | 10 | 25 | 10 | Source bucket traversal work before a scheduled sleep; larger batches give reads fewer scheduling opportunities. |
| `migration_buckets_sleep_usec` | 500 | 250 | 250 | Source sleep between batches, in µs; shorter sleeps reduce migration waiting time. |
| `slot_migration_throttle_us` | 0 | 20 | 20 | Destination sleep after roughly 100 µs of incoming processing, giving other commands time to run. |
| `snapshot_egress_limit_bytes` | 0 | 100mb | 0 | Source per-shard egress budget in bytes/second; 0 disables the cap. |
| `replication_stream_output_limit` | 1048576 | 1048576 | 1048576 | Pending output buffer threshold in bytes: 1 MiB. It is not a bandwidth limit. |

The ring profile changes four defaults: threshold 10→25, sleep 500→250, incoming throttle 0→20, and egress 0→100mb. The A/B/C→D profile changes two: sleep 500→250 and incoming throttle 0→20. Increasing the CPU budget to 0.35 or the output buffer to 4 MiB gave no material speed benefit in the paired screen, so both remain at their defaults.

These are configurations 30 and 12 in the raw data. Both were repeated three times and are within 0.12 seconds of the fastest repeated median for their case; among these near-ties, the smaller worst-node P99 is preferred. Status polling is at 100 ms. The parameter meanings above follow the tested version's [source serializer](https://github.com/dragonflydb/dragonfly/blob/c13712901762329149b13bd50090a2f18f210493/src/server/journal/streamer.cc), [incoming migration loop](https://github.com/dragonflydb/dragonfly/blob/c13712901762329149b13bd50090a2f18f210493/src/server/cluster/incoming_slot_migration.cc), and [egress flag definition](https://github.com/dragonflydb/dragonfly/blob/c13712901762329149b13bd50090a2f18f210493/src/server/server_state.cc).

## Read tails

The profile's GET-only and migration columns come from the same repeated profile runs. This is the comparison that answers whether migration increased normal read latency.

| Case / node | Default migration P95 / P99 (µs) | Profile GET-only P95 / P99 (µs) | Profile migration P95 / P99 (µs) |
| --- | :---: | :---: | :---: |
| ring / A | 62 / 117 | 61 / 67 | 62 / 105 |
| ring / B | 86 / 265 | 84 / 91 | 85 / 176 |
| ring / C | 62 / 119 | 61 / 82 | 62 / 97 |
| ring / D | 86 / 247 | 84 / 92 | 85 / 178 |
| fanin / A | 64 / 115 | 61 / 66 | 64 / 104 |
| fanin / B | 87 / 154 | 84 / 92 | 87 / 140 |
| fanin / C | 64 / 116 | 61 / 67 | 64 / 110 |
| fanin / D | 102 / 163 | 84 / 91 | 85 / 94 |

For example, ring node D still goes from P99 92→178 µs. In A/B/C→D, destination D is close to normal at 91→94 µs, but source A still rises from 66→104 µs. The destination throttle substantially improves D; source-side latency remains a problem. The ring cannot isolate these roles because every node sends and receives simultaneously.

Strict screening compares both P95 and P99 on every node with that run's GET-only window and includes the cleanup window. An additional throughput check requires at least 95% of preceding read QPS. Even without that additional check, no completed run preserved both percentiles on every node during migration. The selected profiles' median worst-node read-QPS retention was 91.2% for the ring and 93.4% for A/B/C→D.

- ring: 0 screened configurations met this strict criterion; 29 configurations had an observed failure.
- fanin: 0 screened configurations met this strict criterion; 0 configurations had an observed failure.

The separate common-setting validation used configuration 60 (budget 0.2, threshold 10, sleep 250µs, destination throttle 20µs, egress 100mb, buffer 4 MiB). Its three fresh repetitions per case are recorded separately:

| Case | Common-setting median (s) | Range (s) | Change versus default |
| --- | :---: | :---: | :---: |
| ring | 18.98 | 16.92–19.15 | +3.5% |
| fanin | 11.10 | 11.09–11.60 | -6.0% |

The common setting reduced read tails but its ring speed gain did not hold in the repetitions. Use the case-specific profiles for the measured speed improvements.

Observed ring failures were journal-write timeouts with stalled streams and low CPU, while host memory remained available. All 32 capped ring screens completed; 29 of 32 uncapped ring screens failed. The three default ring repetitions had completed earlier, so this is timing-dependent behavior in the tested version/workload. The mechanism is consistent with backpressure but was not established by a stack trace.


Source finalization explicitly uses `ClientPause::ALL` in [the tested version's implementation](https://github.com/dragonflydb/dragonfly/blob/c13712901762329149b13bd50090a2f18f210493/src/server/cluster/outgoing_slot_migration.cc). This also affects resident keys outside the migrated slots. Pacing flags do not remove that pause. The selected profiles still recorded median per-run maximum reads around 10 ms on ring nodes and 40 ms on fan-in sources; these measurements do not establish the cause of every slow read. Maximum latency and one-second peaks are saved alongside P95/P99 because a brief pause can disappear from whole-run percentiles.

![Parameter search](build-migration-results/fresh/charts/parameter-search.svg)

![Every tested combination](build-migration-results/fresh/charts/combination-matrix.svg)

## Parameter effects

Each percentage is the median of paired alternative/default results with the other five parameters held fixed. Only pairs where both runs completed are included; failure counts remain separate, so a failed run is never represented as a fast migration.

| Alternative | Ring time change | Fan-in time change | Successful pairs (ring / fan-in) |
| --- | :---: | :---: | :---: |
| `migration_buckets_cpu_budget=0.35` | +0.0% | -0.1% | 17 / 32 |
| `migration_buckets_serialization_threshold=25` | -29.9% | -0.5% | 16 / 32 |
| `migration_buckets_sleep_usec=250` | -15.2% | +0.1% | 17 / 32 |
| `slot_migration_throttle_us=20` | -0.3% | -9.4% | 16 / 32 |
| `snapshot_egress_limit_bytes=100mb` | +23.1% | +4.2% | 3 / 32 |
| `replication_stream_output_limit=4194304` | +0.3% | -0.0% | 16 / 32 |

For this workload, the useful speed controls were larger source batches and shorter sleeps in the ring, and the incoming throttle for A/B/C→D. These paired effects are conditional on the other settings; they cannot be added to predict a combination's result. In particular, retain the ring's egress cap because of the observed uncapped failures, despite its cost in the few successful matched pairs.

## Tuning toward unchanged P95/P99

The following is a proposed follow-up, not a validated no-increase configuration. Keep the same GET workload, CPU pinning, one proactor, and ten-second before/after windows. Record migration time for every candidate and compare each node with its own GET-only baseline.

1. **Keep the settings with no demonstrated reason to increase:** output buffer 1048576 and destination throttle 20 µs. Retain egress 100mb for the ring and 0 for A/B/C→D initially. Do not increase migration concurrency until the current cases meet the latency target.
2. **Reduce source interference first:** starting from the case profile, test CPU budgets 0.15 and 0.10, which were not in the original grid. If tails remain elevated during copying, restore source sleep from 250 to 500 µs; for the ring, also try threshold 25→10. Change one control at a time initially so its speed/latency cost is visible. Lower budgets, longer sleeps, and smaller batches are pacing controls; none guarantees an improvement in percentiles.
3. **If destination latency is the remaining problem:** test incoming throttle 20→30→40 µs, holding source settings fixed. Values 30 and 40 were not tested. This gives reads more scheduling time but may lengthen migration; measure both effects.
4. **Accept only a combination that meets the target:** repeat promising candidates at least three times with fresh data, checking P95/P99 on every node during copying, finalization, and the ten-second cleanup window. Keep read throughput comparable and inspect one-second P99 and maximum latency. Choose the shortest migration among combinations that pass. If finalization remains the source of spikes, investigate its pause separately; increasing serialization speed does not remove it.

Until a combination passes, the answer to “which parameters give fast migration without increasing P95/P99?” is **none demonstrated by this experiment**. The two profiles above are the measured improvements over default migrations.

## Appendix: reproduction and raw data

- [Final validation](build-migration-results/fresh/validation.json): 632 pinned server launches, 1,835,677,423 validated GETs, zero evictions/expiries, correct final data/ownership for all completed runs, and owned processes stopped.
- Workflow: [benchmark skill](.claude/skills/benchmark/SKILL.md). Reproduction helpers: [runner](tools/slot_migration_benchmark.py), [cluster client](tools/slot-migration-client/main.go), [report generator](tools/slot_migration_report.py).

- [Exact command audit](build-migration-results/fresh/commands_audit.log), [environment and binary hashes](build-migration-results/fresh/environment.json), [CPU calibration](build-migration-results/fresh/calibration.json).
- [All run outcomes and parameters](build-migration-results/fresh/runs.csv), [per-node phase metrics](build-migration-results/fresh/node-metrics.csv), [structured summary, effects and failures](build-migration-results/fresh/summary.json).
- Each run directory contains server logs, all opening/closing configurations, process/thread affinities, one-second CPU/RSS/host-memory samples, migration status history and full client histograms.
- The SSH benchmark scripts share a single server PID/port and cannot manage four local pinned processes. The local harness tracks each owned PID, verifies teardown and ports, and enforces zero eviction/expiry and data correctness. This deviation was recorded before workloads.
- Docker monitoring was unavailable because access to its socket was denied. Local process metrics and client HDR logs replace Grafana/Prometheus; charts come from these saved measurements.
- Prefill uses `DEBUG POPULATE` (internal batches of 32), then fixed snapshots. These writes and snapshots occur before measured GET windows.
- A preliminary memtier run produced substantial MOVED traffic with resident hash tags and only whole-run HDR output. It was excluded; the main runs use the bundled Go cluster client.
- Results describe this one-proactor, loopback, string workload. The tested parameter grid has two values per flag; untested values and production hardware may behave differently.

Server SHA256: `93b2d90608f4e5a99149bef36f28dfb9c68183a2e1b890b85494a90151eb873f`.

## Representative time series

The plotted repeats have durations closest to their case median; shaded areas cover migration through final ownership publication. Client maxima show brief source pauses that phase P99 can hide.

![Ring defaults](build-migration-results/fresh/charts/ring-default-00.svg)

![Ring selected configuration](build-migration-results/fresh/charts/ring-finalist-30.svg)

![Fan-in defaults](build-migration-results/fresh/charts/fanin-default-00.svg)

![Fan-in selected configuration](build-migration-results/fresh/charts/fanin-finalist-12.svg)
