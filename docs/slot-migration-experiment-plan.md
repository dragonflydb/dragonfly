# Four-node slot migration experiment

Find the fastest tested combination of migration parameters that preserves read P95/P99 on all
four nodes.
Use primaries A, B, C, and D with the same binary, CPU allocation, and shard/thread counts.
For the local experiment, start every node with `--proactor_threads=1 --num_shards=1`.

Run continuous, successful `GET` operations through a cluster client before, during, and after
migration. Use 1-KiB string values without TTLs or writes. Distribute the GET workload across all
four nodes and keep the read-probe keys outside the migrating slot ranges.

Calibrate the cluster client's concurrency and request pacing until GETs alone use approximately
50% of one CPU core per node, using the same CPU measurement for all nodes. Keep the calibrated
workload running with the same settings during migration. Total CPU can increase when migration
adds work; reducing GET traffic to maintain 50% total CPU would change the workload and skew the
latency comparison.

Use fresh, reproducible data for each run and disable snapshots/uploads and other background jobs.
Use the same data size per migration, for example 1 GiB, with enough destination memory.

| Case | Migration schedule |
| --- | --- |
| Baseline | No migration; continuous cluster-client GETs at approximately 50% CPU per node |
| Concurrency 1 | A to B, B to C, C to D, and D to A simultaneously |
| Concurrency 2 | A, B, and C to D simultaneously |

Concurrency 1 runs four simultaneous ring migrations: each node has one incoming and one outgoing
job. Concurrency 2 runs three simultaneous incoming migrations to D. Each transfer uses a range
originally owned by its source. Start all transfers in a case with the same opening configuration;
wait for all jobs to report `FINISHED`, then publish final ownership.

With 1 GiB per transfer, the first case moves 4 GiB and the second moves 3 GiB. Record bytes as well
as time, and compare each configuration against the default run of the same case.

Use startup arguments on all nodes so each setting applies wherever a node acts as a source or
destination. The values below define the initial combination search.

| Parameter | Default value | Alternative value |
| --- | --- | --- |
| `migration_buckets_cpu_budget` | `0.2` | `0.35` |
| `migration_buckets_serialization_threshold` | `10` | `25` |
| `migration_buckets_sleep_usec` | `500` microseconds | `250` microseconds |
| `slot_migration_throttle_us` | `0` | `20` microseconds |
| `snapshot_egress_limit_bytes` | `0`, disabled | `100mb` per shard thread per second, as an example budget |
| `replication_stream_output_limit` | `1048576` bytes (1 MiB) | `4194304` bytes (4 MiB) |

The starting values are source-code defaults; record any deployment overrides. Keep failure
timeouts and thread counts fixed.

Search for combinations in both migration cases:

1. Measure the default configuration three times in each case.
2. Screen every combination of the six parameters once: two values per parameter gives 64
   configurations per case. Start with the 16 combinations of CPU budget, serialization threshold,
   sleep time, and destination throttle, holding egress and buffering at defaults. Then cover the
   remaining combinations with the alternative egress and buffer values. Test combinations even
   when a parameter showed little benefit alone: another limit can hide its effect.
3. Repeat the three fastest configurations that meet the read-latency limits three times in each
   case. Refine around the best configuration with a few nearby parameter values if useful, and
   verify the final candidate in both cases.

For every run, reload the same initial dataset and run the calibrated GET workload for 10 seconds
before starting migrations. Keep GETs running throughout migration and for 10 seconds after
publishing final ownership. Verify final ownership and data after each run. Keep the calibrated
GET workload settings identical.

Record one result row per configuration, case, and repetition:

- All six parameter values and migration duration for each source/destination pair.
- Whole-batch duration, transferred logical bytes, and logical MiB/second.
- Client GET P95/P99, completed GET QPS, and errors/timeouts, separately for each node.
- CPU before/during migration, peak memory, and cutover/cleanup timestamps in Grafana.

Source cutover pauses ordinary client traffic, including GETs. Inspect latency spikes at cutover
separately from copying, including one-second peaks that an overall P99 could hide.

Rank configurations by median whole-batch migration time within each case, subject to read P95/P99
limits on every node, stable read throughput, and correct data. Report the best combination for each
case. For one common configuration, select a candidate that passes both cases and has the best
worst-case relative improvement against their respective defaults. Report the winning parameter
values, migration times, and read latency; the result is the best tested combination, not a claim
that untested values cannot improve it.

Code references: [source pacing](../src/server/journal/streamer.cc),
[destination throttle and replay](../src/server/cluster/incoming_slot_migration.cc),
[source cutover](../src/server/cluster/outgoing_slot_migration.cc),
[egress budget](../src/server/server_state.cc),
[stream buffering](../src/server/journal/buffered_socket_writer.cc),
[string serialization](../src/server/journal/cmd_serializer.cc), and
[migration job constraints](../src/server/cluster/cluster_config.cc).
