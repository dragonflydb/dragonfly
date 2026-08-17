# Full-Sync Fanout Benchmark Runner

Run the complete benchmark with one argument:

```bash
./tools/full_sync_fanout_benchmark.py 4GiB
```

The runner compares full sync with fanout disabled and enabled for one master and every replica
count from 1 through 10. Every Dragonfly process uses 10 proactors. Ten client threads run random
`GET`, `SET`, `HGET`, `HSET`, and `INCRBY` commands against bounded master-side keyspaces while
each full sync runs. A client that loses its TCP connection reconnects automatically; the benchmark
fails only if a worker cannot reconnect for 30 seconds. Between cases, replicas execute
`REPLICAOF NO ONE` before they are stopped, so the next case starts only after the master has
removed the preceding replication links.

It expects `build-opt/dragonfly`. To use a binary in another location, set `DRAGONFLY_BIN` in the
environment. The only benchmark argument remains the data size.

The target must support Dragonfly's default `io_uring` backend with sufficient locked memory.
Unlimited locked memory is supported but not required: a finite limit, such as 3.83 GiB, is valid.
The runner raises its own soft limit to the inherited hard limit when permitted, then verifies that
every started process actually uses the io_uring backend. If a process falls back to epoll, increase
the locked-memory limit (for example, with `ulimit -l unlimited`) or correct the kernel/container
configuration and rerun the same command.

For the ten-replica case, allow at least roughly 12 times the requested data size in available
memory. The runner creates a timestamped `full-sync-fanout-benchmark-*` directory in the current
directory containing process logs, `results.json`, and a GitHub-ready `results.md` table.
