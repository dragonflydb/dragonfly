<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml) [![Twitter URL](https://img.shields.io/twitter/follow/dragonflydbio?style=social)](https://twitter.com/dragonflydbio)


[Quick Start](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start) | [Discord Chat](https://discord.gg/HsPjXGVH85) | [GitHub Discussions](https://github.com/dragonflydb/dragonfly/discussions) | [GitHub Issues](https://github.com/dragonflydb/dragonfly/issues) | [Contributing](https://github.com/dragonflydb/dragonfly/blob/main/CONTRIBUTING.md)

### Probably, the fastest in-memory store in the universe!

Dragonfly is a modern in-memory datastore, fully compatible with Redis and Memcached APIs. Dragonfly implements novel algorithms and data structures on top of a multi-threaded, shared-nothing architecture. As a result, Dragonfly reaches x25 performance
compared to Redis and supports millions of QPS on a single instance.

Dragonfly's core properties make it a cost-effective, high-performing, and easy-to-use Redis replacement.


## Benchmarks

<img src="http://static.dragonflydb.io/repo-assets/aws-throughput.svg" width="80%" border="0"/>

Dragonfly is crossing 3.8M QPS on c6gn.16xlarge reaching x25 increase in throughput compared to Redis.

99th latency percentile of Dragonfly at its peak throughput:

| op  |r6g | c6gn | c7g |
|-----|-----|------|----|
| set |0.8ms  | 1ms | 1ms   |
| get | 0.9ms | 0.9ms |0.8ms |
|setex| 0.9ms | 1.1ms | 1.3ms


*All benchmarks were performed using `memtier_benchmark`  (see below) with number of threads tuned per server type and the instance type. `memtier` was running on a separate c6gn.16xlarge machine. For setex benchmark we used expiry-range of 500, so it would survive the end of the test.*

```bash
  memtier_benchmark --ratio ... -t <threads> -c 30 -n 200000 --distinct-client-seed -d 256 \
     --expiry-range=...
```

When running in pipeline mode `--pipeline=30`, Dragonfly reaches **10M qps** for SET and **15M qps** for GET operations.

### Memcached / Dragonfly

We compared memcached with Dragonfly on `c6gn.16xlarge` instance on AWS.
As you can see below Dragonfly dominates memcached for both write and read workloads
in terms of throughput with a comparable latency. For write workloads, Dragonfly has also better latency, due to contention on the [write path in memcached](docs/memcached_benchmark.md).

#### SET benchmark

| Server    | QPS(thousands qps) | latency 99% | 99.9%   |
|:---------:|:------------------:|:-----------:|:-------:|
| Dragonfly |  🟩 3844           |🟩 0.9ms     | 🟩 2.4ms |
| Memcached |   806              |   1.6ms     | 3.2ms    |

#### GET benchmark

| Server    | QPS(thousands qps) | latency 99% | 99.9%   |
|-----------|:------------------:|:-----------:|:-------:|
| Dragonfly | 🟩 3717            |   1ms       | 2.4ms   |
| Memcached |   2100             |  🟩 0.34ms  | 🟩 0.6ms |


Memcached exhibited lower latency for the read benchmark, but also lower throughput.

### Memory efficiency

In the following test, we filled Dragonfly and Redis with ~5GB of data
using `debug populate 5000000 key 1024` command. Then we started sending the update traffic with `memtier` and kicked off the snapshotting with the
"bgsave" command. The following figure demonstrates clearly how both servers behave in terms of memory efficiency.

<img src="http://static.dragonflydb.io/repo-assets/bgsave-memusage.svg" width="70%" border="0"/>

Dragonfly was 30% more memory efficient than Redis in the idle state.
It also did not show any visible memory increase during the snapshot phase.
Meanwhile, Redis reached almost x3 memory increase at peak compared to Dragonfly.
Dragonfly also finished the snapshot much faster, just a few seconds after it started.
For more info about memory efficiency in Dragonfly see [dashtable doc](/docs/dashtable.md)

## Running the server

Dragonfly runs on Linux. We advise running it on Linux version 5.11 or later
but you can also run Dragonfly on older kernels as well.


### With docker:

```bash
docker run --network=host --ulimit memlock=-1 docker.dragonflydb.io/dragonflydb/dragonfly

redis-cli PING  # redis-cli can be installed with "apt install -y redis-tools"
```

*You need `--ulimit memlock=-1` because some Linux distros configure the default memlock limit for containers as 64m and Dragonfly requires more.*

### Releases
We maintain [binary releases](https://github.com/dragonflydb/dragonfly/releases) for x86 and arm64 architectures. You will need to install `libunwind8` lib to run the binaries.


### Building from source

See [building from source](./docs/build-from-source.md) for details.

## Configuration
Dragonfly supports common Redis arguments where applicable.
For example, you can run: `dragonfly --requirepass=foo --bind localhost`.

Dragonfly currently supports the following Redis-specific arguments:
 * `port` redis connection port, default: 6379
 * `bind` localhost to only allow locahost connections, Public IP ADDRESS , to allow connections **to that ip** address (aka from outside too)
 * `requirepass` password for AUTH authentication, default: ""
 * `maxmemory` Limit on maximum-memory (in human-readble bytes) that is used by the database. 0 - means the program will automatically determine its maximum memory usage. default: 0
 * `dir` - by default, dragonfly docker uses `/data` folder for snapshotting. the CLI uses: ""
    You can use `-v` docker option to map it to your host folder.
 * `dbfilename` the filename to save/load the DB. default: "dump";

In addition, it has Dragonfly specific arguments options:
 * `memcache_port`  - to enable memcached compatible API on this port. Disabled by default.
 * `keys_output_limit` - maximum number of returned keys in `keys` command. Default is 8192.
   `keys` is a dangerous command. We truncate its result to avoid blowup in memory when fetching too many keys.
 * `dbnum` - maximum number of supported databases for `select`.
 * `cache_mode` - see [Cache](#novel-cache-design) section below.
 * `hz` - key expiry evaluation frequency. Default is 100. Lower frequency uses less cpu when
   idle at the expense of slower eviction rate.
 * `save_schedule` - glob spec for the UTC time to save a snapshot which matches HH:MM (24h time). default: ""

### Example Start Script, with popular options:

```bash
./dragonfly-x86_64 --logtostderr --requirepass=youshallnotpass --cache_mode=true -dbnum 1 --bind localhost --port 6379  --save_schedule "*:30" --maxmemory=12gb --keys_output_limit=12288 --dbfilename dump.rdb
```

for more options like logs management or tls support, run `dragonfly --help`.

## Roadmap and status

Currently, Dragonfly supports ~185 Redis commands and all memcache commands besides `cas`.
We are almost on par with Redis 5 API. Our next milestone will be to stabilize basic
functionality and implement the replication API.
If you see that a command you need, is not implemented yet, please open an issue.

For dragonfly-native replication, we are designing a distributed log format that will support order of magnitude higher speeds.

After the replication feature we will continue with other Redis missing commands from
APIs 3-6.

Please see [API readiness doc](docs/api_status.md) for the current status of Dragonfly.

## Design decisions

### Novel cache design
Dragonfly has a single unified adaptive caching algorithm that is very simple and memory efficient.
You can enable caching mode by passing `--cache_mode=true` flag. Once this mode
is on, Dragonfly will evict items least likely to be stumbled upon in the future but only when
it is near maxmemory limit.

### Expiration deadlines with relative accuracy
Expiration ranges are limited to ~4 years. Moreover, expiration deadlines
with millisecond precision (PEXPIRE/PSETEX etc) will be rounded to closest second
**for deadlines greater than 134217727ms (approximately 37 hours)**.
Such rounding has less than 0.001% error which I hope is acceptable for large ranges.
If it breaks your use-cases - talk to me or open an issue and explain your case.

For more detailed differences between this and Redis implementations [see here](docs/differences.md).

### Native Http console and Prometheus compatible metrics
By default, Dragonfly allows http access via its main TCP port (6379). That's right, you
can connect to Dragonfly via Redis protocol and via HTTP protocol - the server recognizes
the protocol  automatically during the connection initiation. Go ahead and try it with your browser.
Right now it does not have much info but in the future, we are planning to add there useful
debugging and management info. If you go to `:6379/metrics` url you will see some prometheus
compatible metrics.

The Prometheus exported metrics are compatible with the Grafana dashboard [see here](tools/local/monitoring/grafana/provisioning/dashboards/dashboard.json).


Important! The http console is meant to be accessed within a safe network.
If you expose Dragonfly's TCP port externally, it is advised to disable the console
with `--http_admin_console=false` or `--nohttp_admin_console`.


## Background

Dragonfly started as an experiment to see how an in-memory datastore could look like if it was designed in 2022. Based on  lessons learned from our experience as users of memory stores and as engineers who worked for cloud companies, we knew that we need to preserve two key properties for Dragonfly: a) to provide atomicity guarantees for all its operations, and b) to guarantee low, sub-millisecond latency over very high throughput.

Our first challenge was how to fully utilize CPU, memory, and i/o resources using servers that are available today in public clouds. To solve this, we used [shared-nothing architecture](https://en.wikipedia.org/wiki/Shared-nothing_architecture), which allows us to partition the keyspace of the memory store between threads so that each thread would manage its own slice of dictionary data. We call these slices - shards. The library that powers thread and I/O management for shared-nothing architecture is open-sourced [here](https://github.com/romange/helio).

To provide atomicity guarantees for multi-key operations, we used the advancements from recent academic research. We chose the paper ["VLL: a lock manager redesign for main memory database systems”](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) to develop the transactional framework for Dragonfly. The choice of shared-nothing architecture and VLL allowed us to compose atomic multi-key operations without using mutexes or spinlocks. This was a major milestone for our PoC and its performance stood out from other commercial and open-source solutions.

Our second challenge was to engineer more efficient data structures for the new store. To achieve this goal, we based our core hashtable structure on paper ["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf). The paper itself is centered around the persistent memory domain and is not directly related to main-memory stores.
Nevertheless, it's very much applicable to our problem. It suggested a hashtable design that allowed us to maintain two special properties that are present in the Redis dictionary: a) its incremental hashing ability during datastore growth b) its ability to traverse the dictionary under changes using a stateless scan operation. Besides these 2 properties,
Dash is much more efficient in CPU and memory. By leveraging Dash's design, we were able to innovate further with the following features:
 * Efficient record expiry for TTL records.
 * A novel cache eviction algorithm that achieves higher hit rates than other caching strategies like LRU and LFU with **zero memory overhead**.
 * A novel **fork-less** snapshotting algorithm.

After we built the foundation for Dragonfly and [we were happy with its performance](#benchmarks),
we went on to implement the Redis and Memcached functionality. By now, we have implemented ~185 Redis commands (roughly equivalent to Redis 5.0 API) and 13 Memcached commands.

And finally, <br>
<em>Our mission is to build a well-designed, ultra-fast, cost-efficient in-memory datastore for cloud workloads that takes advantage of the latest hardware advancements. We intend to address the pain points of current solutions while preserving their product APIs and propositions.
</em>
