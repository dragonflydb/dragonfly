<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>


# Dragonfly

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml)

A novel, Redis and Memcached compatible memory store.
For more detailed picture of what's implemented - see [Roadmap](#roadmap-and-status).

## Background

Dragonfly started as an experiment to check how in-memory store could look like if it was
designed in 2022. We set as a goal to design a reliable, cost-efficent memory store that uses
cloud economy to its advantage.

Our initial focus was on how to achieve full parallelism on multi-core cloud instances but still
preserve atomicity guarantees for complex multi-key Redis commands or lua scripts. We wanted
to preserve the original philosophy of Redis and preserve its low latency characteristics,
especially its tail latency. Therefore, unlike with other attempts to make Redis multi-threaded,
we wanted to avoid using spinlocks or mutexes for threads coordination.
We succeed to solved this problem. As a result, a single Dragonfly instance can reach
as high as 15M QPS 🚀 and all that with sub-millisecond latency.

Our second important contribution is that we built a completely different dictionary
data-structure for our memory store (called DashTable) that has much less memory overhead than of
Redis-dictionary or memcached. By leveraging DashTable's unique design we added other enhanments:
  * Efficient record expiry for TTL records 🕛.
  * Novel cache eviction algorithm. The latter, by the way, has academic novelty - it achieves
     higher hit rate for the same memory reservation than other caching strategies like LRU and LFU.
  * A novel fork-less snapshotting algorithm.

TODO: to explain about each one of these.

After we achieved a breakthrough in those two areas, we went on to implement Redis and
Memcached functionality, so we could be sure, we can achieve feature parity with most
popular systems that are used today.

Currently we support ~130 Redis commands and 13 memcached commands.

Our work is based on academic research from the last decade. Specifically, our transactional
framework is built upon concepts
from [VLL: a lock manager redesign for main memory databasesystems](http://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf).

Our DashTable design is based on paper [Dash: Scalable Hashing on Persistent Memory](https://arxiv.org/abs/2003.07302).
Our caching scheme is conceptually based on [2Q algorithm from 1994](http://www.vldb.org/conf/1994/P439.PDF) but we leveraged
useful properties from DashTable and come-up with what we think is a novel approach to efficient caching with high hit-ratio.

<em>
Before starting Dragonfly, the authors of this project has been using Redis in production for several
years. They learned along the way of many of Redis strengths and weaknessses.
Afterwards, one of them was lucky to join a team that provides managed services
for Redis and Memcached. There he learned that many pains he observed as a user are
shared by other Redis users as well.
</em>

## Why Dragonfly

1. Unprecedented performance. We reach 15M GET or 10M SET qps on a single machine in pipeline mode
   or 3.3M qps without pipeline mode. Yes, x20 higher throughput with sub-millisecond latency
   using multi-threaded architecture.
2. Memory efficiency that will save you dozens percents of hardware costs. For some cases we observed
   x3 costs reduction just because Dragonfly allowed our users to switch from cluster-mode with dozens
   machines to a big instance with memory-cpu ratio that fit their needs.
3. Reliability and simplicity. Dragonfly has very smooth and consistent behavior,without memory
   spikes or connection disconnects during heavy operations like flushdb, save etc.
4. Robust, memory efficient caching with high hit-rate.
5. Compatibility with Redis API and protocol. Similarly to Redis, all operations are performed
   atomically, yet we also provide complete responsiveness and asynchronisity. Yes, you can
   connect to Dragonfly and run commands while it runs lua scripts or cpu consuming operations.
6. Support for hundreds of thousands of connections.
7. Novel persistence algorithm provides two orders of magnitude faster snapshotting.
   In addition, due to its memory efficiency it can save up-to 50% of peak memory usage
   compared to Redis.
8. A built-in http console with prometheus metrics.


## Running the server

Dragonfly runs on linux. It uses relatively new linux specific [io-uring API](https://github.com/axboe/liburing)
for I/O, hence it requires Linux version 5.11 or later.
Ubuntu 20.04.4 or 22.04 fit these requirements.

If built locally, just run:

```bash

./dragonfly --alsologtostderr

```

or with docker:

```bash
docker pull ghcr.io/dragonflydb/dragonfly:latest && \
docker tag ghcr.io/dragonflydb/dragonfly:latest dragonfly

docker run --network=host --rm dragonfly
```

Some hosts may require adding `--ulimit memlock=-1` to `docker run` options.

We support redis run-time arguments where applicable.
For example, you can run: `docker run --network=host --rm dragonfly --requirepass=foo --bind localhost`.

dragonfly currently supports the following Redis arguments:
 * `port`
 * `bind`
 * `requirepass`
 * `maxmemory`
 * `dir` - by default, dragonfly docker uses `/data` folder for snapshotting.
    You can use `-v` docker option to map it to your host folder.
 * `dbfilename`

In addition, it has Dragonfly specific arguments options:
 * `memcache_port`  - to enable memcached compatible API on this port. Disabled by default.
 * `keys_output_limit` - maximum number of returned keys in `keys` command. Default is 8192.
   We truncate the output to avoid blowup in memory when fetching too many keys.
 * `dbnum` - maximum number of supported databases for `select`.
 * `cache_mode` - see [Cache](#novel-cache-design) section below.


for more options like logs management or tls support, run `dragonfly --help`.


## Building from source

Dragonfly is usually built on Ubuntu 20.04 or later.

```bash
git clone --recursive https://github.com/dragonflydb/dragonfly && cd dragonfly

# to install dependencies
sudo apt install ninja-build libunwind-dev libboost-fiber-dev libssl-dev \
     autoconf-archive libtool

# Configure the build
./helio/blaze.sh -release

cd build-opt && ninja dragonfly  # build

```

## Benchmarks
TODO.

## Roadmap and status

Currently Dragonfly supports ~130 Redis commands and all memcache commands besides `cas`.
We are almost on part with Redis 2.8 API. Our first milestone will be to stabilize basic
functionality and reach API parity with Redis 2.8 and Memcached APIs.
If you see that a command you need is not implemented yet, please open an issue.

The next milestone will be implementing HA with `redis -> dragonfly` and
`dragonfly<->dragonfly` replication.

For dragonfly-native replication we are planning to design a distributed log format that will
support order of magnitude higher speeds when replicating.

After replication and failover feature we will continue with other Redis commands from API 3,4,5
except for cluster mode functionality.


### Initial release

API 1.0
- [X] String family
  - [X] SET
  - [ ] SETNX
  - [X] GET
  - [X] DECR
  - [X] INCR
  - [X] DECRBY
  - [X] GETSET
  - [X] INCRBY
  - [X] MGET
  - [X] MSET
  - [X] MSETNX
  - [X] SUBSTR
- [ ] Generic family
  - [X] DEL
  - [X] ECHO
  - [X] EXISTS
  - [X] EXPIRE
  - [X] EXPIREAT
  - [X] KEYS
  - [X] PING
  - [X] RENAME
  - [X] RENAMENX
  - [X] SELECT
  - [X] TTL
  - [X] TYPE
  - [ ] SORT
- [X] Server Family
  - [X] AUTH
  - [X] QUIT
  - [X] DBSIZE
  - [ ] BGSAVE
  - [X] SAVE
  - [X] DEBUG
  - [X] EXEC
  - [X] FLUSHALL
  - [X] FLUSHDB
  - [X] INFO
  - [X] MULTI
  - [X] SHUTDOWN
  - [X] LASTSAVE
  - [X] SLAVEOF/REPLICAOF
  - [ ] SYNC
- [ ] Set Family
  - [x] SADD
  - [x] SCARD
  - [X] SDIFF
  - [X] SDIFFSTORE
  - [X] SINTER
  - [X] SINTERSTORE
  - [X] SISMEMBER
  - [X] SMOVE
  - [X] SPOP
  - [ ] SRANDMEMBER
  - [X] SREM
  - [X] SMEMBERS
  - [X] SUNION
  - [X] SUNIONSTORE
- [X] List Family
  - [X] LINDEX
  - [X] LLEN
  - [X] LPOP
  - [X] LPUSH
  - [X] LRANGE
  - [X] LREM
  - [X] LSET
  - [X] LTRIM
  - [X] RPOP
  - [X] RPOPLPUSH
  - [X] RPUSH
- [X] SortedSet Family
  - [X] ZADD
  - [X] ZCARD
  - [X] ZINCRBY
  - [X] ZRANGE
  - [X] ZRANGEBYSCORE
  - [X] ZREM
  - [X] ZREMRANGEBYSCORE
  - [X] ZREVRANGE
  - [X] ZSCORE
- [ ] Not sure whether these are required for the initial release.
  - [ ] BGREWRITEAOF
  - [ ] MONITOR
  - [ ] RANDOMKEY
  - [ ] MOVE

API 2.0
- [X] List Family
  - [X] BLPOP
  - [X] BRPOP
  - [ ] BRPOPLPUSH
  - [X] LINSERT
  - [X] LPUSHX
  - [X] RPUSHX
- [X] String Family
  - [X] SETEX
  - [X] APPEND
  - [X] PREPEND (dragonfly specific)
  - [ ] BITCOUNT
  - [ ] BITFIELD
  - [ ] BITOP
  - [ ] BITPOS
  - [ ] GETBIT
  - [X] GETRANGE
  - [X] INCRBYFLOAT
  - [X] PSETEX
  - [ ] SETBIT
  - [X] SETRANGE
  - [X] STRLEN
- [X] HashSet Family
  - [X] HSET
  - [X] HMSET
  - [X] HDEL
  - [X] HEXISTS
  - [X] HGET
  - [X] HMGET
  - [X] HLEN
  - [X] HINCRBY
  - [X] HINCRBYFLOAT
  - [X] HGETALL
  - [X] HKEYS
  - [X] HSETNX
  - [X] HVALS
  - [X] HSCAN
- [ ] PubSub family
  - [X] PUBLISH
  - [ ] PUBSUB
  - [ ] PUBSUB CHANNELS
  - [X] SUBSCRIBE
  - [X] UNSUBSCRIBE
  - [ ] PSUBSCRIBE
  - [ ] PUNSUBSCRIBE
- [ ] Server Family
  - [ ] WATCH
  - [ ] UNWATCH
  - [X] DISCARD
  - [X] CLIENT LIST/SETNAME
  - [ ] CLIENT KILL/UNPAUSE/PAUSE/GETNAME/REPLY/TRACKINGINFO
  - [X] COMMAND
  - [X] COMMAND COUNT
  - [ ] COMMAND GETKEYS/INFO
  - [ ] CONFIG GET/REWRITE/SET/RESETSTAT
  - [ ] MIGRATE
  - [ ] ROLE
  - [ ] SLOWLOG
  - [ ] PSYNC
  - [ ] TIME
  - [ ] LATENCY...
- [X] Generic Family
  - [X] SCAN
  - [X] PEXPIREAT
  - [ ] PEXPIRE
  - [ ] DUMP
  - [X] EVAL
  - [X] EVALSHA
  - [ ] OBJECT
  - [ ] PERSIST
  - [X] PTTL
  - [ ] RESTORE
  - [X] SCRIPT LOAD/EXISTS
  - [ ] SCRIPT DEBUG/KILL/FLUSH
- [X] Set Family
  - [X] SSCAN
- [X] Sorted Set Family
  - [X] ZCOUNT
  - [X] ZINTERSTORE
  - [X] ZLEXCOUNT
  - [X] ZRANGEBYLEX
  - [X] ZRANK
  - [X] ZREMRANGEBYLEX
  - [X] ZREMRANGEBYRANK
  - [X] ZREVRANGEBYSCORE
  - [X] ZREVRANK
  - [X] ZUNIONSTORE
  - [X] ZSCAN
- [ ] HYPERLOGLOG Family
  - [ ] PFADD
  - [ ] PFCOUNT
  - [ ] PFMERGE

Memchache API
- [X] set
- [X] get
- [X] replace
- [X] add
- [X] stats (partial)
- [x] append
- [x] prepend
- [x] delete
- [x] flush_all
- [x] incr
- [x] decr
- [x] version
- [x] quit

Random commands we implemented as decorators along the way:

 - [X] ROLE (2.8) decorator as master.
 - [X] UNLINK (4.0) decorator for DEL command
 - [X] BGSAVE (decorator for save)
 - [X] FUNCTION FLUSH (does nothing)

### Milestone - H/A
Implement leader/follower replication (PSYNC/REPLICAOF/...).

### Milestone - "Maturiry"
APIs 3,4,5 without cluster support, without modules and without memory introspection commands. Also
without geo commands and without support for keyspace notifications, without streams.
Probably design config support. Overall - few dozens commands...
Probably implement cluster-API decorators to allow cluster-configured clients to connect to a
single instance.

### Next milestones will be determined along the way.

## Design decisions

### Novel cache design
Dragonfly has a single unified adaptive caching algorithm that is very simple and memory efficient.
You can enable caching mode by passing `--cache_mode=true` flag. Once this mode
is on, Dragonfly will evict items least likely to be stumbled upon in the future but only when
it reaches maxmemory limit.

### Expiration deadlines with relative accuracy
Expiration ranges are limited to ~4 years. Moreover, expiration deadlines
with millisecond precision (PEXPIRE/PSETEX etc) will be rounded to closest second
**for deadlines greater than 134217727ms (approximately 37 hours)**.
Such rounding has less than 0.001% error which I hope is acceptable for large ranges.
If it breaks your use-cases - talk to me or open an issue and explain your case.

For more detailed differences between this and Redis implementations [see here](doc/differences.md).

### Native Http console and Prometheus compatible metrics
By default Dragonfly also allows http access on its main TCP port (6379). That's right, you
can use for Redis protocol and for HTTP protocol - type of protocol is determined automatically
during the connection initiation. Go ahead and try it with your browser.
Right now it does not have much info but in the future we are planning to add there useful
debugging and management info. If you go to `:6379/metrics` url you will see some prometheus
compatible metrics.

Important! Http console is meant to be accessed within a safe network.
If you expose Dragonfly's TCP port externally, it is advised to disable the console
with `--http_admin_console=false` or `--nohttp_admin_console`.


## FAQ

1. Did you really rewrote all from scratch?<br>
   <em>We reused Redis low-level data-structures like quicklist, listpack, zset etc. It's about 13K
   lines of code. We rewrote everything else including networking stack, server code,
   rdb serialization and many other components.</em>
2. Is your license open-source?<br>
   <em>We released the code under source-available license which is more permissive than
       AGPL-like licenses. Basically it says, the software is free to use and free to change
       as long as you do not provide paying support or managed services for this software.
       We followed the trend of other technological companies
       like Elastic, Redis, MongoDB, Cochroach labs, Redpanda Data to protect our rights
       to provide support for the software we built. If you want to learn more about why this
       trend started, and the fragile balance between open source, innovation and sustainability,
       I invite you to read [this article](https://techcrunch.com/2018/11/29/the-crusade-against-open-source-abuse/).
   </em>
3. It seems that Dragonfly provides vertical scale, but we can achieve similar
   throughput with X nodes with Redis cluster.<br>
  <em>Dragonfly utilizes the underlying hardware in an optimal way. Meaning it can run on small
  8GB instances and on large 768GB machines with 64 cores. This versatility allows to drastically
  reduce complexity of running small to medium clusters of 1-20 nodes and instead
  run the same workload on a single Dragonfly node. As a result it may save you hardware costs but even more importantly,
  it will vastly reduce the complexity (total cost of ownership) of handling the multi-node cluster.
  Also, Redis cluster-mode imposes some limitations on multi-key and transactinal operations.
  Dragonfly provides the same semantics as single node Redis. </em>
4. Are you against horizontal scale? <br>
  <em> No, we are not against horizontal scale :). We are against the common mis-conception that
  horizontal scale is a solution for everything. I think this trend started 20-25 years ago
  within Google that built their internal systems using commonly used hardware. Today, though,
  our cloud instances are everything but common. Quoting from Scylla blog:
  "Hardware on which modern workloads must run is remarkably different from the hardware on which
   current programming paradigms depend, and for which current software infrastructure is designed.”

   We will work on horizontally scalable solution for Dragonfly but it will be after we reach feature
   parity with Redis 5-6 and after we introduce other disrupting features we plan to release in Dragonfly.
   </em>

5. I use Redis and I do not need 3M qps. Why should I use Dragonfly? <br>
   <em>First of all, if you use Redis and you are happy with it - continue using it,
       it's a great product 🍻 and maybe you have not reached yet the scale where problems start.
       Having said that, even for low throughput case you may find Dragonfly
       working better. It may be that you are sufferring from random latency spikes, or maybe
       your ETL involving Redis takes hours to finish or maybe its memory usage and hardware costs
       give you a headache. With Dragonfly we tried to solve every design defficiency
       we expirienced ourselves in the past.</em>
