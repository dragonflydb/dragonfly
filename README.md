<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>


# Dragonfly

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml)

Dragonfly is a multi-core blazing fast memory store engine fully compatible with Redis and Memcached APIs.

## Background

Dragonfly started as an experiment to check how in-memory store could look like if it was designed in 2022 based on state of the art academic research .  Our mission is to build a well-designed, ultra-fast, and cost-effective in-memory engine for cloud workloads that takes advantage of the latest hardware advancements. We want to address the pain-points of current solutions while preserving their product APIs and propositions.
 
Our first focus was to achieve **full parallelism** on multi-core cloud instances while preserving low tail latency and atomicity guarantees for complex multi-key operations and transactional commands. For that we use shared nothing architecture and base our transactional framework on paper [VLL: a lock manager redesign for main memory databasesystems](http://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf). This allows to achieve strongly consistent guarantees while avoiding spinlocks or mutexes for thread coordination. As a result, a single Dragonfly instance can reach 15M qps🚀 with sub-millisecond latency. 

Our second target was to optimize memory management. We based the core dictionary structure on the [Dash: Scalable Hashing on Persistent Memory](https://arxiv.org/abs/2003.07302) paper. Dragonfly's caching is conceptually based on [2Q algorithm from 1994](http://www.vldb.org/conf/1994/P439.PDF). By leveraging Dashtable's unique design we were able to minimize the data structure memory overhead while implementing:
 * Efficient record expiry for TTL records 🕛.
 * An academic novelty cache eviction algorithm that achieves higher hit rates than other caching strategies like LRU and LFU without overhead memory consumption.
 * A novel fork-less snapshotting algorithm.

After we scored those two breakthroughs, we went on to implement the Redis and Memcached functionality. By now we implemented ~130 Redis and 13 Memcached commands and have the ability to add new commands in a few days.

## Personal
<em>
@roman to write
</em>

## Why Dragonfly?

1. Simple - A single container on a single instance can power most usecases. 
   No need for clusters in most cases. No need for read replicas. 
2. Throughput - on a c6gn.16xlarge instance 3.3M qps without pipeline mode. With pipline - 15M GET or 10M SET qps.
   That is x20 higher throughput with sub-millisecond latency using multi-threaded architecture.
3. Cost efficiency - Dragonfly is memory optimized and can scale on many hardware configurations. 
   We observed x3 costs reduction just because Dragonfly allowes developers to switch from cluster-mode with dozens
   machines to a big instance with memory-cpu ratio that fit their needs.
4. Reliability - Dragonfly has very smooth and consistent behavior, without memory
   spikes or connection disconnects during heavy operations like flushdb, save etc.
5. Robust, memory efficient caching with high hit-rate.
6. Compatibility with Redis API and protocol. Similarly to Redis, all operations are performed
   atomically, yet Dragonfly also provide complete responsiveness and asynchronisity. You can
   connect to Dragonfly and run commands while it runs lua scripts or cpu consuming operations.
7. Support for hundreds of thousands of connections.
8. Novel persistence algorithm provides two orders of magnitude faster snapshotting.
   In addition, due to its memory efficiency it can save up-to 50% of peak memory usage
   compared to Redis.
9. A built-in http console with prometheus metrics.


## Running the server

Dragonfly runs on linux. It uses relatively new linux specific [io-uring API](https://github.com/axboe/liburing)
for I/O, hence it requires Linux version 5.11 or later.
Ubuntu 20.04.4 or 22.04 fit these requirements.

When built locally, just run:

```bash

./dragonfly --alsologtostderr

```

or use docker:

```bash
docker pull ghcr.io/dragonflydb/dragonfly:latest && \
docker tag ghcr.io/dragonflydb/dragonfly:latest dragonfly

docker run --network=host --rm dragonfly
```

Some hosts may require adding `--ulimit memlock=-1` to the `docker run` options.

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
 * `memcache_port` - to enable memcached compatible API on this port. Disabled by default.
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
We are almost on par with Redis 2.8 APIs. Our first milestone will be to stabilize basic
functionality and reach API parity with Redis 2.8 and Memcached APIs.
If you see that a command you need is not implemented yet, please open an issue or send a pull request.

The next milestone we will be implementing HA with `redis -> dragonfly` and
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

### Next milestones will be determined along the way.

## Design decisions

### Novel cache design
Dragonfly has a single unified adaptive caching algorithm that is very simple and memory efficient.
You can enable caching mode by passing `--cache_mode=true` flag. Once this mode
is on, Dragonfly will evict items least likely to be stumbled upon in the future but only when
it reaches the maxmemory limit.

### Expiration deadlines with relative accuracy
Expiration ranges are limited to ~4 years. Expiration has
millisecond precision (PEXPIRE/PSETEX etc). For **deadlines greater than 134217727ms (approximately 37 hours)** expiration is rounded to closest second. Such rounding has less than 0.001% error which we assume is acceptable for long expiration deadlines. If it breaks your use-cases please open an issue with your unique case.

For more detailed differences between the Dragonfly and Redis implementations [see here](doc/differences.md).

### Native Http console and Prometheus compatible metrics
By default Dragonfly also allows http access on its main TCP port (6379). The type of the connection is determined automatically during the connection initiation. Go ahead and try it with your browser.
Right now it has only basic info but more will be added int he future. If you go to `:6379/metrics` url you will see some prometheus compatible metrics.

Important! Http console is meant to be accessed within a safe network.
If you expose Dragonfly's TCP port externally, it is advised to disable the http console
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
       we invite you to read [this article](https://techcrunch.com/2018/11/29/the-crusade-against-open-source-abuse/).
   </em>
3. It seems that Dragonfly provides vertical scale, but we can achieve similar
   throughput with X nodes in a Redis cluster.<br>
  <em>Dragonfly utilizes the underlying hardware in an optimal way. Meaning it can run on small
  8GB instances and scale verticly to large 768GB machines with 64 cores. This versatility allows to drastically
  reduce complexity of running cluster workloads to a single node saving hardware resources and costs. More importantly,
  it reduces the complexity (total cost of ownership) of handling the multi-node cluster.
  In addition, Redis cluster-mode imposes some limitations on multi-key and transactinal operations.
  Dragonfly provides the same semantics as single node Redis. </em>
4. Are you against horizontal scale? <br>
  <em> No, we are not against horizontal scale :). Horizontal scale as the first solution for in-memory datastores is a necessity of the limitations of old architectures. For example, in many cases it makes little sense to create read replicas (X2-5 in memory and costs) instead of adding CPU resources to the original instance (Saving memory and complexity). 

  Horizontal scale as a solution for everything is no longer valid. This trend started 20-25 years ago
  within Google that built their internal systems using commonly used hardware. Today, though,
  cloud instances are everything but common. Quoting from Scylla blog:
  "Hardware on which modern workloads must run is remarkably different from the hardware on which 
  current programming paradigms depend, and for which current software infrastructure is designed.” 

  In many of the cases Dragonfly architecture can save us from the complexities, resource waste and associated costs of horizontal scaling. However, instances have their physical limitations, in those cases horizontal scaling is a must and should be on our future roadmap.
   </em>

5. I use Redis and I do not need 3M qps. Why should I use Dragonfly? <br>
   <em>First of all, if you use Redis and you are happy with it - continue using it,
       it's a great product 🍻 and maybe you have not reached the scale where problems start.
       Having said that, even for low throughput case you may find Dragonfly
       beneficial. It may be that you are sufferring from random latency spikes, or maybe
       your ETL involving Redis takes hours to finish or maybe its memory usage and hardware costs
       give you a headache. With Dragonfly we tried to solve every design defficiency
       we expirienced ourselves in the past.</em>

6. I get it, but why not change Redis open source? <br>
   <em>Many of the pain points we addressed like Multi-core support, memory optimization, forkless snapshot, improved eviction algo, unit testing and more... have been the achilles heel and a long standing requests by the community. Some were not addressed for as long as ten years, others were dismissed because they require a complete rewrite of the engine. With Redis open source long legacy codebase it is impossible to innovate at this scale.</em>  

7. Who is behind Dragonfly? <br>
  <em>We! and hofully you. We decided to make the Dragonfly code avilable so the community can enjoy it and contribute to grow it for the collective benefits of all. There are many ways to contribute. Here are some: Star, test, blog, fork, open issues, address issues, suggest features, contibute to code, implement a command, tweet... </em>


