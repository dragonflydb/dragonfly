<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml)

A novel, Redis and Memcached compatible memory store.


## Benchmarks
TODO.

## Running the server

Dragonfly runs on linux. It uses relatively new linux specific [io-uring API](https://github.com/axboe/liburing)
for I/O, hence it requires Linux version 5.11 or later.
Ubuntu 20.04.4 or 22.04 fit these requirements.


### With docker:

```bash
docker pull ghcr.io/dragonflydb/dragonfly:latest && \
docker tag ghcr.io/dragonflydb/dragonfly:latest dragonfly

docker run --network=host --rm dragonfly
```

Some hosts may require adding `--ulimit memlock=-1` to `docker run` options.

### Building from source

Dragonfly is usually built on Ubuntu 20.04 or later.

```bash
git clone --recursive https://github.com/dragonflydb/dragonfly && cd dragonfly

# to install dependencies
sudo apt install ninja-build libunwind-dev libboost-fiber-dev libssl-dev \
     autoconf-archive libtool

# Configure the build
./helio/blaze.sh -release

# Build
cd build-opt && ninja dragonfly  

# Run
./dragonfly --alsologtostderr

```


## Configuration
Dragonfly supports redis run-time arguments where applicable.
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
   `keys` is a dangerous command. we truncate its result to avoid blowup in memory when fetching too many keys.
 * `dbnum` - maximum number of supported databases for `select`.
 * `cache_mode` - see [Cache](#novel-cache-design) section below.


for more options like logs management or tls support, run `dragonfly --help`.



## Roadmap and status

Currently Dragonfly supports ~130 Redis commands and all memcache commands besides `cas`.
We are almost on part with Redis 2.8 API. Our first milestone will be to stabilize basic
functionality and reach API parity with Redis 2.8 and Memcached APIs.
If you see that a command you need, is not implemented yet, please open an issue.

The next milestone will be implementing H/A with `redis -> dragonfly` and
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

### Milestone - "Maturity"
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
it is near maxmemory limit.

### Expiration deadlines with relative accuracy
Expiration ranges are limited to ~4 years. Moreover, expiration deadlines
with millisecond precision (PEXPIRE/PSETEX etc) will be rounded to closest second
**for deadlines greater than 134217727ms (approximately 37 hours)**.
Such rounding has less than 0.001% error which I hope is acceptable for large ranges.
If it breaks your use-cases - talk to me or open an issue and explain your case.

For more detailed differences between this and Redis implementations [see here](doc/differences.md).

### Native Http console and Prometheus compatible metrics
By default Dragonfly allows http access via its main TCP port (6379). That's right, you
can connect to Dragonfly via Redis protocol and via HTTP protocol - the server recognizes
the protocol  automatically during the connection initiation. Go ahead and try it with your browser.
Right now it does not have much info but in the future we are planning to add there useful
debugging and management info. If you go to `:6379/metrics` url you will see some prometheus
compatible metrics.

Important! Http console is meant to be accessed within a safe network.
If you expose Dragonfly's TCP port externally, it is advised to disable the console
with `--http_admin_console=false` or `--nohttp_admin_console`.


## Background

Dragonfly started as an experiment to see how an in-memory datastore could look like if it was designed in 2022. Based on  lessons learned from our experience as users of memory stores and as engineers who worked for cloud companies, we knew that we need to preserve two key properties for Dragonfly: a) to provide atomicity guarantees for all its operations, and b) to guarantee low, sub-millisecond latency over very high throughput.

Our first challenge was how to fully utilize CPU, memory, and i/o resources using servers that are available today in public clouds. To solve this, we used [shared-nothing architecture](https://en.wikipedia.org/wiki/Shared-nothing_architecture), which allows us to partition the keyspace of the memory store between threads, so that each thread would manage its own slice of dictionary data. We call these slices - shards. The library that powers thread and I/O management for shared-nothing architecture is open-sourced [here](https://github.com/romange/helio).

To provide atomicity guarantees for multi-key operations, we used the advancements from recent academic research. We chose the paper ["VLL: a lock manager redesign for main memory database systems”](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) to develop the transactional framework for Dragonfly. The choice of shared-nothing architecture and VLL allowed us to compose atomic multi-key operations without using mutexes or spinlocks. This was a major milestone for our PoC and its performance stood out from other commercial and open-source solutions.

Our second challenge was to engineer more efficient data structures for the new store. To achieve this goal, we based our core hashtable structure on paper ["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf). The paper itself is centered around persistent memory domain and is not directly related to main-memory stores.
Nevertheless, its very much applicable for our problem. It suggested a hashtable design that allowed us to maintain two special properties that are present in the Redis dictionary: a) its incremental hashing ability during datastore growth b) its ability to traverse the dictionary under changes using a stateless scan operation. Besides these 2 properties,
Dash is much more efficient in CPU and memory. By leveraging Dash's design, we were able to innovate further with the following features:
 * Efficient record expiry for TTL records.
 * A novel cache eviction algorithm that achieves higher hit rates than other caching strategies like LRU and LFU with **zero memory overhead**.
 * A novel **fork-less** snapshotting algorithm.

After we built the foundation for Dragonfly and [we were happy with its performance](#benchmarks),
we went on to implement the Redis and Memcached functionality. By now, we have implemented ~130 Redis commands (equivalent to v2.8) and 13 Memcached commands.

And finally, <br>
<em>Our mission is to build a well-designed, ultra-fast, cost-efficient in-memory datastore for cloud workloads that takes advantage of the latest hardware advancements. We intend to address the pain points of current solutions while preserving their product APIs and propositions.
</em>

P.S. other engineers share a similar sentiment about what makes a good memory store. See, for example, [here](https://twitter.github.io/pelikan/2019/why-pelikan.html) and [here](https://twitter.github.io/pelikan/2021/segcache.html) blog posts from Twitter's memcache team, or [this post](https://medium.com/@john_63123/redis-should-be-multi-threaded-e28319cab744) from authors of keydb.
