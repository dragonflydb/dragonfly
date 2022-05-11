# Dragonfly

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml)

A novel memory store that supports Redis and Memcached commands.
For more detailed status of what's implemented - see below.

Features include:
1. High throughput reaching millions of QPS on a single node.
2. TLS support.
3. Pipelining mode.
4. A novel cache design, which does not require specifying eviction policies.
5. Memory efficiency that can save 20-40% for regular workloads and even more for cache like
   workloads

## Running
dragonfly requires Linux OS version 5.11 or later.
Ubuntu 20.04.4 or 22.04 fit these requirements.

If built locally just run:

```bash

./dragonfly --logtostderr

```

or with docker:

```bash
docker pull ghcr.io/dragonflydb/dragonfly:latest && \
docker tag ghcr.io/dragonflydb/dragonfly:latest dragonfly

docker run --network=host --rm dragonfly
```

Some systems may require adding `--ulimit memlock=-1` to `docker run` options.

We support redis command arguments where applicable.
For example, you can run: `docker run --network=host --rm dragonfly --requirepass=foo --bind localhost`.

dragonfly currently supports the following commandline options:
 * `port`
 * `bind`
 * `requirepass`
 * `maxmemory`
 * `memcache_port`  - to enable memcached compatible API on this port. Disabled by default.
 * `dir` - by default, dragonfly docker uses `/data` folder for snapshotting. You can use `-v` docker option to map it to your host folder.
 * `dbfilename`
 * `dbnum` - maximum number of supported databases for `select`.
 * `keys_output_limit` - maximum number of returned keys in `keys` command. Default is 8192.
   We truncate the output to avoid blowup in memory when fetching too many keys.

for more options like logs management or tls support, run `dragonfly --help`.


## Building from source
I've tested the build on Ubuntu 20.04+.
Requires: CMake, Ninja, boost, libunwind8-dev

```bash
# to install dependencies
sudo apt install ninja-build libunwind-dev libboost-fiber-dev libssl-dev

git clone --recursive https://github.com/dragonflydb/dragonfly && cd dragonfly

# another way to install dependencies
./helio/install-dependencies.sh

# Configure the build
./helio/blaze.sh -release

cd build-opt && ninja dragonfly  # build
```

## Roadmap and milestones

We are planning to implement most of the APIs 1.x and 2.8 (except the replication) before we release the project to source availability on github. In addition, we will support efficient expiry (TTL) and cache eviction algorithms.

The next milestone afterwards will be implementing `redis -> dragonfly` and
`dragonfly<->dragonfly` replication.

For dragonfly-native replication we are planning to design a distributed log format that will support order of magnitude higher speeds when replicating.

Commands that I wish to implement after releasing the initial code:
  - PUNSUBSCRIBE
  - PSUBSCRIBE
  - HYPERLOGLOG
  - SCRIPT DEBUG
  - OBJECT
  - DUMP/RESTORE
  - CLIENT

Their priority will be determined based on the requests from the community.
Also, I will omit keyspace notifications. For that I would like to deep dive and learn
exact the exact needs for this API.

### Milestone - "Source Available"

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

 - [X] ROLE (2.8) decorator for for master without replicas
 - [X] UNLINK (4.0) decorator for DEL command
 - [X] BGSAVE (decorator for save)
 - [X] FUNCTION FLUSH (does nothing)

## Milestone "Stability"
APIs 3,4,5 without cluster support, without modules, without memory introspection commands.
Without geo commands and without support for keyspace notifications, without streams.
Design config support. ~10-20 commands overall...
Probably implement cluster-API decorators to allow cluster-configured clients to connect to a single
instance.

 - [X] HSTRLEN

## Design decisions along the way

### Expiration deadlines with relative accuracy
Expiration ranges are limited to ~4 years. Moreover, expiration deadlines
with millisecond precision (PEXPIRE/PSETEX etc) will be rounded to closest second
**for deadlines greater than 134217727ms (approximately 37 hours)**.
Such rounding has less than 0.001% error which I hope is acceptable for large ranges.
If it breaks your use-cases - talk to me or open an issue and explain your case.

For more detailed differences between this and Redis implementations [see here](doc/differences.md).