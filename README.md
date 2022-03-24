# Dragonfly

[![ci-tests](https://github.com/romange/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/romange/dragonfly/actions/workflows/ci.yml)

A novel memory store that supports Redis and Memcached commands.
For more detailed status of what's implemented - see below.

Features include:
1. High throughput reaching millions of QPS on a single node.
2. TLS support.
3. Pipelining mode.
4. A novel cache design, which does not require specifying eviction policies.
5. Memory efficiency that can save 20-40% for regular workloads and even more for cache like
   workloads

## Building from source
I've tested the build on Ubuntu 21.04+.
Requires: CMake, Ninja, boost, libunwind8-dev

```
sudo apt install ninja-build
sudo apt install libunwind-dev
sudo apt-get install libboost-all-dev
```

```
git clone --recursive https://github.com/romange/dragonfly
cd dragonfly && ./helio/blaze.sh -release
cd build-opt && ninja dragonfly

```

## Running

```
./dragonfly --logtostderr
```

for more options, run `./dragonfly --help`

## Milestone Egg 🥚

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
  - [ ] MSETNX
  - [ ] SUBSTR
- [ ] Generic family
  - [X] DEL
  - [X] ECHO
  - [X] EXISTS
  - [X] EXPIRE
  - [X] EXPIREAT
  - [X] PING
  - [X] RENAME
  - [X] RENAMENX
  - [X] SELECT
  - [X] TTL
  - [X] TYPE
  - [ ] SORT
- [X] Server Family
  - [X] QUIT
  - [X] DBSIZE
  - [ ] BGSAVE
  - [X] SAVE
  - [X] DBSIZE
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
  - [ ] RPOPLPUSH
  - [X] RPUSH
- [X] SortedSet Family
  - [X] ZADD
  - [X] ZCARD
  - [ ] ZINCRBY
  - [X] ZRANGE
  - [X] ZRANGEBYSCORE
  - [X] ZREM
  - [X] ZREMRANGEBYSCORE
  - [X] ZREVRANGE
  - [X] ZSCORE
- [ ] Not sure whether these are required for the initial release.
  - [X] AUTH
  - [ ] BGREWRITEAOF
  - [ ] KEYS
  - [ ] MONITOR
  - [ ] RANDOMKEY
  - [ ] MOVE

In addition, we want to support efficient expiry (TTL) and cache eviction algorithms.
We should implement basic memory management support. For Master/Slave replication we should design
a distributed log format.

### Memchache API
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

API 2.0
- [ ] List Family
  - [X] BLPOP
  - [ ] BRPOP
  - [ ] BRPOPLPUSH
  - [ ] BLMOVE
  - [ ] LINSERT
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
  - [ ] INCRBYFLOAT
  - [ ] PSETEX
  - [ ] SETBIT
  - [ ] SETRANGE
  - [X] STRLEN
- [X] HashSet Family
  - [X] HSET
  - [X] HMSET
  - [X] HDEL
  - [X] HEXISTS
  - [X] HGET
  - [X] HMGET
  - [X] HLEN
  - [ ] HINCRBY
  - [ ] HINCRBYFLOAT
  - [X] HGETALL
  - [X] HKEYS
  - [ ] HSETNX
  - [ ] HVALS
  - [ ] HSCAN
- [ ] PubSub family
  - [ ] PUBLISH
  - [ ] PUBSUB
  - [ ] PUBSUB CHANNELS
  - [ ] SUBSCRIBE
  - [ ] UNSUBSCRIBE
- [ ] Server Family
  - [ ] WATCH
  - [ ] UNWATCH
  - [ ] DISCARD
  - [ ] CLIENT KILL/LIST/UNPAUSE/PAUSE/GETNAME/SETNAME/REPLY/TRACKINGINFO
  - [X] COMMAND
  - [ ] COMMAND COUNT/GETKEYS/INFO
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
  - [X] SCRIPT LOAD
  - [ ] SCRIPT DEBUG/KILL/FLUSH/EXISTS
  - [ ] TOUCH
- [X] Set Family
  - [X] SSCAN
  - [X] SMISMEMBER
- [X] Sorted Set Family
  - [ ] ZCOUNT
  - [ ] ZINTERSTORE
  - [ ] ZLEXCOUNT
  - [ ] ZRANGEBYLEX
  - [ ] ZRANK
  - [ ] ZREMRANGEBYLEX
  - [X] ZREMRANGEBYRANK
  - [ ] ZREVRANGEBYSCORE
  - [ ] ZREVRANK
  - [ ] ZUNIONSTORE
  - [ ] ZSCAN
- [ ] HYPERLOGLOG Family
  - [ ] PFADD
  - [ ] PFCOUNT
  - [ ] PFMERGE
- [ ] PUBSUB Family
  - [ ] PSUBSCRIBE
  - [ ] PUBSUB
  - [ ] PUBLISH
  - [ ] PUNSUBSCRIBE
  - [ ] SUBSCRIBE
  - [ ] UNSUBSCRIBE


Commands that I prefer avoid implementing before launch:
  - PUNSUBSCRIBE
  - PSUBSCRIBE
  - HYPERLOGLOG
  - SCRIPT DEBUG
  - OBJECT
  - DUMP/RESTORE
  - CLIENT

Also, I would omit keyspace notifications. For that I would like to deep dive and learn
exact use-cases for this API.

### Random commands we implemented along the way

 - [X] ROLE (2.8) decorator for for master withour replicas
 - [X] UNLINK (4.0) decorator for DEL command
 - [X] BGSAVE
## Milestone Nymph
API 2,3,4 without cluster support, without modules, without memory inspection commands.
Without support for keyspace notifications.

Design config support. ~140 commands overall...
## Milestone Molt
API 5,6 - without cluster and modules. Streams support. ~80 commands overall.
## Milestone Adult
TBD.


## Design decisions along the way
### Expiration deadlines with relative accuracy
I decided to limit the expiration range to 365 days. Moreover, expiration deadlines
with millisecond precision (PEXPIRE/PSETEX etc) will be rounded to closest second
**for deadlines greater than 33554431ms (approximately 560 minutes). In other words,
expiries of `PEXPIRE key 10010` will expire exactly after 10 seconds and 10ms. However,
`PEXPIRE key 34000300` will expire after 34000 seconds (i.e. 300ms earlier). Similarly,
`PEXPIRE key 34000800` will expire after 34001 seconds, i.e. 200ms later.

Such rounding has at most 0.002% error which I hope is acceptable for large ranges.
If it breaks your use-cases - talk to me or open an issue and explain your case.
