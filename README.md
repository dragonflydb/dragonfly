# Dragonfly

[![ci-tests](https://github.com/romange/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/romange/dragonfly/actions/workflows/ci.yml)

A toy memory store that supports basic commands like `SET` and `GET` for both memcached and redis protocols. In addition, it supports redis `PING` command.

Demo features include:
1. High throughput reaching millions of QPS on a single node.
2. TLS support.
3. Pipelining mode.

## Building from source
I've tested the build on Ubuntu 21.04+.


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
  - [ ] SADD
  - [ ] SCARD
  - [ ] SDIFF
  - [ ] SDIFFSTORE
  - [ ] SINTER
  - [ ] SINTERSTORE
  - [ ] SISMEMBER
  - [ ] SMOVE
  - [ ] SPOP
  - [ ] SRANDMEMBER
  - [ ] SREM
  - [ ] SMEMBERS
  - [ ] SUNION
  - [ ] SUNIONSTORE
- [X] List Family
  - [X] LINDEX
  - [X] LLEN
  - [X] LPOP
  - [X] LPUSH
  - [ ] LRANGE
  - [ ] LREM
  - [ ] LSET
  - [ ] LTRIM
  - [X] RPOP
  - [ ] RPOPLPUSH
  - [X] RPUSH
- [ ] SortedSet Family
  - [ ] ZADD
  - [ ] ZCARD
  - [ ] ZINCRBY
  - [ ] ZRANGE
  - [ ] ZRANGEBYSCORE
  - [ ] ZREM
  - [ ] ZREMRANGEBYSCORE
  - [ ] ZREVRANGE
  - [ ] ZSCORE
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
- [ ] flush_all
- [x] incr
- [x] decr
- [x] version
- [x] quit

API 2.0
- [ ] List Family
  - [X] BLPOP
  - [ ] BRPOP
  - [ ] BRPOPLPUSH
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
- [X] Generic Family
  - [X] SCAN
- [X] String Family
  - [X] APPEND
  - [X] PREPEND (dragonfly specific)


Commands that I prefer not implement before launch:
  - [ ] PUNSUBSCRIBE
  - [ ] PSUBSCRIBE

Also, I would omit keyspace notifications. For that I would like to deep dive and learn
exact use-cases for this API.

## Milestone Nymph
API 2,3,4 without cluster support, without modules, without memory inspection commands.
Without support for keyspace notifications.

Design config support. ~140 commands overall...
## Milestone Molt
API 5,6 - without cluster and modules. Streams support. ~80 commands overall.
## Milestone Adult
TBD.