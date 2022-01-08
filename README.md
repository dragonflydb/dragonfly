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
  - [ ] DECR
  - [ ] INCR
  - [ ] DECRBY
  - [ ] GETSET
  - [ ] INCRBY
  - [X] MGET
  - [X] MSET
  - [ ] MSETNX
  - [ ] SUBSTR
- [ ] Generic family
  - [X] DEL
  - [X] ECHO
  - [X] EXISTS
  - [X] EXPIRE
  - [ ] EXPIREAT
  - [X] Ping
  - [X] RENAME
  - [X] RENAMENX
  - [X] SELECT
  - [ ] TTL
  - [ ] TYPE
  - [ ] SORT
- [X] Server Family
  - [X] QUIT
  - [X] DBSIZE
  - [ ] BGSAVE
  - [ ] SAVE
  - [ ] DBSIZE
  - [ ] DEBUG
  - [ ] EXEC
  - [ ] FLUSHALL
  - [ ] FLUSHDB
  - [ ] INFO
  - [ ] MULTI
  - [ ] SHUTDOWN
  - [ ] LASTSAVE
  - [ ] SLAVEOF/REPLICAOF
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
  - [ ] AUTH
  - [ ] BGREWRITEAOF
  - [ ] KEYS
  - [ ] MONITOR
  - [ ] RANDOMKEY
  - [ ] MOVE

In addition, we want to support efficient expiry (TTL) and cache eviction algorithms.
We should implement basic memory management support. For Master/Slave replication we should design
a distributed log format.

API 2.0
- [ ] List Family
  - [ ] BLPOP
  - [ ] BRPOP
  - [ ] BRPOPLPUSH
- [ ] PubSub family
  - [ ] PUBLISH
  - [ ] PUBSUB
  - [ ] PUBSUB CHANNELS
  - [ ] SUBSCRIBE
  - [ ] UNSUBSCRIBE
  - [ ] PUNSUBSCRIBE
  - [ ] PSUBSCRIBE
- [ ] Server Family
  - [ ] WATCH
  - [ ] UNWATCH
  - [ ] DISCARD


## Milestone Nymph
API 2,3,4 without cluster support, without modules and without memory inspection commands.
Design config support. ~140 commands overall...
## Milestone Molt
API 5,6 - without cluster and modules. Streams support. ~80 commands overall.
## Milestone Adult
TBD.