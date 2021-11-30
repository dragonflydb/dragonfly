# midi-redis

A toy memory store that supports basic commands like `SET` and `GET` for both memcached and redis protocols.
In addition, it supports redis `PING` command.

Demo features include:
1. High throughput reaching millions of QPS on a single node.
2. TLS support.
3. Pipelining mode.

## Building from source
I've tested the build on Ubuntu 21.04+.


```
git clone --recursive https://github.com/romange/midi-redis
cd midi-redis && ./helio/blaze.sh -release
cd build-opt && ninja midi-redis

```

## Running

```
./midi-redis --logtostderr
```

for more options, run `./midi-redis --help`
