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
