# mini-redis

A toy memory store that supports basic commands like `SET` and `GET` for both memcached and redis protocols.
In addition, it supports redis `PING` command.

Demo features include:
1. High throughput reaching to millions of QPS on a single node.
2. TLS support
3. Pipelining mode that allows reaching order of magnitude higher QPS i.e. 30M qps and higher on a single node.


